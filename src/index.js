/* ============================================================
   المجلس — نسخة Cloudflare (Workers + Durable Objects)
   غرفة واحدة = كائن دائم واحد، مع WebSocket Hibernation
   حتى لا تُحتسب مدة تشغيل على الغرف الخاملة.
   ============================================================ */

const CODE_ALPHABET = 'ACDEFGHJKLMNPQRTUVWXY34679';
const MAX_ROOM = 4;
const GRACE_MS = 45_000;      // مهلة العودة بعد انقطاع
const IDLE_MS  = 3 * 60 * 60 * 1000;   // تُغلق الغرفة بعد ٣ ساعات خمول

const code5 = () =>
  Array.from({ length: 5 }, () => CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)]).join('');

function cors(env, req, extra = {}) {
  const allow = (env.ALLOWED_ORIGINS || '*').split(',').map(s => s.trim());
  const origin = req.headers.get('Origin') || '';
  const ok = allow.includes('*') || allow.includes(origin);
  return {
    'Access-Control-Allow-Origin': ok ? (origin || '*') : 'null',
    'Access-Control-Allow-Headers': 'content-type',
    'Vary': 'Origin',
    ...extra
  };
}
const json = (obj, env, req, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: cors(env, req, { 'content-type': 'application/json; charset=utf-8' })
  });

/* ---------- الواجهة الأمامية للعامل ---------- */
export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors(env, req) });

    if (url.pathname === '/health' || url.pathname === '/')
      return json({ ok: true, service: 'majlis', edge: true }, env, req);

    /* خوادم الصوت */
    if (url.pathname === '/ice') {
      const iceServers = [{ urls: ['stun:stun.l.google.com:19302', 'stun:global.stun.twilio.com:3478'] }];
      if (env.TURN_URL) {
        iceServers.push({
          urls: env.TURN_URL.split(','),
          username: env.TURN_USER || '',
          credential: env.TURN_PASS || ''
        });
      }
      return json({ iceServers }, env, req);
    }

    /* رمز غرفة جديد */
    if (url.pathname === '/new') return json({ code: code5() }, env, req);

    /* قناة اللعب: كل رمز يذهب إلى كائنه الدائم */
    if (url.pathname === '/ws') {
      const code = (url.searchParams.get('code') || '').toUpperCase();
      if (!/^[A-Z0-9]{4,6}$/.test(code) && code !== 'LOBBY') return new Response('bad code', { status: 400 });
      const id = env.ROOM.idFromName(code);
      return env.ROOM.get(id).fetch(req);
    }

    return new Response('not found', { status: 404 });
  }
};

/* ============================================================
   الكائن الدائم: غرفة واحدة
   ============================================================ */
export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.code = null;
  }

  /* ---- تخزين خفيف: قائمة اللاعبين تعيش في التخزين لتصمد أمام السبات ---- */
  async load() {
    if (!this.mem) {
      this.mem = (await this.state.storage.get('m')) || { hostId: null, game: 'mj', started: false, state: null, seq: 0, players: {} };
    }
    return this.mem;
  }
  async save() { await this.state.storage.put('m', this.mem); }

  sockets() { return this.state.getWebSockets(); }
  meta(ws) { try { return ws.deserializeAttachment() || {}; } catch { return {}; } }

  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }
  broadcast(obj, exceptId = null) {
    for (const ws of this.sockets()) {
      const a = this.meta(ws);
      if (exceptId && a.id === exceptId) continue;
      this.send(ws, obj);
    }
  }
  socketOf(id) { return this.sockets().find(ws => this.meta(ws).id === id); }

  pub() {
    const m = this.mem;
    return {
      code: this.code, game: m.game, hostId: m.hostId, started: m.started,
      players: Object.values(m.players).map(p => ({
        id: p.id, name: p.name, seat: p.seat, online: p.online, status: p.status || 'free',
        voice: p.voice, ping: p.ping, isHost: p.id === m.hostId
      }))
    };
  }
  pushRoom() { this.broadcast({ t: 'room', room: this.pub() }); }

  freeSeat() {
    const taken = new Set(Object.values(this.mem.players).map(p => p.seat));
    const cap = this.code === 'LOBBY' ? 64 : MAX_ROOM;
    for (let i = 0; i < cap; i++) if (!taken.has(i)) return i;
    return -1;
  }
  reassignHost() {
    const m = this.mem;
    if (m.players[m.hostId]?.online) return;
    const next = Object.values(m.players).find(p => p.online);
    if (next) { m.hostId = next.id; this.broadcast({ t: 'sys', code: 'host_changed', name: next.name }); }
  }

  /* ---- الترقية إلى WebSocket ---- */
  async fetch(req) {
    const url = new URL(req.url);
    this.code = (url.searchParams.get('code') || '').toUpperCase();
    await this.state.storage.put('code', this.code);
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    /* السبات: لا تُحتسب مدة تشغيل بين الرسائل */
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ id: null });
    this.send(server, { t: 'hello', v: 1, edge: true });
    await this.state.storage.setAlarm(Date.now() + IDLE_MS);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;
    await this.load();
    if (!this.code) this.code = await this.state.storage.get('code');
    const a = this.meta(ws);
    const me = a.id ? this.mem.players[a.id] : null;
    const mm = this.mem;

    switch (m.t) {

      case 'create': {
        const id = crypto.randomUUID();
        mm.players[id] = mkPlayer(id, m.name, 0);
        mm.hostId = id; mm.game = m.game || 'mj'; mm.started = false; mm.state = null; mm.seq = 0;
        ws.serializeAttachment({ id });
        await this.save();
        this.send(ws, { t: 'joined', you: id, room: this.pub() });
        this.pushRoom();
        break;
      }

      case 'join': {
        /* عودة بعد انقطاع */
        if (m.rejoin && mm.players[m.rejoin]) {
          const p = mm.players[m.rejoin];
          p.online = true; p.name = m.name || p.name; p.leftAt = null;
          ws.serializeAttachment({ id: p.id });
          await this.save();
          this.send(ws, { t: 'joined', you: p.id, room: this.pub(), state: mm.state, seq: mm.seq });
          this.broadcast({ t: 'sys', code: 'reconnected', name: p.name, id: p.id }, p.id);
          this.pushRoom();
          break;
        }
        if (!mm.hostId && this.code !== 'LOBBY') { this.send(ws, { t: 'err', code: 'no_room' }); break; }
        if (this.code !== 'LOBBY' && Object.keys(mm.players).length >= MAX_ROOM) { this.send(ws, { t: 'err', code: 'full' }); break; }
        if (this.code === 'LOBBY') {               // نفس الاسم يستبدل الجلسة القديمة فوراً
          for (const pid of Object.keys(mm.players)) {
            if (mm.players[pid].name === String(m.name || '').slice(0, 18)) delete mm.players[pid];
          }
        }
        const seat = this.freeSeat();
        if (seat < 0) { this.send(ws, { t: 'err', code: 'full' }); break; }
        const id = crypto.randomUUID();
        mm.players[id] = mkPlayer(id, m.name, seat);
        ws.serializeAttachment({ id });
        await this.save();
        this.send(ws, { t: 'joined', you: id, room: this.pub(), state: mm.state, seq: mm.seq });
        this.broadcast({ t: 'sys', code: 'joined', name: mm.players[id].name, id }, id);
        this.pushRoom();
        break;
      }

      case 'state': {
        if (!me || me.id !== mm.hostId) break;
        mm.state = m.state; mm.seq = (mm.seq || 0) + 1; mm.started = true;
        await this.save();
        this.broadcast({ t: 'state', state: m.state, seq: mm.seq, game: m.game }, me.id);
        break;
      }

      case 'input': {
        if (!me) break;
        const host = this.socketOf(mm.hostId);
        if (host) this.send(host, { t: 'input', from: me.id, seat: me.seat, action: m.action, args: m.args });
        break;
      }

      case 'game': {
        if (!me || me.id !== mm.hostId) break;
        mm.game = m.game; mm.state = null; mm.started = false;
        await this.save();
        this.broadcast({ t: 'game', game: m.game });
        this.pushRoom();
        break;
      }

      case 'invite': {
        if (!me) break;
        const tgt = this.socketOf(m.to);
        if (!tgt) { this.send(ws, { t: 'err', code: 'gone' }); break; }
        this.send(tgt, { t: 'invite', from: me.id, name: me.name, code: m.code, game: m.game });
        break;
      }
      case 'inviteReply': {
        if (!me) break;
        const tgt = this.socketOf(m.to);
        if (tgt) this.send(tgt, { t: 'inviteReply', from: me.id, name: me.name, ok: !!m.ok });
        break;
      }
      case 'status': {
        if (!me) break;
        me.status = String(m.s || 'free').slice(0, 8);
        await this.save(); this.pushRoom();
        break;
      }

      case 'emote':
        if (me) this.broadcast({ t: 'emote', from: me.id, name: me.name, e: String(m.e || '').slice(0, 8) });
        break;

      case 'voice': {
        if (!me) break;
        me.voice = !!m.on; await this.save();
        this.broadcast({ t: 'voice', id: me.id, on: me.voice });
        this.pushRoom();
        break;
      }

      case 'signal': {
        if (!me) break;
        const target = this.socketOf(m.to);
        if (target) this.send(target, { t: 'signal', from: me.id, data: m.data });
        break;
      }

      case 'ping': {
        this.send(ws, { t: 'pong', ts: m.ts });
        if (me && typeof m.rtt === 'number') {
          const prev = me.ping;
          me.ping = Math.round(m.rtt);
          if (prev === null || Math.abs(prev - me.ping) > 40) { await this.save(); this.pushRoom(); }
        }
        break;
      }

      case 'leave': {
        if (!me) break;
        delete mm.players[me.id];
        await this.save();
        this.broadcast({ t: 'sys', code: 'left', name: me.name });
        this.reassignHost(); this.pushRoom();
        try { ws.close(1000, 'left'); } catch {}
        break;
      }
    }
  }

  async webSocketClose(ws) {
    await this.load();
    const a = this.meta(ws);
    const p = a.id ? this.mem.players[a.id] : null;
    if (!p) return;
    if (this.code === 'LOBBY') {           // الردهة: لا مهلة انتظار
      delete this.mem.players[p.id];
      await this.save(); this.pushRoom();
      return;
    }
    p.online = false; p.leftAt = Date.now();
    await this.save();
    this.broadcast({ t: 'sys', code: 'disconnected', name: p.name, id: p.id });
    this.reassignHost();
    this.pushRoom();
    /* منبّه لإزالة من لم يعد خلال المهلة */
    await this.state.storage.setAlarm(Date.now() + GRACE_MS + 1000);
  }

  async webSocketError(ws) { return this.webSocketClose(ws); }

  async alarm() {
    await this.load();
    const now = Date.now();
    let changed = false;
    for (const p of Object.values(this.mem.players)) {
      if (!p.online && p.leftAt && now - p.leftAt > GRACE_MS) {
        delete this.mem.players[p.id];
        this.broadcast({ t: 'sys', code: 'left', name: p.name, reason: 'timeout' });
        changed = true;
      }
    }
    if (changed) { this.reassignHost(); await this.save(); this.pushRoom(); }

    const anyone = Object.keys(this.mem.players).length > 0;
    if (!anyone) { await this.state.storage.deleteAll(); this.mem = null; return; }
    /* منبّه بعيد للتنظيف عند الخمول الطويل */
    await this.state.storage.setAlarm(now + Math.min(IDLE_MS, 10 * 60 * 1000));
  }
}

function mkPlayer(id, name, seat) {
  return {
    id, seat, online: true, voice: false, ping: null, status: 'free',
    name: String(name || 'ضيف').slice(0, 18),
    joinedAt: Date.now(), leftAt: null
  };
}
