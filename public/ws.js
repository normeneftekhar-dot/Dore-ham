import { createServer } from "node:http";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import Redis from "ioredis";

const MAX_PAYLOAD = 8192;
const MAX_LOCAL_CLIENTS = 400;
const MAX_ROOM_SIZE = 80;
const MAX_PRIVATE_SIZE = 2;
const PRESENCE_TTL_MS = 45000;
const HEARTBEAT_MS = 15000;
const EVENT_CHANNEL = "ki:v1:events";
const INSTANCE_ID = randomBytes(12).toString("hex");

const redisUrl = process.env.REDIS_URL;
const redis = redisUrl ? new Redis(redisUrl, {
  maxRetriesPerRequest: 2,
  enableReadyCheck: true,
  retryStrategy: (times) => Math.min(times * 250, 4000),
}) : null;
if (redis) redis.on("error", (err) => console.error("[ws] Redis connection error:", err?.message || err));

const server = createServer((_req, res) => {
  res.writeHead(426, { "content-type": "text/plain; charset=utf-8" });
  res.end("WebSocket endpoint. Connect using a WebSocket client.");
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD, perMessageDeflate: false });
const localClients = new Set();
const localRooms = new Map();
let subscriber = null;
let subscriberRunning = false;
let subscriberReady = null;
let heartbeatTimer = null;

const roomSetKey = (room) => `ki:v1:room:${room}:members`;
const roomHashKey = (room) => `ki:v1:room:${room}:presence`;

function send(ws, packet) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(packet)); } catch (_) {}
  }
}
function safeOrigin(req) {
  const origin = req.headers.origin;
  if (!origin) return true;
  try { return new URL(origin).host === req.headers.host; }
  catch (_) { return false; }
}
function validRoom(room) {
  return room === "lobby" || (typeof room === "string" && /^p:[a-z0-9]{20}$/.test(room));
}
function error(client, room, code) {
  send(client.ws, { t: "error", room, code });
}
function localRoom(room) {
  let set = localRooms.get(room);
  if (!set) { set = new Set(); localRooms.set(room, set); }
  return set;
}
function broadcastLocal(room, packet) {
  const set = localRooms.get(room);
  if (!set) return;
  set.forEach((client) => send(client.ws, packet));
}
function safePresencePatch(input, current) {
  if (!input || typeof input !== "object" || Array.isArray(input)) return null;
  const next = { ...current };
  for (const [key, value] of Object.entries(input)) {
    if (!/^[a-zA-Z][a-zA-Z0-9_]{0,31}$/.test(key)) continue;
    if (value === null) delete next[key];
    else if (typeof value === "string" && value.length <= 120) next[key] = value;
    else if (typeof value === "number" && Number.isFinite(value)) next[key] = value;
    else if (typeof value === "boolean") next[key] = value;
  }
  if (Buffer.byteLength(JSON.stringify(next)) > 4096) return null;
  return next;
}

async function publishEvent(event) {
  if (!redis) return;
  await redis.publish(EVENT_CHANNEL, JSON.stringify({ origin: INSTANCE_ID, event }));
}

async function getPeers(room) {
  if (!redis) return [];
  const setKey = roomSetKey(room);
  const hashKey = roomHashKey(room);
  const expired = await redis.zrangebyscore(setKey, "-inf", Date.now());
  if (expired.length) {
    const pipeline = redis.pipeline();
    pipeline.zrem(setKey, ...expired);
    pipeline.hdel(hashKey, ...expired);
    await pipeline.exec();
  }
  const ids = await redis.zrange(setKey, 0, -1);
  if (!ids.length) return [];
  const values = await redis.hmget(hashKey, ...ids);
  const peers = [];
  for (let i = 0; i < ids.length; i++) {
    if (!values[i]) continue;
    try {
      const data = JSON.parse(values[i]);
      peers.push({ peer: ids[i], presence: data && data.presence && typeof data.presence === "object" ? data.presence : {} });
    } catch (_) {}
  }
  return peers;
}

async function refreshPeers(room) {
  if (!localRooms.has(room)) return;
  const list = await getPeers(room);
  broadcastLocal(room, { t: "peers", room, list });
}

async function dispatchRemote(event) {
  if (!event || typeof event.room !== "string" || !validRoom(event.room)) return;
  if (event.kind === "peers") {
    await refreshPeers(event.room);
  } else if (event.kind === "msg" && event.packet) {
    broadcastLocal(event.room, event.packet);
  }
}

function startReader() {
  if (!redis) return Promise.resolve();
  if (subscriberRunning) return subscriberReady || Promise.resolve();
  subscriberRunning = true;
  subscriberReady = (async () => {
    try {
      subscriber = redis.duplicate({ maxRetriesPerRequest: null });
      subscriber.on("error", (err) => console.error("[ws] Redis subscriber error:", err?.message || err));
      subscriber.on("message", (_channel, raw) => {
        try {
          const envelope = JSON.parse(raw);
          if (!envelope || envelope.origin === INSTANCE_ID) return;
          void dispatchRemote(envelope.event);
        } catch (_) {}
      });
      await subscriber.subscribe(EVENT_CHANNEL);
    } catch (err) {
      console.error("[ws] Could not subscribe to Redis events:", err?.message || err);
      subscriberRunning = false;
      subscriberReady = null;
      if (subscriber) { try { subscriber.disconnect(); } catch (_) {} }
      subscriber = null;
    }
  })();
  return subscriberReady;
}

function ensureHeartbeat() {
  if (heartbeatTimer || !redis) return;
  heartbeatTimer = setInterval(async () => {
    const now = Date.now();
    const pipeline = redis.pipeline();
    for (const client of localClients) {
      for (const room of client.joined) {
        pipeline.zadd(roomSetKey(room), now + PRESENCE_TTL_MS, client.id);
      }
    }
    for (const client of localClients) {
      for (const room of client.joined) {
        pipeline.pexpire(roomSetKey(room), 120000);
        pipeline.pexpire(roomHashKey(room), 120000);
      }
    }
    try { await pipeline.exec(); }
    catch (err) { console.error("[ws] Presence heartbeat failed:", err?.message || err); }
  }, HEARTBEAT_MS);
}

async function stopReaderIfIdle() {
  if (localClients.size) return;
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
  subscriberRunning = false;
  subscriberReady = null;
  if (subscriber) {
    const old = subscriber;
    subscriber = null;
    try { await old.unsubscribe(EVENT_CHANNEL); } catch (_) {}
    try { old.disconnect(); } catch (_) {}
  }
}

const JOIN_SCRIPT = `
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(expired) do redis.call('HDEL', KEYS[2], id) end
if #expired > 0 then redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]) end
local exists = redis.call('ZSCORE', KEYS[1], ARGV[2])
local count = redis.call('ZCARD', KEYS[1])
if (not exists) and count >= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[5])
redis.call('PEXPIRE', KEYS[1], 120000)
redis.call('PEXPIRE', KEYS[2], 120000)
return 1
`;

async function joinRoom(client, room) {
  if (!validRoom(room)) return error(client, room, "invalid_room");
  if (client.joined.has(room)) {
    const list = await getPeers(room);
    send(client.ws, { t: "peers", room, list });
    return;
  }
  if (!redis) return error(client, room, "redis_not_configured");
  await startReader();
  if (!subscriber) return error(client, room, "service_unavailable");
  const cap = room === "lobby" ? MAX_ROOM_SIZE : MAX_PRIVATE_SIZE;
  const record = JSON.stringify({ presence: client.presence.get(room) || {} });
  let accepted;
  try {
    accepted = await redis.eval(JOIN_SCRIPT, 2, roomSetKey(room), roomHashKey(room),
      Date.now(), client.id, cap, Date.now() + PRESENCE_TTL_MS, record);
  } catch (err) {
    console.error("[ws] join failed:", err?.message || err);
    return error(client, room, "service_unavailable");
  }
  if (Number(accepted) !== 1) return error(client, room, "room_full");
  client.joined.add(room);
  localRoom(room).add(client);
  await refreshPeers(room);
  await publishEvent({ kind: "peers", room });
}

async function leaveRoom(client, room) {
  if (!client.joined.has(room)) return;
  client.joined.delete(room);
  const set = localRooms.get(room);
  if (set) {
    set.delete(client);
    if (!set.size) localRooms.delete(room);
  }
  if (redis) {
    try {
      const pipeline = redis.pipeline();
      pipeline.zrem(roomSetKey(room), client.id);
      pipeline.hdel(roomHashKey(room), client.id);
      await pipeline.exec();
      await publishEvent({ kind: "peers", room });
    } catch (err) {
      console.error("[ws] leave failed:", err?.message || err);
    }
  }
  await refreshPeers(room);
}

async function updatePresence(client, room, patch) {
  if (!client.joined.has(room)) return error(client, room, "not_in_room");
  const next = safePresencePatch(patch, client.presence.get(room) || {});
  if (!next) return error(client, room, "invalid_presence");
  client.presence.set(room, next);
  if (!redis) return error(client, room, "redis_not_configured");
  try {
    const pipeline = redis.pipeline();
    pipeline.hset(roomHashKey(room), client.id, JSON.stringify({ presence: next }));
    pipeline.zadd(roomSetKey(room), Date.now() + PRESENCE_TTL_MS, client.id);
    pipeline.pexpire(roomSetKey(room), 120000);
    pipeline.pexpire(roomHashKey(room), 120000);
    await pipeline.exec();
    await refreshPeers(room);
    await publishEvent({ kind: "peers", room });
  } catch (err) {
    console.error("[ws] presence update failed:", err?.message || err);
    error(client, room, "service_unavailable");
  }
}

async function emitMessage(client, room, topic, data) {
  if (!client.joined.has(room)) return error(client, room, "not_in_room");
  if (typeof topic !== "string" || !/^[a-z][a-z0-9_.-]{0,47}$/.test(topic)) return error(client, room, "invalid_message");
  let serialized;
  try { serialized = JSON.stringify(data === undefined ? null : data); }
  catch (_) { return error(client, room, "invalid_message"); }
  if (Buffer.byteLength(serialized) > 4096) return error(client, room, "invalid_message");
  const packet = { t: "msg", room, topic, data: data === undefined ? null : data, peer: client.id };
  broadcastLocal(room, packet);
  try { await publishEvent({ kind: "msg", room, packet }); }
  catch (err) { console.error("[ws] message relay failed:", err?.message || err); }
}

async function unregister(client) {
  for (const room of Array.from(client.joined)) await leaveRoom(client, room);
  client.presence.clear();
  localClients.delete(client);
  await stopReaderIfIdle();
}

wss.on("connection", (ws) => {
  if (!redis && process.env.NODE_ENV === "production") {
    send(ws, { t: "error", room: "lobby", code: "redis_not_configured" });
    ws.close(1011, "Redis is required for production rooms");
    return;
  }
  if (localClients.size >= MAX_LOCAL_CLIENTS) {
    ws.close(1013, "instance is full");
    return;
  }
  const client = { ws, id: randomBytes(12).toString("hex"), joined: new Set(), presence: new Map(), rate: [], processing: Promise.resolve() };
  localClients.add(client);
  void startReader();
  ensureHeartbeat();
  send(ws, { t: "hello", id: client.id });

  ws.on("message", (raw) => {
    client.processing = client.processing.then(async () => {
      const now = Date.now();
      client.rate = client.rate.filter((time) => now - time < 10000);
      if (client.rate.length >= 100) { ws.close(1008, "rate limit"); return; }
      client.rate.push(now);
      let message;
      try { message = JSON.parse(raw.toString()); }
      catch (_) { ws.close(1007, "invalid json"); return; }
      if (!message || typeof message.t !== "string") return;
      const room = message.room;
      if (message.t === "join") { await joinRoom(client, room); return; }
      if (message.t === "leave") { if (typeof room === "string") await leaveRoom(client, room); return; }
      if (!client.joined.has(room)) return error(client, room, "not_in_room");
      if (message.t === "pres") { await updatePresence(client, room, message.patch); return; }
      if (message.t === "emit") { await emitMessage(client, room, message.topic, message.data); }
    }).catch((err) => console.error("[ws] message handler failed:", err?.message || err));
  });

  ws.on("close", () => { void unregister(client); });
  ws.on("error", () => { void unregister(client); });
});

server.on("upgrade", (req, socket, head) => {
  if (!safeOrigin(req) || (!redis && process.env.NODE_ENV === "production")) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
});

export default server;
