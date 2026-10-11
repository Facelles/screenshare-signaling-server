import 'dotenv/config';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import crypto from 'crypto';
import { Redis } from '@upstash/redis';

// ─── Types ───────────────────────────────────────────────────────────────────

interface Room {
  token: string;
  hostSocketId: string;
  viewerSocketId: string | null;
  // Buffered viewer_offer SDP: stored when viewer sends mic offer before host
  // is connected/ready, so we can replay it when the host reconnects.
  pendingViewerOffer: object | null;
}

interface SocketData {
  roomId?: string;
  role?: 'host' | 'viewer';
}

interface ServerToClientEvents {
  room_created:      (payload: { roomId: string; token: string }) => void;
  joined:            (payload: { roomId: string }) => void;
  join_error:        (payload: { message: string }) => void;
  viewer_joined:     () => void;
  viewer_left:       () => void;
  host_left:         () => void;
  room_closed:       () => void;
  offer:             (payload: { sdp: object }) => void;
  answer:            (payload: { sdp: object }) => void;
  viewer_offer:      (payload: { sdp: object }) => void;
  host_answer:       (payload: { sdp: object }) => void;
  ice_candidate:     (payload: { candidate: object }) => void;
}

interface ClientToServerEvents {
  create_room:         () => void;
  join_room:           (payload: { token: string }) => void;
  rejoin_room_as_host: (payload: { token: string }) => void;
  close_room:          () => void;
  offer:               (payload: { sdp: object }) => void;
  answer:              (payload: { sdp: object }) => void;
  viewer_offer:        (payload: { sdp: object }) => void;
  host_answer:         (payload: { sdp: object }) => void;
  ice_candidate:       (payload: { candidate: object }) => void;
}

// ─── Config ──────────────────────────────────────────────────────────────────

const CLIENT_ORIGIN  = process.env.CLIENT_ORIGIN  ?? 'http://localhost:5173';
const PORT           = Number(process.env.PORT     ?? 3001);
const ACCESS_PASSWORD = process.env.ACCESS_PASSWORD ?? '';
const UPSTASH_URL    = process.env.UPSTASH_REDIS_REST_URL   ?? '';
const UPSTASH_TOKEN  = process.env.UPSTASH_REDIS_REST_TOKEN ?? '';

// Room TTL: 24 hours (seconds for Redis EXPIRE, ms for in-memory fallback)
const ROOM_TTL_SEC = 60 * 60 * 24;          // 86 400 s
const ROOM_TTL_MS  = ROOM_TTL_SEC * 1_000;  // 86 400 000 ms

// Grace period before a host-less room is marked offline (socket still tracked in-memory)
const HOST_GRACE_MS   = 15_000;
const VIEWER_GRACE_MS =  3_000;

// ─── Redis setup ─────────────────────────────────────────────────────────────

// @upstash/redis uses HTTP REST API — works on any hosting, no TCP port needed.
let redis: Redis | null = null;

if (UPSTASH_URL && UPSTASH_TOKEN) {
  redis = new Redis({ url: UPSTASH_URL, token: UPSTASH_TOKEN });
  // Verify connectivity at startup
  redis.ping()
    .then(() => console.log('[redis] Upstash connected ✓'))
    .catch((err) => {
      console.error('[redis] Upstash ping failed, falling back to in-memory:', err.message);
      redis = null;
    });
} else {
  console.warn('[redis] UPSTASH_REDIS_REST_URL / TOKEN not set — running in-memory only (rooms lost on restart)');
}

// ─── Persistent Room Store (Redis-backed, in-memory fallback) ─────────────────
//
//  Redis key layout:
//    room:<roomId>   → JSON-serialised Room (without live socketIds – those are session-only)
//    token:<token>   → roomId
//
//  Both keys share the same 24h TTL and are refreshed on every host reconnect.
//
//  socketIds are NOT stored in Redis because a socket is tied to a single server
//  process instance.  After a server restart the host MUST reconnect (which
//  resets hostSocketId in the in-memory map), but the room token survives.

interface PersistedRoom {
  token:              string;
  createdAt:          number;   // Unix ms – for auditing
  pendingViewerOffer: object | null;
}

async function persistRoom(roomId: string, data: PersistedRoom): Promise<void> {
  if (!redis) return;
  // @upstash/redis automatically serializes objects and supports { ex: seconds }
  await Promise.all([
    redis.set(`room:${roomId}`, data,        { ex: ROOM_TTL_SEC }),
    redis.set(`token:${data.token}`, roomId, { ex: ROOM_TTL_SEC }),
  ]);
}

async function loadRoom(roomId: string): Promise<PersistedRoom | null> {
  if (!redis) return null;
  // @upstash/redis automatically deserializes JSON objects upon get()
  const raw = await redis.get<PersistedRoom | string>(`room:${roomId}`);
  if (!raw) return null;
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw) as PersistedRoom;
    } catch {
      return null;
    }
  }
  return raw as PersistedRoom;
}

async function roomIdByToken(token: string): Promise<string | null> {
  if (!redis) return null;
  return redis.get<string>(`token:${token}`);
}

async function deleteRoom(roomId: string, token: string): Promise<void> {
  if (!redis) return;
  await Promise.all([
    redis.del(`room:${roomId}`),
    redis.del(`token:${token}`),
  ]);
}

async function refreshRoomTTL(roomId: string, token: string): Promise<void> {
  if (!redis) return;
  await Promise.all([
    redis.expire(`room:${roomId}`, ROOM_TTL_SEC),
    redis.expire(`token:${token}`, ROOM_TTL_SEC),
  ]);
}

// ─── In-Memory Live State ────────────────────────────────────────────────────
//
//  Everything below is ephemeral and lives only for the lifetime of this
//  server process.  On restart only the token lookup is restored from Redis
//  as soon as the host calls rejoin_room_as_host.

// Live socket associations
const rooms = new Map<string, Room>();

// Grace-period timers (not persisted – a restart gives a fresh grace window)
const hostGraceTimers   = new Map<string, ReturnType<typeof setTimeout>>();
const viewerGraceTimers = new Map<string, ReturnType<typeof setTimeout>>();
const viewerWaitTimers  = new Map<string, ReturnType<typeof setTimeout>>(); // timeout if host never joins

// Rate limiting
const rateLimits = new Map<string, { count: number; resetAt: number }>();
function checkRateLimit(ip: string): boolean {
  const now = Date.now();
  const record = rateLimits.get(ip);
  if (!record || now > record.resetAt) {
    rateLimits.set(ip, { count: 1, resetAt: now + 60_000 }); // 1 min window
    return true;
  }
  if (record.count >= 10) { // Max 10 rooms per minute per IP
    return false;
  }
  record.count++;
  return true;
}

// In-memory TTL timers (used when Redis is unavailable)
const memoryTtlTimers = new Map<string, ReturnType<typeof setTimeout>>();

// ─── Helpers ─────────────────────────────────────────────────────────────────

const generateToken = (): string => crypto.randomBytes(24).toString('base64url');

/**
 * Look up a room by token.
 * 1. Check in-memory map first (fast path).
 * 2. Fall back to Redis lookup (needed after server restart if host hasn't
 *    called rejoin yet – mainly for viewer joining before host comes back).
 */
async function findRoomByToken(
  token: string,
): Promise<{ roomId: string; room: Room } | null> {
  // Fast path: room already loaded in memory
  for (const [roomId, room] of rooms.entries()) {
    if (room.token === token) return { roomId, room };
  }

  // Slow path: try Redis
  const roomId = await roomIdByToken(token);
  if (!roomId) return null;

  const persisted = await loadRoom(roomId);
  if (!persisted) return null;

  // Reconstruct a placeholder room in memory (host not yet online)
  const room: Room = {
    token: persisted.token,
    hostSocketId: '',          // host not connected yet
    viewerSocketId: null,
    pendingViewerOffer: persisted.pendingViewerOffer,
  };
  rooms.set(roomId, room);
  scheduleMemoryTtl(roomId, token);
  return { roomId, room };
}

/**
 * Start an in-memory cleanup timer for rooms that exist in memory but Redis
 * is unavailable (or as a safety net regardless).
 */
function scheduleMemoryTtl(roomId: string, token: string) {
  const existing = memoryTtlTimers.get(roomId);
  if (existing) clearTimeout(existing);

  const t = setTimeout(() => {
    const room = rooms.get(roomId);
    if (room) {
      if (room.viewerSocketId) {
        io.to(room.viewerSocketId).emit('host_left');
      }
      rooms.delete(roomId);
    }
    memoryTtlTimers.delete(roomId);
    console.log(`[room] expired from memory after ${ROOM_TTL_MS / 3_600_000}h: ${roomId}`);
  }, ROOM_TTL_MS);

  // Don't block process exit
  if (t.unref) t.unref();
  memoryTtlTimers.set(roomId, t);
}

// ─── App Setup ───────────────────────────────────────────────────────────────

const app    = express();
const server = http.createServer(app);

app.use(cors({ origin: CLIENT_ORIGIN, credentials: true }));
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({
    status:      'ok',
    activeRooms: rooms.size,
    redis:       redis ? 'connected' : 'off',
  });
});

// ─── Socket.IO ───────────────────────────────────────────────────────────────

const io = new Server<ClientToServerEvents, ServerToClientEvents, Record<string, never>, SocketData>(
  server,
  {
    cors:       { origin: CLIENT_ORIGIN, methods: ['GET', 'POST'] },
    transports: ['websocket', 'polling'],
  },
);

// ── Auth Middleware ───────────────────────────────────────────────────────────

io.use((socket, next) => {
  if (!ACCESS_PASSWORD) return next();
  if (socket.handshake.auth.password === ACCESS_PASSWORD) return next();
  return next(new Error('Unauthorized: Invalid password'));
});

// ── Connection Handler ────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log(`[+] connected: ${socket.id}`);

  // ── HOST: Create a new room ───────────────────────────────────────────────
  socket.on('create_room', async () => {
    const ip = socket.handshake.address;
    if (!checkRateLimit(ip)) {
      socket.emit('join_error', { message: 'Забагато запитів. Спробуйте пізніше.' });
      return;
    }

    const roomId = uuidv4().slice(0, 8);
    const token  = generateToken();

    const room: Room = { token, hostSocketId: socket.id, viewerSocketId: null, pendingViewerOffer: null };
    rooms.set(roomId, room);

    // Persist to Redis with 24h TTL
    await persistRoom(roomId, { token, createdAt: Date.now(), pendingViewerOffer: null });
    scheduleMemoryTtl(roomId, token);

    void socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role   = 'host';

    console.log(`[room] created: ${roomId} by ${ip}`);
    socket.emit('room_created', { roomId, token });
  });

  // ── HOST: Close room explicitly ───────────────────────────────────────────
  socket.on('close_room', async () => {
    const { roomId, role } = socket.data;
    if (!roomId || role !== 'host') return;
    const room = rooms.get(roomId);
    if (room) {
      if (room.viewerSocketId) {
        io.to(room.viewerSocketId).emit('room_closed');
      }
      rooms.delete(roomId);
      await deleteRoom(roomId, room.token);
      console.log(`[room] explicitly closed by host: ${roomId}`);
    }
  });

  // ── HOST: Rejoin after reconnect ──────────────────────────────────────────
  socket.on('rejoin_room_as_host', async ({ token }) => {
    const found = await findRoomByToken(token);
    if (!found) {
      // Room expired (> 24h) or never existed — host must create a new one.
      socket.emit('join_error', { message: 'Кімната не знайдена або посилання застаріло (> 24 год).' });
      return;
    }

    const { roomId, room } = found;

    // Cancel the grace-period destroy timer
    const graceTimer = hostGraceTimers.get(roomId);
    if (graceTimer) {
      clearTimeout(graceTimer);
      hostGraceTimers.delete(roomId);
      console.log(`[room] host reconnected, grace cancelled: ${roomId}`);
    }

    // Update live socket reference — atomic within the single Node.js event loop
    room.hostSocketId = socket.id;
    void socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role   = 'host';

    // Refresh 24h TTL on every successful reconnect
    await refreshRoomTTL(roomId, token);
    scheduleMemoryTtl(roomId, token); // reset in-memory watchdog too

    socket.emit('room_created', { roomId, token });
    console.log(`[room] host rejoined: ${roomId}`);

    // Let host know a viewer is already waiting
    if (room.viewerSocketId) {
      socket.emit('viewer_joined');
      const waitTimer = viewerWaitTimers.get(roomId);
      if (waitTimer) {
        clearTimeout(waitTimer);
        viewerWaitTimers.delete(roomId);
      }
    }

    // ── FIX: Viewer mic offer was buffered while host was away ─────────────
    // Replay it now so the host can answer and the viewer mic starts working.
    if (room.pendingViewerOffer) {
      console.log(`[sdp] replaying buffered viewer_offer → host in ${roomId}`);
      socket.emit('viewer_offer', { sdp: room.pendingViewerOffer });
      room.pendingViewerOffer = null;
      // Clear from Redis too
      await persistRoom(roomId, {
        token,
        createdAt: Date.now(),
        pendingViewerOffer: null,
      });
    }
  });

  // ── VIEWER: Join ──────────────────────────────────────────────────────────
  socket.on('join_room', async ({ token }) => {
    const found = await findRoomByToken(token);

    if (!found) {
      socket.emit('join_error', { message: 'Кімнату не знайдено або посилання застаріло.' });
      return;
    }

    const { roomId, room } = found;

    // Kick old viewer if a different socket tries to join with the same token
    if (room.viewerSocketId && room.viewerSocketId !== socket.id) {
      const old = io.sockets.sockets.get(room.viewerSocketId);
      if (old) {
        old.emit('join_error', { message: 'Ви підключилися з іншої вкладки.' });
        old.disconnect(true);
      }
      console.log(`[room] viewer slot overtaken in ${roomId}`);
    }

    // Cancel viewer grace timer on quick reconnect
    const viewerTimer = viewerGraceTimers.get(roomId);
    if (viewerTimer) {
      clearTimeout(viewerTimer);
      viewerGraceTimers.delete(roomId);
      console.log(`[room] viewer reconnected, grace cancelled: ${roomId}`);
    }

    room.viewerSocketId = socket.id;
    void socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role   = 'viewer';

    console.log(`[room] viewer joined: ${roomId}`);

    // Notify host (only if host socket is currently alive)
    const hostSocket = room.hostSocketId ? io.sockets.sockets.get(room.hostSocketId) : null;
    if (hostSocket) {
      hostSocket.emit('viewer_joined');
    } else {
      // Host is not here, start a 10 min wait timer
      const timer = setTimeout(() => {
        const currentRoom = rooms.get(roomId);
        if (currentRoom && currentRoom.viewerSocketId === socket.id) {
          socket.emit('join_error', { message: 'Хост не підключився (таймаут 10 хв).' });
          socket.disconnect(true);
        }
        viewerWaitTimers.delete(roomId);
      }, 10 * 60 * 1000);
      viewerWaitTimers.set(roomId, timer);
    }

    socket.emit('joined', { roomId });
  });

  // ── WebRTC Signaling ─────────────────────────────────────────────────────

  socket.on('offer', ({ sdp }) => {
    const { roomId, role } = socket.data;
    if (!roomId || role !== 'host') return;
    const room = rooms.get(roomId);
    if (!room?.viewerSocketId) return;
    io.to(room.viewerSocketId).emit('offer', { sdp });
    console.log(`[sdp] offer → viewer in ${roomId}`);
  });

  socket.on('answer', ({ sdp }) => {
    const { roomId, role } = socket.data;
    if (!roomId || role !== 'viewer') return;
    const room = rooms.get(roomId);
    if (!room?.hostSocketId) return;
    io.to(room.hostSocketId).emit('answer', { sdp });
    console.log(`[sdp] answer → host in ${roomId}`);
  });

  // viewer_offer: Viewer mic renegotiation → Host
  // If the host is currently offline, buffer the offer so it can be replayed
  // the moment the host reconnects (fixes "viewer mic silent until host enables mic first").
  socket.on('viewer_offer', async ({ sdp }) => {
    const { roomId, role } = socket.data;
    if (!roomId || role !== 'viewer') return;

    const room = rooms.get(roomId);
    if (!room) return;

    const hostSocket = room.hostSocketId ? io.sockets.sockets.get(room.hostSocketId) : null;

    if (hostSocket) {
      // Host is live — forward immediately
      hostSocket.emit('viewer_offer', { sdp });
      console.log(`[sdp] viewer_offer → host in ${roomId}`);
    } else {
      // Host is temporarily offline — buffer the offer
      room.pendingViewerOffer = sdp;
      // Persist so it survives a server restart (best-effort)
      const persisted = await loadRoom(roomId);
      if (persisted) {
        await persistRoom(roomId, { ...persisted, pendingViewerOffer: sdp });
      }
      console.log(`[sdp] viewer_offer buffered (host offline) in ${roomId}`);
    }
  });

  socket.on('host_answer', ({ sdp }) => {
    const { roomId, role } = socket.data;
    if (!roomId || role !== 'host') return;
    const room = rooms.get(roomId);
    if (!room?.viewerSocketId) return;
    io.to(room.viewerSocketId).emit('host_answer', { sdp });
    console.log(`[sdp] host_answer → viewer in ${roomId}`);
  });

  socket.on('ice_candidate', ({ candidate }) => {
    const { roomId, role } = socket.data;
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;
    const targetId = role === 'host' ? room.viewerSocketId : room.hostSocketId;
    if (targetId) io.to(targetId).emit('ice_candidate', { candidate });
  });

  // ── Disconnect ────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    const { roomId, role } = socket.data;
    console.log(`[-] disconnected: ${socket.id} (${role ?? 'unknown'})`);

    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    if (role === 'host') {
      // Room stays alive in Redis for up to 24h.
      // Give the host a 15s window to reconnect before we notify the viewer.
      console.log(`[room] host disconnected, 15s grace started: ${roomId}`);
      const timer = setTimeout(async () => {
        const current = rooms.get(roomId);
        if (current) {
          if (current.viewerSocketId) {
            io.to(current.viewerSocketId).emit('host_left');
          }
          // Do NOT delete from Redis — room survives for 24h.
          // Only clear from the in-memory live map so we stop routing messages.
          rooms.delete(roomId);
          hostGraceTimers.delete(roomId);
          console.log(`[room] removed from memory after grace (Redis TTL still active): ${roomId}`);
        }
      }, HOST_GRACE_MS);
      hostGraceTimers.set(roomId, timer);

    } else if (role === 'viewer') {
      const timer = setTimeout(() => {
        const current = rooms.get(roomId);
        if (current && current.viewerSocketId === socket.id) {
          current.viewerSocketId = null;
          // Also clear any buffered viewer offer since the viewer left
          current.pendingViewerOffer = null;
          const hostSocket = current.hostSocketId
            ? io.sockets.sockets.get(current.hostSocketId)
            : null;
          hostSocket?.emit('viewer_left');
          console.log(`[room] viewer slot freed after grace: ${roomId}`);
        }
        viewerGraceTimers.delete(roomId);
      }, VIEWER_GRACE_MS);
      viewerGraceTimers.set(roomId, timer);
    }
  });
});

// ─── Start ────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🚀  Signaling server → http://localhost:${PORT}`);
  console.log(`    Client origin:    ${CLIENT_ORIGIN}`);
  console.log(`    Redis:            ${UPSTASH_URL ? 'Upstash enabled' : 'disabled (in-memory only)'}\n`);
});
