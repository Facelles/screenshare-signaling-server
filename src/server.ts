import 'dotenv/config';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import crypto from 'crypto';

// ─── Types ──────────────────────────────────────────────────────────────────

interface Room {
  token: string;
  hostSocketId: string;
  viewerSocketId: string | null;
}

interface SocketData {
  roomId?: string;
  role?: 'host' | 'viewer';
}

// Socket.IO event maps — keeps emit/on fully typed without 'never'
interface ServerToClientEvents {
  room_created:   (payload: { roomId: string; token: string }) => void;
  joined:         (payload: { roomId: string }) => void;
  join_error:     (payload: { message: string }) => void;
  viewer_joined:  () => void;
  viewer_left:    () => void;
  host_left:      () => void;
  offer:          (payload: { sdp: object }) => void;
  answer:         (payload: { sdp: object }) => void;
  viewer_offer:   (payload: { sdp: object }) => void;
  host_answer:    (payload: { sdp: object }) => void;
  ice_candidate:  (payload: { candidate: object }) => void;
}

interface ClientToServerEvents {
  create_room:   () => void;
  join_room:     (payload: { token: string }) => void;
  rejoin_room_as_host: (payload: { token: string }) => void;
  offer:         (payload: { sdp: object }) => void;
  answer:        (payload: { sdp: object }) => void;
  viewer_offer:  (payload: { sdp: object }) => void;
  host_answer:   (payload: { sdp: object }) => void;
  ice_candidate: (payload: { candidate: object }) => void;
}

// ─── Config ──────────────────────────────────────────────────────────────────

const CLIENT_ORIGIN = process.env.CLIENT_ORIGIN ?? 'http://localhost:5173';
const PORT = Number(process.env.PORT ?? 3001);
const ACCESS_PASSWORD = process.env.ACCESS_PASSWORD ?? '';

// ─── App Setup ───────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);

app.use(cors({ origin: CLIENT_ORIGIN, credentials: true }));
app.use(express.json());

// ─── In-Memory Room Storage ──────────────────────────────────────────────────

const rooms = new Map<string, Room>();

// Grace period timers: roomId → NodeJS.Timeout
const hostGraceTimers = new Map<string, ReturnType<typeof setTimeout>>();
const viewerGraceTimers = new Map<string, ReturnType<typeof setTimeout>>();

const generateToken = (): string =>
  crypto.randomBytes(24).toString('base64url');

const getRoomByToken = (token: string): { roomId: string; room: Room } | null => {
  for (const [roomId, room] of rooms.entries()) {
    if (room.token === token) return { roomId, room };
  }
  return null;
};

// ─── HTTP Routes ─────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', activeRooms: rooms.size });
});

// ─── Socket.IO ───────────────────────────────────────────────────────────────

const io = new Server<ClientToServerEvents, ServerToClientEvents, Record<string, never>, SocketData>(
  server,
  {
    cors: { origin: CLIENT_ORIGIN, methods: ['GET', 'POST'] },
    transports: ['websocket', 'polling'],
  },
);

// ─── Authentication Middleware ───────────────────────────────────────────────

io.use((socket, next) => {
  if (!ACCESS_PASSWORD) {
    return next(); // If no password set on server, allow all
  }
  
  const clientPassword = socket.handshake.auth.password;
  if (clientPassword === ACCESS_PASSWORD) {
    return next();
  }
  
  return next(new Error('Unauthorized: Invalid password'));
});

io.on('connection', (socket) => {
  console.log(`[+] connected: ${socket.id}`);

  // ── HOST: Create a new room ─────────────────────────────────────────────
  socket.on('create_room', () => {
    const roomId = uuidv4().slice(0, 8);
    const token = generateToken();

    rooms.set(roomId, { token, hostSocketId: socket.id, viewerSocketId: null });

    void socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'host';

    console.log(`[room] created: ${roomId}`);
    socket.emit('room_created', { roomId, token });
  });

  socket.on('rejoin_room_as_host', ({ token }) => {
    const found = getRoomByToken(token);
    if (!found) {
      // Room was destroyed (e.g. server restart). Host should create a new one.
      socket.emit('join_error', { message: 'Кімната не знайдена.' });
      return;
    }
    const { roomId, room } = found;
    
    // Cancel the grace timer so the room isn’t destroyed!
    const existingTimer = hostGraceTimers.get(roomId);
    if (existingTimer) {
      clearTimeout(existingTimer);
      hostGraceTimers.delete(roomId);
      console.log(`[room] host reconnected, grace timer cancelled for ${roomId}`);
    }
    
    room.hostSocketId = socket.id;
    void socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'host';
    socket.emit('room_created', { roomId, token });
    console.log(`[room] host rejoined: ${roomId}`);
    
    // Notify host if viewer is already in the room
    if (room.viewerSocketId) {
      socket.emit('viewer_joined');
    }
  });

  // ── VIEWER: Join via one-time token ────────────────────────────────────
  socket.on('join_room', ({ token }) => {
    const found = getRoomByToken(token);

    if (!found) {
      socket.emit('join_error', { message: 'Кімнату не знайдено або посилання застаріло.' });
      return;
    }

    const { roomId, room } = found;

    if (room.viewerSocketId && room.viewerSocketId !== socket.id) {
      // Viewer slot is taken by a DIFFERENT socket.
      // Since they have the secure token, we kick the old socket and let them take over.
      const oldSocket = io.sockets.sockets.get(room.viewerSocketId);
      if (oldSocket) {
        oldSocket.emit('join_error', { message: 'Ви підключилися з іншої вкладки.' });
        oldSocket.disconnect(true);
      }
      console.log(`[room] viewer slot overtaken in ${roomId}`);
    }

    // Cancel viewer grace timer if they reconnect quickly
    const existingViewerTimer = viewerGraceTimers.get(roomId);
    if (existingViewerTimer) {
      clearTimeout(existingViewerTimer);
      viewerGraceTimers.delete(roomId);
      console.log(`[room] viewer reconnected, grace timer cancelled for ${roomId}`);
    }

    room.viewerSocketId = socket.id;
    void socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.role = 'viewer';

    console.log(`[room] viewer joined: ${roomId}`);

    const hostSocket = io.sockets.sockets.get(room.hostSocketId);
    hostSocket?.emit('viewer_joined');

    socket.emit('joined', { roomId });
  });

  // ── WebRTC Signaling Relay ──────────────────────────────────────────────

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
  socket.on('viewer_offer', ({ sdp }) => {
    const { roomId, role } = socket.data;
    if (!roomId || role !== 'viewer') return;

    const room = rooms.get(roomId);
    if (!room?.hostSocketId) return;

    io.to(room.hostSocketId).emit('viewer_offer', { sdp });
    console.log(`[sdp] viewer_offer → host in ${roomId}`);
  });

  // host_answer: Host answer to viewer mic offer → Viewer
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

  // ── Disconnect ──────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    const { roomId, role } = socket.data;
    console.log(`[-] disconnected: ${socket.id} (${role ?? 'unknown'})`);

    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    if (role === 'host') {
      // Grace period: wait 15s before destroying the room.
      // If the host reconnects in time, the timer is cancelled.
      console.log(`[room] host disconnected, starting 15s grace timer for room ${roomId}`);
      const timer = setTimeout(() => {
        const currentRoom = rooms.get(roomId);
        if (currentRoom) {
          if (currentRoom.viewerSocketId) {
            io.to(currentRoom.viewerSocketId).emit('host_left');
          }
          rooms.delete(roomId);
          hostGraceTimers.delete(roomId);
          console.log(`[room] destroyed after grace period: ${roomId}`);
        }
      }, 15_000);
      hostGraceTimers.set(roomId, timer);

    } else if (role === 'viewer') {
      // Small grace period for viewer disconnect (e.g. page refresh)
      const timer = setTimeout(() => {
        const currentRoom = rooms.get(roomId);
        if (currentRoom && currentRoom.viewerSocketId === socket.id) {
          currentRoom.viewerSocketId = null;
          const hostSocket = io.sockets.sockets.get(currentRoom.hostSocketId);
          hostSocket?.emit('viewer_left');
          console.log(`[room] viewer slot freed after grace: ${roomId}`);
        }
        viewerGraceTimers.delete(roomId);
      }, 3_000);
      viewerGraceTimers.set(roomId, timer);
    }
  });
});

// ─── Start ───────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🚀  Signaling server → http://localhost:${PORT}`);
  console.log(`    Client origin:    ${CLIENT_ORIGIN}\n`);
});
