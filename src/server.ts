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
  room_created:  (payload: { roomId: string; token: string }) => void;
  joined:        (payload: { roomId: string }) => void;
  join_error:    (payload: { message: string }) => void;
  viewer_joined: () => void;
  viewer_left:   () => void;
  host_left:     () => void;
  offer:         (payload: { sdp: object }) => void;
  answer:        (payload: { sdp: object }) => void;
  ice_candidate: (payload: { candidate: object }) => void;
}

interface ClientToServerEvents {
  create_room:   () => void;
  join_room:     (payload: { token: string }) => void;
  offer:         (payload: { sdp: object }) => void;
  answer:        (payload: { sdp: object }) => void;
  ice_candidate: (payload: { candidate: object }) => void;
}

// ─── Config ──────────────────────────────────────────────────────────────────

const CLIENT_ORIGIN = process.env.CLIENT_ORIGIN ?? 'http://localhost:5173';
const PORT = Number(process.env.PORT ?? 3001);

// ─── App Setup ───────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);

app.use(cors({ origin: CLIENT_ORIGIN, credentials: true }));
app.use(express.json());

// ─── In-Memory Room Storage ──────────────────────────────────────────────────

const rooms = new Map<string, Room>();

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

  // ── VIEWER: Join via one-time token ────────────────────────────────────
  socket.on('join_room', ({ token }) => {
    const found = getRoomByToken(token);

    if (!found) {
      socket.emit('join_error', { message: 'Кімнату не знайдено або посилання застаріло.' });
      return;
    }

    const { roomId, room } = found;

    if (room.viewerSocketId) {
      socket.emit('join_error', { message: 'Кімната заповнена (максимум 2 учасники).' });
      return;
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
      if (room.viewerSocketId) {
        io.to(room.viewerSocketId).emit('host_left');
      }
      rooms.delete(roomId);
      console.log(`[room] destroyed: ${roomId}`);
    } else if (role === 'viewer') {
      room.viewerSocketId = null;
      const hostSocket = io.sockets.sockets.get(room.hostSocketId);
      hostSocket?.emit('viewer_left');
      console.log(`[room] viewer slot freed: ${roomId}`);
    }
  });
});

// ─── Start ───────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🚀  Signaling server → http://localhost:${PORT}`);
  console.log(`    Client origin:    ${CLIENT_ORIGIN}\n`);
});
