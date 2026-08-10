# 📡 Screenshare Signaling Server (Backend)

The signaling server for the Screenshare & Intercom Web App. This server facilitates the initial connection (WebRTC signaling) between the Host and the Viewer using Socket.IO.

## ✨ Features
- **Room Management**: Creates secure, unique rooms for each session.
- **Authentication**: (Optional) Password protection for joining rooms.
- **Auto-cleanup**: Automatically cleans up empty rooms to save memory.
- **Smart Reconnection**: Allows viewers to seamlessly take over their slot if they refresh the page or drop connection temporarily.
- **CORS Configured**: Safely communicates with your frontend client.

## 🚀 Tech Stack
- **Node.js**
- **Express.js**
- **Socket.IO** (WebSockets)
- **TypeScript**

## 🛠 Installation & Setup

1. Install dependencies:
```bash
npm install
```

2. Create a `.env` file (you can copy `.env.example`):
```bash
cp .env.example .env
```

3. Configure your `.env`:
```env
PORT=3001
CLIENT_ORIGIN=http://localhost:5173 # URL of your frontend
ROOM_PASSWORD=optional_password
```

4. Start the development server (with nodemon/ts-node):
```bash
npm run dev
```

5. Build for production:
```bash
npm run build
```

## 🚢 Deployment
This Node.js server can be deployed to [Render](https://render.com/) or Railway as a Web Service. A `render.yaml` file is included in the project root for quick deployment on Render.
