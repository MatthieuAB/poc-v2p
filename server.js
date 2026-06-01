/**
 * POC V2P — Serveur Node.js
 * Communication bidirectionnelle VRU ↔ Conducteur via WebSockets
 * Lancement : node server.js
 */

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const path       = require('path');
const os         = require('os');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const PORT           = process.env.PORT || 3000;
const ALERT_DISTANCE = 50;
const CLEANUP_DELAY  = 5000;

// { socketId: { role, lat, lng, timer } }
const positions = {};

// ── Haversine ─────────────────────────────────────────────────────────────────
function haversine(lat1, lng1, lat2, lng2) {
  const R   = 6371000;
  const toR = d => d * Math.PI / 180;
  const dLat = toR(lat2 - lat1);
  const dLng = toR(lng2 - lng1);
  const a = Math.sin(dLat/2)**2
          + Math.cos(toR(lat1)) * Math.cos(toR(lat2)) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ── Logique d'alerte + diffusion positions ────────────────────────────────────
function checkAndBroadcast(updatedId) {
  const updated = positions[updatedId];
  if (!updated || updated.lat === null) return;

  for (const [otherId, other] of Object.entries(positions)) {
    if (otherId === updatedId) continue;
    if (other.role === updated.role) continue;
    if (other.lat === null) continue;

    const dist   = haversine(updated.lat, updated.lng, other.lat, other.lng);
    const danger = dist <= ALERT_DISTANCE;

    // Alerte + position de l'autre → aux deux clients
    io.to(updatedId).emit('alert', {
      danger, distance: Math.round(dist),
      otherLat: other.lat, otherLng: other.lng,
      otherRole: other.role
    });
    io.to(otherId).emit('alert', {
      danger, distance: Math.round(dist),
      otherLat: updated.lat, otherLng: updated.lng,
      otherRole: updated.role
    });

    const tag = danger ? '🚨 ALERTE' : '✅ OK';
    console.log(`[${tag}] ${updated.role} ↔ ${other.role} — ${Math.round(dist)} m`);
  }
}

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`🔌 Connecté : ${socket.id}`);

  socket.on('register', (role) => {
    positions[socket.id] = { role, lat: null, lng: null, timer: null };
    console.log(`📱 ${role.toUpperCase()} : ${socket.id}`);
    socket.emit('registered', { role, alertDistance: ALERT_DISTANCE });
    broadcastStatus();
  });

  socket.on('position', ({ lat, lng }) => {
    const entry = positions[socket.id];
    if (!entry) return;
    entry.lat = lat;
    entry.lng = lng;

    if (entry.timer) clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      if (positions[socket.id]) {
        positions[socket.id].lat = null;
        positions[socket.id].lng = null;
        console.log(`🗑️  RGPD suppression : ${socket.id}`);
      }
    }, CLEANUP_DELAY);

    checkAndBroadcast(socket.id);
  });

  socket.on('disconnect', () => {
    const entry = positions[socket.id];
    if (entry?.timer) clearTimeout(entry.timer);
    delete positions[socket.id];
    console.log(`❌ Déconnecté : ${socket.id}`);
    broadcastStatus();
  });
});

function broadcastStatus() {
  const list  = Object.values(positions);
  io.emit('status', {
    vru:    list.filter(p => p.role === 'vru').length,
    driver: list.filter(p => p.role === 'driver').length
  });
}

server.listen(PORT, '0.0.0.0', () => {
  const nets = os.networkInterfaces();
  let ip = 'localhost';
  for (const ifaces of Object.values(nets))
    for (const i of ifaces)
      if (i.family === 'IPv4' && !i.internal) { ip = i.address; break; }

  console.log('\n🚀 Serveur V2P démarré !');
  console.log('─────────────────────────────────');
  console.log(`💻 Laptop     : http://localhost:${PORT}`);
  console.log(`📡 Téléphones : http://${ip}:${PORT}`);
  console.log(`⚠️  Seuil      : ${ALERT_DISTANCE} m`);
  console.log('─────────────────────────────────\n');
});
