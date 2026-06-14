/**
 * SupportView – WebSocket Signaling Server
 * 
 * KURULUM:
 *   npm install ws
 *   node server.js
 * 
 * Railway'de HTML dosyasını da serve etmek için:
 *   npm install ws
 *   node server.js
 *   → http://localhost:3001 adresinden HTML'e erişebilirsiniz
 */

const { WebSocketServer } = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3001;

// HTTP sunucusu (HTML dosyasını serve eder)
const httpServer = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    const filePath = path.join(__dirname, 'screen-share-support.html');
    if (fs.existsSync(filePath)) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      fs.createReadStream(filePath).pipe(res);
    } else {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('SupportView Signaling Server çalışıyor ✓\nHTML dosyası bulunamadı.');
    }
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// WebSocket sunucusu
const wss = new WebSocketServer({ server: httpServer });

// sessions: { code: { customer: ws, agent: ws } }
const sessions = {};

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

wss.on('connection', (ws) => {
  log('Yeni bağlantı');
  let myCode = null;
  let myRole = null;

  // Her bağlantı için ping-pong keep-alive
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const { type, code, data } = msg;

    // Ping'e cevap ver
    if (type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
      return;
    }

    // ── JOIN ────────────────────────────────────────────────────────────
    if (type === 'join') {
      myCode = code.toUpperCase();
      myRole = data.role;

      if (!sessions[myCode]) sessions[myCode] = {};
      sessions[myCode][myRole] = ws;

      log(`${myRole} oturuma katıldı: ${myCode}`);
      ws.send(JSON.stringify({ type: 'joined', code: myCode, role: myRole }));

      const other = myRole === 'customer' ? sessions[myCode].agent : sessions[myCode].customer;
      if (other && other.readyState === 1) {
        other.send(JSON.stringify({ type: 'peer_joined', role: myRole }));
        ws.send(JSON.stringify({ type: 'peer_joined', role: myRole === 'customer' ? 'agent' : 'customer' }));
      }
      return;
    }

    // ── RELAY ────────────────────────────────────────────────────────────
    if (['offer', 'answer', 'ice'].includes(type)) {
      const session = sessions[myCode];
      if (!session) return;

      const targetRole = myRole === 'customer' ? 'agent' : 'customer';
      const target = session[targetRole];

      if (target && target.readyState === 1) {
        target.send(JSON.stringify({ type, data }));
        log(`${type} iletildi: ${myRole} → ${targetRole} (${myCode})`);
      }
      return;
    }

    // ── LEAVE ────────────────────────────────────────────────────────────
    if (type === 'leave') {
      cleanup(ws, myCode, myRole);
    }
  });

  ws.on('close', () => cleanup(ws, myCode, myRole));
  ws.on('error', (err) => log('WS hata: ' + err.message));
});

// Ölü bağlantıları temizle (her 30 saniyede)
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(pingInterval));

function cleanup(ws, code, role) {
  if (!code || !role) return;
  const session = sessions[code];
  if (!session) return;

  if (session[role] === ws) {
    delete session[role];
    log(`${role} ayrıldı: ${code}`);

    const otherRole = role === 'customer' ? 'agent' : 'customer';
    const other = session[otherRole];
    if (other && other.readyState === 1) {
      other.send(JSON.stringify({ type: 'peer_left', role }));
    }
  }

  if (!session.customer && !session.agent) {
    delete sessions[code];
    log(`Oturum silindi: ${code}`);
  }
}

httpServer.listen(PORT, () => {
  log(`SupportView sunucusu çalışıyor → http://localhost:${PORT}`);
  log(`WebSocket → ws://localhost:${PORT}`);
});
