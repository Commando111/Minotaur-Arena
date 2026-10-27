// Minotaur's Arena game server. No packages needed: just Node.js 18 or newer.
// Serves the game page and relays live game messages between the phones in each room.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const INDEX = fs.readFileSync(path.join(__dirname, 'index.html'));
const MAX_PER_ROOM = 12;
const MAX_FRAME = 16 * 1024;

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://local');
  if (u.pathname === '/' || u.pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return res.end(INDEX);
  }
  if (u.pathname === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found');
});

/* ---------- minimal WebSocket (RFC 6455) ---------- */
function sendFrame(sock, opcode, payload) {
  if (sock.destroyed) return;
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  sock.write(Buffer.concat([head, payload]));
}
const sendText = (sock, obj) => sendFrame(sock, 0x1, Buffer.from(JSON.stringify(obj)));

function attachFrames(sock, onText, onClose) {
  let buf = Buffer.alloc(0), parts = [];
  sock.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_FRAME || !masked) { sock.destroy(); return; }
      if (buf.length < off + 4 + len) return;
      const key = buf.subarray(off, off + 4), data = Buffer.from(buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= key[i & 3];
      buf = buf.subarray(off + 4 + len);
      if (op === 0x8) { sendFrame(sock, 0x8, Buffer.alloc(0)); sock.end(); return; }
      if (op === 0x9) { sendFrame(sock, 0xA, data); continue; }
      if (op === 0xA) { sock.alive = true; continue; }
      if (op === 0x1 || op === 0x0) {
        parts.push(data);
        if (fin) { const text = Buffer.concat(parts).toString('utf8'); parts = []; if (text.length <= MAX_FRAME) onText(text); }
      }
    }
  });
  sock.on('close', onClose);
  sock.on('error', () => sock.destroy());
}

/* ---------- rooms ---------- */
const rooms = new Map(); // room code -> Map(peer id -> { sock, presence })
const peersOf = r => [...r.entries()].map(([id, c]) => ({ id, presence: c.presence }));
function broadcast(r, obj) { const s = Buffer.from(JSON.stringify(obj)); for (const c of r.values()) sendFrame(c.sock, 0x1, s); }

server.on('upgrade', (req, sock) => {
  const u = new URL(req.url, 'http://local');
  const key = req.headers['sec-websocket-key'];
  if (u.pathname !== '/ws' || !key) { sock.destroy(); return; }
  const code = (u.searchParams.get('room') || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
  const id = (u.searchParams.get('id') || '').replace(/[^a-z0-9]/gi, '').slice(0, 24);
  if (!code || !id) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);

  let r = rooms.get(code);
  if (!r) { r = new Map(); rooms.set(code, r); }
  if (r.size >= MAX_PER_ROOM && !r.has(id)) { sendFrame(sock, 0x8, Buffer.alloc(0)); sock.end(); return; }
  const prev = r.get(id);
  if (prev) prev.sock.destroy();
  const me = { sock, presence: prev ? prev.presence : {} };
  r.set(id, me);
  sock.alive = true;
  sockets.add(sock);
  broadcast(r, { t: 'peers', peers: peersOf(r) });

  let budget = 80, last = Date.now();
  attachFrames(sock, text => {
    const now = Date.now(); budget = Math.min(80, budget + (now - last) / 1000 * 40); last = now;
    if (budget < 1) return; budget--;
    let m; try { m = JSON.parse(text); } catch (e) { return; }
    if (m.t === 'emit' && typeof m.topic === 'string' && /^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic)) {
      broadcast(r, { t: 'msg', topic: m.topic, data: m.data, from: id });
    } else if (m.t === 'presence' && m.p && typeof m.p === 'object' && !Array.isArray(m.p)) {
      me.presence = m.p; broadcast(r, { t: 'peers', peers: peersOf(r) });
    }
  }, () => {
    sockets.delete(sock);
    if (r.get(id) !== me) return;
    r.delete(id);
    if (r.size === 0) rooms.delete(code); else broadcast(r, { t: 'peers', peers: peersOf(r) });
  });
});

const sockets = new Set();
setInterval(() => {
  for (const s of sockets) { if (s.alive === false) { s.destroy(); continue; } s.alive = false; sendFrame(s, 0x9, Buffer.alloc(0)); }
}, 25000);

server.listen(PORT, () => console.log("Minotaur's Arena is running on port " + PORT));
