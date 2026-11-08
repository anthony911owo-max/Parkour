// Servidor del juego + salas online (sin dependencias: solo Node.js 18+).
// Uso:  node server.js   (puerto con la variable PORT, por defecto 3000)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const MAX_ROOM = 8, MAX_MSG = 4096, MAX_PRESENCE = 2048;
const rooms = new Map();           // nombre -> Map(id -> cliente)
let nextId = 1;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    return fs.readFile(path.join(__dirname, 'index.html'), (e, b) => {
      if (e) { res.writeHead(500); return res.end('index.html no encontrado'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(b);
    });
  }
  res.writeHead(404); res.end('not found');
});

// ---- WebSocket minimo (RFC 6455) ----
function frame(op, payload) {
  const len = payload.length;
  const head = len < 126 ? Buffer.from([0x80 | op, len])
    : Buffer.from([0x80 | op, 126, len >> 8, len & 255]);
  return Buffer.concat([head, payload]);
}
const sendText = (c, obj) => { if (c.socket.writable) c.socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); };

function parseFrames(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const op = b[0] & 15, masked = (b[1] & 128) !== 0;
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) return c.socket.destroy();            // mensajes enormes: no admitidos
    if (!masked || len > MAX_MSG) return c.socket.destroy();
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4), data = Buffer.from(b.slice(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = b.slice(off + 4 + len);
    c.alive = true;
    if (op === 8) { c.socket.end(frame(8, Buffer.alloc(0))); return; }
    if (op === 9) { c.socket.write(frame(10, data)); continue; }
    if (op === 1) onMessage(c, data.toString('utf8'));
  }
}

function broadcast(roomName, exceptId, obj) {
  const r = rooms.get(roomName); if (!r) return;
  for (const [id, c] of r) if (id !== exceptId) sendText(c, obj);
}
function leave(c) {
  if (!c.room) return;
  const r = rooms.get(c.room);
  if (r) { r.delete(c.id); broadcast(c.room, c.id, { t: 'l', id: c.id }); if (!r.size) rooms.delete(c.room); }
  c.room = null;
}
function merge(target, patch) {
  for (const k of Object.keys(patch)) {
    if (!/^[a-z]{1,4}$/.test(k)) continue;
    const v = patch[k];
    if (v === null) delete target[k];
    else if (typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean') target[k] = typeof v === 'string' ? v.slice(0, 24) : v;
  }
}
function onMessage(c, text) {
  const now = Date.now();
  if (now - c.t > 1000) { c.t = now; c.n = 0; }
  if (++c.n > 80) return;                                         // limite de mensajes por segundo
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'join' && !c.room) {
    const name = String(m.room || '');
    if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name)) return sendText(c, { t: 'err', m: 'nombre de sala invalido' });
    let r = rooms.get(name); if (!r) { r = new Map(); rooms.set(name, r); }
    if (r.size >= MAX_ROOM) return sendText(c, { t: 'err', m: 'sala llena' });
    c.room = name; merge(c.p, m.p && typeof m.p === 'object' ? m.p : {});
    sendText(c, { t: 'hi', id: c.id });
    sendText(c, { t: 's', peers: [...r].map(([id, o]) => ({ id, p: o.p })) });
    r.set(c.id, c);
    broadcast(name, c.id, { t: 'u', id: c.id, d: c.p });
  } else if (m.t === 'p' && c.room && m.d && typeof m.d === 'object') {
    const before = JSON.stringify(c.p);
    merge(c.p, m.d);
    if (JSON.stringify(c.p).length > MAX_PRESENCE) { c.p = JSON.parse(before); return; }
    broadcast(c.room, c.id, { t: 'u', id: c.id, d: m.d });
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (req.url.split('?')[0] !== '/ws' || !key) return socket.destroy();
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const c = { socket, id: 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 6), room: null, p: {}, buf: Buffer.alloc(0), alive: true, t: 0, n: 0 };
  socket.on('data', d => parseFrames(c, d));
  socket.on('close', () => leave(c));
  socket.on('error', () => socket.destroy());
});

// ping periodico: cierra conexiones muertas
setInterval(() => {
  for (const r of rooms.values()) for (const c of r.values()) {
    if (!c.alive) { c.socket.destroy(); continue; }
    c.alive = false; if (c.socket.writable) c.socket.write(frame(9, Buffer.alloc(0)));
  }
}, 25000).unref();

server.listen(PORT, () => console.log('Parkour online en http://localhost:' + PORT));
module.exports = server;
