/*
 * MA SÓI NIGHTFALL — máy chủ chuyển tin (relay)
 * ------------------------------------------------------------
 * - Phục vụ trang game (public/index.html) qua HTTP(S).
 * - Đường /ws: WebSocket chỉ CHUYỂN TIN giữa các trình duyệt.
 *   Luật chơi chạy ở trình duyệt của chủ phòng, máy chủ không biết vai.
 * - Không cần cài thư viện: chạy bằng Node.js 18+ (`node server.js`).
 */
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT) || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_FRAME = 1024 * 1024;          // 1 MB / tin
const ID_RE = /^[a-z0-9-]{6,64}$/;
const KEY_RE = /^[a-f0-9]{16,64}$/;
const RESERVE_MS = 30 * 60 * 1000;     // giữ mã phòng 30 phút cho chủ phòng cũ quay lại
const PING_MS = 25 * 1000;

/* ---------------- HTTP: phục vụ file tĩnh ---------------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok ' + clients.size); }
  let p = decodeURIComponent(url.pathname);
  if (p === '/' || p === '') p = '/index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, p));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('Không tìm thấy'); }
    res.writeHead(200, {
      'content-type': TYPES[path.extname(file)] || 'application/octet-stream',
      'cache-control': 'no-cache',
      'x-content-type-options': 'nosniff'
    });
    res.end(buf);
  });
});

/* ---------------- WebSocket tối giản (RFC 6455) ---------------- */
class Socket {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.frags = []; this.open = true; this.alive = true;
    this.onmessage = () => {}; this.onclose = () => {};
    sock.setNoDelay(true);
    sock.on('data', d => this._data(d));
    sock.on('close', () => this._closed());
    sock.on('error', () => this._closed());
  }
  _data(d) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    while (this.open) {
      const b = this.buf; if (b.length < 2) return;
      const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const hi = b.readUInt32BE(2); len = b.readUInt32BE(6); if (hi) return this.close(1009); off = 10; }
      if (len > MAX_FRAME) return this.close(1009);
      if (!masked) return this.close(1002);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4); off += 4;
      const payload = Buffer.from(b.subarray(off, off + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + len);
      this.alive = true;
      if (op === 0x8) return this.close(1000);
      if (op === 0x9) { this._send(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x2 || op === 0x0) {
        this.frags.push(payload);
        const total = this.frags.reduce((s, x) => s + x.length, 0);
        if (total > MAX_FRAME) return this.close(1009);
        if (fin) { const msg = Buffer.concat(this.frags).toString('utf8'); this.frags = []; try { this.onmessage(msg); } catch (e) { console.error(e); } }
      }
    }
  }
  _send(op, payload) {
    if (!this.open) return;
    const len = payload.length; let head;
    if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
    else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
    head[0] = 0x80 | op;
    try { this.sock.write(Buffer.concat([head, payload])); } catch { this._closed(); }
  }
  send(obj) { this._send(0x1, Buffer.from(JSON.stringify(obj), 'utf8')); }
  ping() { this._send(0x9, Buffer.alloc(0)); }
  close(code = 1000) {
    if (!this.open) return;
    const p = Buffer.alloc(2); p.writeUInt16BE(code, 0); this._send(0x8, p);
    this.sock.end(); setTimeout(() => this.sock.destroy(), 1000);
    this._closed();
  }
  _closed() { if (!this.open) return; this.open = false; try { this.sock.destroy(); } catch {} this.onclose(); }
}

/* ---------------- Bảng định tuyến ---------------- */
const clients = new Map();   // id -> { ws, key, links:Set<id> }
const reserved = new Map();  // id -> { key, until }  (giữ chỗ khi vừa rớt mạng)

function deliver(id, obj) { const c = clients.get(id); if (c) c.ws.send(obj); return !!c; }

function register(ws, id, key) {
  const now = Date.now();
  const cur = clients.get(id);
  if (cur && cur.key !== key) { ws.send({ type: 'error', code: 'unavailable-id', message: 'Mã phòng đã có người dùng.' }); return ws.close(4001); }
  const r = reserved.get(id);
  if (!cur && r && r.until > now && r.key !== key) { ws.send({ type: 'error', code: 'unavailable-id', message: 'Mã phòng vừa được dùng. Hãy đổi mã khác.' }); return ws.close(4001); }
  reserved.delete(id);
  const entry = { ws, key, links: new Set() };
  clients.set(id, entry);
  if (cur) { // cùng khóa: cùng người mở lại -> thay socket cũ
    cur.ws.close(4000);
    for (const other of cur.links) {
      const o = clients.get(other);
      if (o) { o.links.delete(id); o.ws.send({ type: 'peer-left', from: id }); }
    }
  }
  ws.send({ type: 'registered', id });

  ws.onmessage = raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (m.type === 'ping') return ws.send({ type: 'pong' });
    const to = typeof m.to === 'string' ? m.to : '';
    if (!ID_RE.test(to) || to === id) return;
    const target = clients.get(to);
    switch (m.type) {
      case 'connect':
        if (!target) return ws.send({ type: 'error', from: to, code: 'peer-unavailable', message: 'Không tìm thấy phòng.' });
        entry.links.add(to); target.links.add(id);
        target.ws.send({ type: 'incoming', from: id });
        break;
      case 'accept':
        if (target) { entry.links.add(to); target.links.add(id); target.ws.send({ type: 'accepted', from: id }); }
        break;
      case 'data':
        if (!target || !entry.links.has(to)) return ws.send({ type: 'error', from: to, code: 'peer-unavailable', message: 'Người nhận đã rời phòng.' });
        target.ws.send({ type: 'data', from: id, payload: m.payload });
        break;
      case 'disconnect':
        entry.links.delete(to);
        if (target) { target.links.delete(id); target.ws.send({ type: 'peer-left', from: id }); }
        break;
    }
  };
  ws.onclose = () => {
    if (clients.get(id) !== entry) return;   // đã bị socket mới thay thế
    clients.delete(id);
    reserved.set(id, { key, until: Date.now() + RESERVE_MS });
    for (const other of entry.links) {
      const o = clients.get(other);
      if (o) { o.links.delete(id); o.ws.send({ type: 'peer-left', from: id }); }
    }
  };
}

server.on('upgrade', (req, sock) => {
  const url = new URL(req.url, 'http://x');
  const wsKey = req.headers['sec-websocket-key'];
  if (url.pathname !== '/ws' || !wsKey || String(req.headers.upgrade).toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(wsKey + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const ws = new Socket(sock);
  const id = String(url.searchParams.get('id') || '').toLowerCase();
  const key = String(url.searchParams.get('key') || '').toLowerCase();
  if (!ID_RE.test(id) || !KEY_RE.test(key)) { ws.send({ type: 'error', code: 'invalid-id', message: 'Mã kết nối không hợp lệ.' }); return ws.close(4002); }
  register(ws, id, key);
});

/* Giữ kết nối sống + dọn chỗ giữ hết hạn */
setInterval(() => {
  for (const [, c] of clients) {
    if (!c.ws.alive) { c.ws.close(4003); continue; }
    c.ws.alive = false; c.ws.ping();
  }
  const now = Date.now();
  for (const [id, r] of reserved) if (r.until < now) reserved.delete(id);
}, PING_MS);

server.listen(PORT, () => console.log('🐺 Ma Sói Nightfall đang chạy tại http://localhost:' + PORT));
