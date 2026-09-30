// Коллекторы онлайн — сервер-ретранслятор.
// Раздаёт страницу игры и пересылает сообщения между создателем стола и игроками.
// Саму партию считает браузер создателя стола; сервер хранит только, кто за каким столом.
// Без сторонних библиотек: нужен только Node.js 18+.

const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, "public");
const MAX_BODY = 512 * 1024;          // одно сообщение не больше 512 КБ
const HOST_GRACE_MS = 90 * 1000;      // сколько ждать создателя стола после обрыва связи
const ROOM_IDLE_MS = 6 * 3600 * 1000; // стол без активности удаляется через 6 часов

// clients: cid -> { res, room, role, lastSeen }
// rooms:   roomId -> { host: cid, guests: Set<cid>, touched, hostGoneAt }
const clients = new Map();
const rooms = new Map();

const okId = (s) => typeof s === "string" && /^[A-Za-z0-9_\-]{3,64}$/.test(s);

function sse(res, event, data) {
  if (!res) return false;
  try {
    if (event) res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
    return true;
  } catch (e) { return false; }
}
function deliver(cid, msg) {
  const c = clients.get(cid);
  return !!(c && c.res && sse(c.res, null, msg));
}

function openStream(req, res, q) {
  const cid = q.get("cid"), role = q.get("role"), room = q.get("room");
  if (!okId(cid) || (role !== "host" && role !== "guest") || (role === "host" && !okId(room))) {
    res.writeHead(400); return res.end("bad request");
  }
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.write("retry: 2000\n\n");

  if (role === "host") {
    const r = rooms.get(room);
    if (r && r.host !== cid && !r.hostGoneAt) { sse(res, "taken", {}); return res.end(); }
    if (!r) rooms.set(room, { host: cid, guests: new Set(), touched: Date.now(), hostGoneAt: 0 });
    else { r.hostGoneAt = 0; r.touched = Date.now(); }
  }
  const prev = clients.get(cid);
  if (prev && prev.res && prev.res !== res) { try { prev.res.end(); } catch (e) {} }
  clients.set(cid, { res, room: role === "host" ? room : (prev && prev.room) || null, role, lastSeen: Date.now() });
  sse(res, "ready", { cid });

  req.on("close", () => {
    const c = clients.get(cid);
    if (!c || c.res !== res) return;
    c.res = null;
    const r = c.room && rooms.get(c.room);
    if (!r) { clients.delete(cid); return; }
    if (role === "host") {
      r.hostGoneAt = Date.now();
      r.guests.forEach((g) => deliver(g, { type: "close", from: r.host }));
    } else {
      deliver(r.host, { type: "close", from: cid });
    }
  });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on("data", (c) => { size += c.length; if (size > MAX_BODY) { reject(new Error("too big")); req.destroy(); } else chunks.push(c); });
    req.on("end", () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8"))); } catch (e) { reject(e); } });
    req.on("error", reject);
  });
}

async function handleSend(req, res) {
  let m;
  try { m = await readBody(req); } catch (e) { res.writeHead(400); return res.end("bad body"); }
  const { from, room, to, type } = m || {};
  if (!okId(from) || !okId(room)) { res.writeHead(400); return res.end("bad ids"); }
  const r = rooms.get(room);
  if (!r || (r.hostGoneAt && r.host !== from)) { res.writeHead(404, { "Content-Type": "application/json" }); return res.end('{"error":"no-room"}'); }
  r.touched = Date.now();

  if (type === "hello") {                       // гость просится за стол
    if (from === r.host) { res.writeHead(400); return res.end("host"); }
    const c = clients.get(from);
    if (!c || !c.res) { res.writeHead(409); return res.end("no stream"); }
    c.room = room; r.guests.add(from);
    deliver(r.host, { type: "conn", from });
    res.writeHead(200, { "Content-Type": "application/json" }); return res.end('{"ok":true}');
  }
  if (type === "data") {                        // сообщение по столу
    const isHost = from === r.host;
    if (!isHost && !r.guests.has(from)) { res.writeHead(403); return res.end("not seated"); }
    if (!isHost && to !== r.host) { res.writeHead(403); return res.end("guests talk to host only"); }
    if (isHost && !r.guests.has(to)) { res.writeHead(404); return res.end("no such guest"); }
    const ok = deliver(to, { type: "data", from, data: m.data });
    res.writeHead(ok ? 200 : 410, { "Content-Type": "application/json" }); return res.end(ok ? '{"ok":true}' : '{"error":"offline"}');
  }
  if (type === "bye") {
    if (from === r.host) { r.guests.forEach((g) => deliver(g, { type: "close", from })); rooms.delete(room); }
    else { r.guests.delete(from); deliver(r.host, { type: "close", from }); }
    res.writeHead(200); return res.end("{}");
  }
  res.writeHead(400); res.end("bad type");
}

function serveStatic(req, res, pathname) {
  let file = pathname === "/" ? "/index.html" : pathname;
  file = path.normalize(file).replace(/^(\.\.[\/\\])+/, "");
  const full = path.join(PUBLIC, file);
  if (!full.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(full, (err, buf) => {
    if (err) { res.writeHead(404); return res.end("not found"); }
    const type = full.endsWith(".html") ? "text/html; charset=utf-8" : full.endsWith(".js") ? "text/javascript" : full.endsWith(".css") ? "text/css" : "application/octet-stream";
    res.writeHead(200, { "Content-Type": type, "Cache-Control": "no-cache" });
    res.end(buf);
  });
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://x");
  if (req.method === "GET" && u.pathname === "/events") return openStream(req, res, u.searchParams);
  if (req.method === "POST" && u.pathname === "/send") return handleSend(req, res);
  if (req.method === "GET" && u.pathname === "/health") { res.writeHead(200); return res.end("ok"); }
  if (req.method === "GET") return serveStatic(req, res, u.pathname);
  res.writeHead(405); res.end();
});
server.requestTimeout = 0;
server.headersTimeout = 65000;
server.keepAliveTimeout = 61000;

// пинг, чтобы хостинг не рвал тихие соединения, и уборка брошенных столов
setInterval(() => {
  const now = Date.now();
  clients.forEach((c) => { if (c.res) { try { c.res.write(": ping\n\n"); } catch (e) {} } });
  rooms.forEach((r, id) => {
    if ((r.hostGoneAt && now - r.hostGoneAt > HOST_GRACE_MS) || now - r.touched > ROOM_IDLE_MS) {
      r.guests.forEach((g) => deliver(g, { type: "close", from: r.host }));
      rooms.delete(id);
    }
  });
  clients.forEach((c, cid) => { if (!c.res && (!c.room || !rooms.has(c.room))) clients.delete(cid); });
}, 20000);

server.listen(PORT, () => console.log(`Коллекторы онлайн: http://localhost:${PORT}`));
