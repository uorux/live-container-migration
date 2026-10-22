// holdproxy: minimal model of the proposed lcm-gateway "hold in login" behaviour.
// Accepts a client, reads Handshake (+ Login Start), then holds it for HOLD_S seconds while the
// "backend boots". During the hold it injects a keepalive every KA_S seconds (mode):
//   none      - send nothing (control: expect the client's 30 s ReadTimeoutHandler to fire)
//   setcomp   - Login "Set Compression" with threshold -1 (no-op when compression is off)
//   query     - Login "Custom Query" (plugin request) on an unknown channel; the client answers,
//               the proxy swallows the answers.
// Then it dials the backend, replays the buffered Handshake + Login Start, and splices.
// Status requests (next state 1) are answered locally with a "starting" MOTD.
// Proof of concept for docs/on-demand.md, not production code (query mode does not wait for the
// last answer before splicing).
// Usage: node holdproxy.js [listen=25600] [backend=127.0.0.1:25599] [mode=setcomp] [hold_s=120] [ka_s=10]
'use strict';
const net = require('net');
const [,, LISTEN = '25600', BACKEND = '127.0.0.1:25599', MODE = 'setcomp', HOLD_S = '120', KA_S = '10'] = process.argv;
const [bh, bp] = BACKEND.split(':');
const log = (...a) => console.log(new Date().toISOString(), ...a);

function readVarInt(buf, off) {
  let v = 0, s = 0, b;
  do {
    if (off >= buf.length) return null;
    b = buf[off++]; v |= (b & 0x7f) << s; s += 7;
    if (s > 35) throw new Error('varint too long');
  } while (b & 0x80);
  return [v | 0, off];
}
function varInt(v) {
  const out = []; v >>>= 0;
  do { let b = v & 0x7f; v >>>= 7; if (v) b |= 0x80; out.push(b); } while (v);
  return Buffer.from(out);
}
function packet(id, body) {
  const p = Buffer.concat([varInt(id), body]);
  return Buffer.concat([varInt(p.length), p]);
}
function str(s) { const b = Buffer.from(s, 'utf8'); return Buffer.concat([varInt(b.length), b]); }
// Split complete frames off the front of buf. Returns [frames, rest].
function frames(buf) {
  const out = []; let off = 0;
  for (;;) {
    const r = readVarInt(buf, off); if (!r) break;
    const [len, o] = r; if (o + len > buf.length) break;
    out.push(buf.subarray(o, o + len)); off = o + len;
  }
  return [out, buf.subarray(off)];
}

let n = 0;
net.createServer((c) => {
  const id = ++n; const t0 = Date.now();
  const el = () => ((Date.now() - t0) / 1000).toFixed(1) + 's';
  let buf = Buffer.alloc(0), replay = [], state = 'handshake', ka = null, qid = 1000, done = false;
  c.on('error', (e) => log(id, 'client error', e.code || e.message, el()));
  c.on('close', () => { done = true; clearInterval(ka); log(id, 'client closed', el()); });
  c.on('data', function onData(d) {
    buf = Buffer.concat([buf, d]);
    let fs; [fs, buf] = frames(buf);
    for (const f of fs) {
      const [pid, o] = readVarInt(f, 0);
      if (state === 'handshake') {
        let r = readVarInt(f, o); const proto = r[0];
        r = readVarInt(f, r[1]); const hl = r[0]; const host = f.subarray(r[1], r[1] + hl).toString();
        const next = f[r[1] + hl + 2];
        log(id, 'handshake', { proto, host: JSON.stringify(host), next });
        if (next === 1) { state = 'status'; continue; }
        replay.push(f); state = 'loginstart'; continue;
      }
      if (state === 'status') {
        if (pid === 0) {
          const j = { version: { name: 'starting', protocol: -1 }, players: { max: 0, online: 0 },
            description: { text: 'Server is starting... join to wait' } };
          c.write(packet(0, str(JSON.stringify(j))));
        } else if (pid === 1) { c.write(packet(1, f.subarray(o))); c.end(); }
        continue;
      }
      if (state === 'loginstart') {
        replay.push(f); state = 'held';
        log(id, 'login start; holding', HOLD_S, 's mode', MODE);
        startHold();
        continue;
      }
      if (state === 'held') {
        if (MODE === 'query' && pid === 0x02) { log(id, 'swallowed custom query answer', el()); continue; }
        log(id, 'unexpected client packet during hold', pid, el());
      }
    }
  });
  function startHold() {
    const kaMs = Number(KA_S) * 1000;
    if (MODE !== 'none') ka = setInterval(() => {
      if (done) return;
      if (MODE === 'setcomp') c.write(packet(0x03, varInt(-1)));
      else if (MODE === 'query') c.write(packet(0x04, Buffer.concat([varInt(qid++), str('lcm:hold')])));
      log(id, 'keepalive sent', MODE, el());
    }, kaMs);
    setTimeout(() => {
      clearInterval(ka);
      if (done) return;
      c.removeAllListeners('data');
      c.pause();
      log(id, 'backend ready; dialing', BACKEND, el());
      const b = net.connect(Number(bp), bh, () => {
        for (const f of replay) b.write(Buffer.concat([varInt(f.length), f]));
        if (buf.length) b.write(buf);
        c.pipe(b); b.pipe(c); c.resume();
        let first = true;
        b.on('data', () => { if (first) { first = false; log(id, 'first backend byte', el()); } });
      });
      b.on('error', (e) => { log(id, 'backend error', e.message); c.destroy(); });
      b.on('close', () => { log(id, 'backend closed', el()); c.end(); });
    }, Number(HOLD_S) * 1000);
  }
}).listen(Number(LISTEN), () => log('holdproxy on', LISTEN, '->', BACKEND, 'mode', MODE, 'hold', HOLD_S, 'ka', KA_S));
