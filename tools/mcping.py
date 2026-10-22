#!/usr/bin/env python3
"""Minecraft server-list ping or offline-mode login probe.

  mcping.py host [port]                         ping: latency and the status JSON
  mcping.py host port --login NAME [--vhost H] [--protocol N] [--timeout S]
      log in (offline mode) and report how long until Login Success. Copes with what a real
      client copes with: lcm-gateway's hold keepalive (Set Compression -1) and real compression.
      --vhost is the hostname sent in the handshake (lcm-gateway routes on it).
"""
import argparse, json, socket, struct, sys, time, uuid, zlib

def varint(n):
    out = b""
    while True:
        b = n & 0x7F; n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n: return out

def read_varint_bytes(buf, i=0):
    n = shift = 0
    while True:
        b = buf[i]; i += 1
        n |= (b & 0x7F) << shift; shift += 7
        if not b & 0x80: return n, i

def read_varint(s):
    n = shift = 0
    while True:
        b = recv_exact(s, 1)[0]
        n |= (b & 0x7F) << shift; shift += 7
        if not b & 0x80: return n

def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk: raise EOFError("connection closed")
        buf += chunk
    return buf

def mcstr(x):
    b = x.encode()
    return varint(len(b)) + b

def handshake(vhost, port, protocol, intent):
    return varint(0) + varint(protocol) + mcstr(vhost) + struct.pack(">H", port) + varint(intent)

def frame(body):
    return varint(len(body)) + body

def ping(host, port, vhost, protocol):
    t0 = time.time()
    s = socket.create_connection((host, port), timeout=5)
    s.sendall(frame(handshake(vhost, port, protocol, 1)) + frame(varint(0)))
    read_varint(s); read_varint(s)
    status = json.loads(recv_exact(s, read_varint(s)))
    print(f"{(time.time()-t0)*1000:.0f}ms {status['version']['name']} players={status['players']['online']}/{status['players']['max']} motd={status.get('description')}")

def login(host, port, vhost, protocol, name, timeout):
    t0 = time.time()
    s = socket.create_connection((host, port), timeout=timeout)
    offline_uuid = uuid.uuid3(uuid.NAMESPACE_OID, "OfflinePlayer:" + name)  # any UUID; offline servers recompute it
    start = varint(0) + mcstr(name) + offline_uuid.bytes
    s.sendall(frame(handshake(vhost, port, protocol, 2)) + frame(start))
    threshold, keepalives = -1, 0
    while True:
        body = recv_exact(s, read_varint(s))
        if threshold >= 0:
            datalen, i = read_varint_bytes(body)
            body = zlib.decompress(body[i:]) if datalen else body[i:]
        pid, i = read_varint_bytes(body)
        if pid == 0x03:  # Set Compression
            threshold, _ = read_varint_bytes(body, i)
            if threshold >= 2**31: threshold -= 2**32
            if threshold < 0:
                keepalives += 1
            continue
        if pid == 0x02:
            print(f"login ok after {time.time()-t0:.1f}s (hold keepalives: {keepalives})")
            return 0
        if pid == 0x00:
            n, j = read_varint_bytes(body, i)
            print(f"disconnected after {time.time()-t0:.1f}s: {body[j:j+n].decode(errors='replace')}")
            return 1
        if pid == 0x01:
            print(f"server wants encryption (online mode) after {time.time()-t0:.1f}s")
            return 1
        if pid == 0x04:  # Login Plugin Request: answer "not understood"
            mid, _ = read_varint_bytes(body, i)
            s.sendall(frame(varint(0x02) + varint(mid) + b"\x00"))
            continue

ap = argparse.ArgumentParser()
ap.add_argument("host"); ap.add_argument("port", nargs="?", type=int, default=25565)
ap.add_argument("--login"); ap.add_argument("--vhost"); ap.add_argument("--protocol", type=int, default=773)
ap.add_argument("--timeout", type=float, default=300)
a = ap.parse_args()
vhost = a.vhost or a.host
if a.login:
    sys.exit(login(a.host, a.port, vhost, a.protocol, a.login, a.timeout))
ping(a.host, a.port, vhost, a.protocol)
