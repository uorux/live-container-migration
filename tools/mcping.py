#!/usr/bin/env python3
"""Minecraft server-list ping: prints latency and the status JSON. usage: mcping.py host [port]"""
import json, socket, struct, sys, time

def varint(n):
    out = b""
    while True:
        b = n & 0x7F; n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n: return out

def read_varint(s):
    n = shift = 0
    while True:
        b = s.recv(1)[0]
        n |= (b & 0x7F) << shift; shift += 7
        if not b & 0x80: return n

def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk: raise EOFError
        buf += chunk
    return buf

host, port = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 25565
t0 = time.time()
s = socket.create_connection((host, port), timeout=5)
hs = varint(0) + varint(773) + varint(len(host)) + host.encode() + struct.pack(">H", port) + varint(1)
s.sendall(varint(len(hs)) + hs + varint(1) + varint(0))
read_varint(s); read_varint(s)
status = json.loads(recv_exact(s, read_varint(s)))
print(f"{(time.time()-t0)*1000:.0f}ms {status['version']['name']} players={status['players']['online']}/{status['players']['max']} motd={status.get('description')}")
