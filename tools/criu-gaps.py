#!/usr/bin/env python3
"""Show where a CRIU log spends its time: the largest jumps between consecutive timestamps.
usage: criu-gaps.py restore.log [min-seconds]"""
import re, sys
path, thresh = sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 0.3
prev, prevl = 0.0, ""
for line in open(path, errors="replace"):
    m = re.match(r"\((\d+\.\d+)\)", line)
    if not m:
        continue
    t = float(m.group(1))
    if t - prev > thresh:
        print(f"{prev:8.3f} -> {t:8.3f} (+{t-prev:.3f}s) after: {prevl.strip()[:140]}")
        print(f"{'':28}next: {line.strip()[:140]}")
    prev, prevl = t, line
print(f"total {prev:.3f}s")
