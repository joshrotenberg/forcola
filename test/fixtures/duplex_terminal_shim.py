#!/usr/bin/env python3
"""Small framed-protocol fixture for terminal evidence; spawns no process."""

import json
import struct
import sys


def frame(tag, payload):
    packet = bytes([tag]) + payload
    sys.stdout.buffer.write(struct.pack(">I", len(packet)) + packet)
    sys.stdout.buffer.flush()


header = sys.stdin.buffer.read(4)
if len(header) != 4:
    raise SystemExit("missing spawn")
length = struct.unpack(">I", header)[0]
if length > 65536:
    raise SystemExit("oversize spawn")
packet = sys.stdin.buffer.read(length)
if len(packet) != length or packet[0] != 1:
    raise SystemExit("invalid spawn")

argv = json.loads(packet[1:])["argv"]
mode = argv[0]
exited = False

if mode == "script":
    # argv[1] is a JSON list of ["out" | "err", text] and ["exit", status]
    # steps. Each output step is one frame, so tests control frame boundaries.
    for kind, value in json.loads(argv[1]):
        if kind == "out":
            frame(0x11, value.encode())
        elif kind == "err":
            frame(0x12, value.encode())
        elif kind == "exit":
            report = {"status": value, "confirmed": True, "contained": False}
            frame(0x13, json.dumps(report).encode())
            exited = True
else:
    frame(0x11, b"ready\n")

if mode == "malformed_exit":
    # A terminal report with neither status nor signal. No mode may confirm
    # cleanup from it, and none may crash on it.
    frame(0x13, b'{"confirmed":true,"contained":false}')
    raise SystemExit(0)

while True:
    header = sys.stdin.buffer.read(4)
    if len(header) != 4:
        break
    length = struct.unpack(">I", header)[0]
    if length > 65536:
        raise SystemExit("oversize frame")
    packet = sys.stdin.buffer.read(length)
    if len(packet) != length:
        break
    if packet[0] == 4:
        # A scripted exit was already reported; a late KILL gets no second one.
        if mode == "timeout" or exited:
            continue
        frame(
            0x13,
            json.dumps(
                {
                    "status": 7,
                    "confirmed": mode != "unconfirmed",
                    "contained": mode == "contained",
                }
            ).encode(),
        )
        break
