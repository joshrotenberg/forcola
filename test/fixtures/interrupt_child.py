#!/usr/bin/env python3
"""A test-owned child that records signals before either cooperating or waiting."""
import os
import signal
import sys
import time

mode, directory = sys.argv[1:]


def record(name, value):
    with open(os.path.join(directory, name), "a", encoding="utf-8") as out:
        out.write(str(value) + "\n")


def interrupt(*_):
    record("signals", "INT " + str(time.monotonic_ns() // 1_000_000))
    if mode == "cooperate":
        sys.exit(42)


def terminate(*_):
    record("signals", "TERM " + str(time.monotonic_ns() // 1_000_000))


signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, terminate)
record("pid", os.getpid())
print("ready", flush=True)
if mode == "flood":
    print("x" * 1024, flush=True)
while True:
    signal.pause()
