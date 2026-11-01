#!/usr/bin/env python3
"""Test-owned escaped groups, controlled through files rather than child stdin."""
import os
import signal
import sys
import time

kind, directory = sys.argv[1:]


def record(name, value):
    with open(os.path.join(directory, name), "a", encoding="utf-8") as out:
        out.write(str(value) + "\n")


def handlers(label):
    def received(number, _frame):
        record(label + ".signals", f"{number} {time.monotonic_ns() // 1_000_000}")
    signal.signal(signal.SIGINT, received)
    signal.signal(signal.SIGTERM, received)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)


def escape(label):
    child = os.fork()
    if child == 0:
        if kind == "setsid":
            os.setsid()
        else:
            os.setpgid(0, 0)
        handlers(label)
        record(label + ".pid", os.getpid())
        while True:
            signal.pause()
    return child


handlers("root")
record("root.pid", os.getpid())
escape("descendant")
second = False
while True:
    if os.path.exists(os.path.join(directory, "exit")):
        sys.exit(0)
    if not second and os.path.exists(os.path.join(directory, "spawn_second")):
        escape("second")
        second = True
    try:
        if os.path.exists(os.path.join(directory, "flood")):
            os.write(1, b"x" * 8192 + b"\n")
        elif not os.path.exists(os.path.join(directory, "quiet")):
            os.write(1, b"tick\n")
    except BrokenPipeError:
        # Output cancellation must not end the signal-ordering fixture early.
        pass
    time.sleep(0.05)
