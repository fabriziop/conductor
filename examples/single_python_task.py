# Copyright (C) 2026 Fabrizio Pollastri <mxgbot@gmail.com>
# SPDX-License-Identifier: GPL-3.0-or-later

import time
import conductor


def hello(name, n):
    print("hello", name, n)


s = conductor.Scheduler()
s.add_task(
    task=hello,
    args=("alice", 7),
    period_us=100_000,
    start="asap",
    count=3,
)
s.start_engine()

while s.read_stats()["running"]:
    time.sleep(0.01)

print(s.read_jobs())
