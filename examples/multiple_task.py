# Copyright (C) 2026 Fabrizio Pollastri <mxgbot@gmail.com>
# SPDX-License-Identifier: GPL-3.0-or-later

import time
import conductor


def busy_wait_us(duration_us):
    deadline = time.perf_counter_ns() + int(duration_us) * 1000
    while time.perf_counter_ns() < deadline:
        pass


def py_task(msg):
    print("py:", msg)


s = conductor.Scheduler()
s.add_task(
    task=busy_wait_us,
    args=(50,),
    period_us=1000,
    start="next_second",
    offset_us=100,
    count=1000,
    task_id="fast_py",
)
s.add_task(
    task=py_task,
    args=("tick",),
    period_us=200_000,
    start="asap",
    count=5,
    task_id="slow_py",
)
s.start_engine()

while s.read_stats()["running"]:
    time.sleep(0.05)

print(s.read_stats())
for job in s.read_jobs():
    print(job)
