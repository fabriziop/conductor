# Copyright (C) 2026 Fabrizio Pollastri <mxgbot@gmail.com>
# SPDX-License-Identifier: GPL-3.0-or-later

import time

import conductor


def busy_wait_us(duration_us):
    deadline = time.perf_counter_ns() + int(duration_us) * 1000
    while time.perf_counter_ns() < deadline:
        pass


def wait_until_complete(scheduler, timeout_s=5.0):
    deadline = time.perf_counter() + timeout_s
    while scheduler.read_stats()["running"]:
        if time.perf_counter() >= deadline:
            raise TimeoutError("scheduler did not complete within timeout")
        time.sleep(0.01)


s = conductor.Scheduler()
s.create_pool("fast", workers=2)
s.create_pool("slow", workers=1)

start = "next_second"

s.add_task(
    busy_wait_us,
    args=(150_000,),
    start=start,
    count=2,
    task_id="fast_a",
    pool_id="fast",
    overlap_policy="overlap",
)
s.add_task(
    busy_wait_us,
    args=(150_000,),
    start=start,
    count=2,
    task_id="fast_b",
    pool_id="fast",
    overlap_policy="overlap",
)
s.add_task(
    busy_wait_us,
    args=(150_000,),
    start=start,
    count=2,
    task_id="slow_a",
    pool_id="slow",
    overlap_policy="serial",
)

t0 = time.perf_counter()
s.start_engine()
wait_until_complete(s)
elapsed_s = time.perf_counter() - t0

print(f"elapsed_s={elapsed_s:.3f}")
print("pool stats:")
for pool in s.list_pools():
    print(pool)

print("job stats:")
for job in s.read_jobs():
    print(job)
