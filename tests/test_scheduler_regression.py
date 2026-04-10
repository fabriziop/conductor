# Copyright (C) 2026 Fabrizio Pollastri <mxgbot@gmail.com>
# SPDX-License-Identifier: GPL-3.0-or-later

import time
import unittest
from pathlib import Path
import sys

# Prefer the local built extension when present to avoid importing an older
# system-wide installation from /usr/lib/python3/dist-packages.
_REPO_ROOT = Path(__file__).resolve().parents[1]
for _candidate in (_REPO_ROOT / "build", _REPO_ROOT / "build-deb"):
    if _candidate.exists() and any(_candidate.glob("conductor*.so")):
        sys.path.insert(0, str(_candidate))
        break

import conductor


def busy_wait_us(duration_us):
    deadline = time.perf_counter_ns() + int(duration_us) * 1000
    while time.perf_counter_ns() < deadline:
        pass


def wait_until_done(scheduler, timeout_s=5.0):
    deadline = time.perf_counter() + timeout_s
    while scheduler.read_stats()["running"]:
        if time.perf_counter() >= deadline:
            raise TimeoutError("scheduler did not finish in time")
        time.sleep(0.005)


class SchedulerRegressionTests(unittest.TestCase):
    def test_default_pool_workers_runtime_configurable(self):
        scheduler = conductor.Scheduler(default_pool_workers=3)
        default_stats = scheduler.read_pool_stats("default")
        self.assertEqual(default_stats["workers"], 3)

        with self.assertRaisesRegex(
            RuntimeError,
            "default_pool_workers must be >= 1",
        ):
            conductor.Scheduler(default_pool_workers=0)

    def test_rejects_non_callable_task(self):
        scheduler = conductor.Scheduler()
        with self.assertRaisesRegex(
            RuntimeError,
            "task must be a Python callable",
        ):
            scheduler.add_task("spin_us", args=(10,), period_us=1_000, count=1)

    def test_overlap_policies_and_stats(self):
        serial = conductor.Scheduler()
        serial.create_pool("serial_pool", workers=2)
        serial.add_task(
            busy_wait_us,
            args=(20_000,),
            period_us=5_000,
            count=2,
            pool_id="serial_pool",
            overlap_policy="serial",
        )
        serial.start_engine()
        wait_until_done(serial)
        serial_job = serial.read_jobs()[0]
        self.assertEqual(serial_job["overlap_policy"], "serial")
        self.assertEqual(serial_job["run_count"], 2)
        self.assertEqual(serial_job["skipped_count"], 0)

        overlap = conductor.Scheduler()
        overlap.create_pool("fast", workers=2)
        overlap.add_task(
            busy_wait_us,
            args=(50_000,),
            period_us=10_000,
            count=3,
            pool_id="fast",
            overlap_policy="overlap",
        )
        overlap.start_engine()
        wait_until_done(overlap)
        overlap_job = overlap.read_jobs()[0]
        self.assertEqual(overlap_job["overlap_policy"], "overlap")
        self.assertEqual(overlap_job["run_count"], 3)
        self.assertEqual(overlap_job["skipped_count"], 0)

        skip = conductor.Scheduler()
        skip.create_pool("skipper", workers=1)
        skip.add_task(
            busy_wait_us,
            args=(40_000,),
            period_us=10_000,
            count=2,
            pool_id="skipper",
            overlap_policy="skip",
        )
        skip.start_engine()
        wait_until_done(skip)
        skip_job = skip.read_jobs()[0]
        self.assertEqual(skip_job["overlap_policy"], "skip")
        self.assertEqual(skip_job["run_count"], 2)
        self.assertGreaterEqual(skip_job["skipped_count"], 1)

    def test_read_pool_stats_api_and_queue_wait_metrics(self):
        scheduler = conductor.Scheduler()
        scheduler.create_pool("fast", workers=2)
        scheduler.add_task(
            busy_wait_us,
            args=(100_000,),
            period_us=5_000,
            count=3,
            pool_id="fast",
            overlap_policy="overlap",
        )
        scheduler.start_engine()
        wait_until_done(scheduler)

        all_stats = scheduler.read_pool_stats()
        self.assertIn("default", all_stats)
        self.assertIn("fast", all_stats)

        fast_stats = scheduler.read_pool_stats("fast")
        self.assertEqual(fast_stats["id"], "fast")
        self.assertEqual(fast_stats["submitted"], 3)
        self.assertEqual(fast_stats["completed"], 3)
        self.assertEqual(fast_stats["queued"], 0)
        self.assertEqual(fast_stats["running"], 0)

        job = scheduler.read_jobs()[0]
        self.assertIn("queue_wait_mean_us", job)
        self.assertIn("queue_wait_max_us", job)
        self.assertGreaterEqual(job["queue_wait_mean_us"], 0.0)
        self.assertGreaterEqual(
            job["queue_wait_max_us"],
            job["queue_wait_mean_us"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
