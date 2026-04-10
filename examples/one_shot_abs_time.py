# Copyright (C) 2026 Fabrizio Pollastri <mxgbot@gmail.com>
# SPDX-License-Identifier: GPL-3.0-or-later

import conductor
import time


def one_shot_task():
    print("one-shot task fired")


s = conductor.Scheduler()
s.add_task(
    task=one_shot_task,
    period_us=1000,
    start="2026-04-09 18:30:00.250",
    count=1,
)
s.start_engine()

while s.read_stats()["running"]:
    time.sleep(0.01)
