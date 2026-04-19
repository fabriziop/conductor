# conductor

A lightweight periodic task scheduler exposed to Python through `pybind11`, built on top of `Boost.Asio` timers.

`conductor` schedules Python callables on named thread pools with microsecond period configuration, flexible start-time syntax, optional run limits, overlap policies, and runtime timing statistics.

## Table of contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Build and installation](#build-and-installation)
  - [Option A: Build wheel with Python packaging](#option-a-build-wheel-with-python-packaging)
  - [Option B: Build with CMake](#option-b-build-with-cmake)
  - [Option C: Build and install a Debian package](#option-c-build-and-install-a-debian-package)
- [Quick start](#quick-start)
- [API reference](#api-reference)
  - [`Scheduler` class](#scheduler-class)
  - [`add_task(...)`](#add_task)
  - [`create_pool(...)`, `remove_pool(...)`, `list_pools()`, `read_pool_stats(...)`](#create_pool-remove_pool-list_pools-read_pool_stats)
  - [`start_engine()`, `stop()`, `clear_tasks()`](#start_engine-stop-clear_tasks)
  - [`read_stats()`](#read_stats)
  - [`read_jobs()`](#read_jobs)
  - [`suspend(...)`](#suspend)
  - [`resume(...)`](#resume)
- [Task model and timing semantics](#task-model-and-timing-semantics)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Notes and limitations](#notes-and-limitations)
- [License](#license)

## Overview

The module exports one main class:

- `conductor.Scheduler`

A `Scheduler` instance keeps an internal async event loop (single-threaded `Boost.Asio` `io_context`) and runs due tasks from worker threads once started.

Typical flow:

1. Create scheduler.
2. Add one or more Python callables via `add_task(...)`.
3. Start engine with `start_engine()`.
4. Poll status/stats.
5. Stop or wait for natural completion.

## Features

- Schedule by period in microseconds (`period_us`)
- Start at:
  - immediate (`"asap"`, `"now"`)
  - aligned boundaries (`"next_second"`, `"next_minute"`, `"next_hour"`)
  - explicit local wall clock (`"YYYY-MM-DD HH:MM:SS[.frac]"`)
  - numeric epoch timestamps
- Optional start offset in microseconds (`offset_us`)
- Optional finite run count (`count`) or run forever
- Named thread-pool registry managed from Python at runtime
- Per-task pool assignment with `pool_id`
- Per-task overlap policy with `overlap_policy`
- Task-level and global timing statistics:
  - run counters
  - skipped tick counters
  - lateness counters
  - mean absolute timing error
  - standard deviation (global)
- Pool-level counters:
  - queued
  - running
  - completed
  - submitted
- Per-task queueing latency metrics:
  - `queue_wait_mean_us`
  - `queue_wait_max_us`

## Requirements

- C++17 compiler
- CMake >= 3.18
- Python >= 3.9
- `pybind11`
- `Boost` (Asio)
- For Python packaging: `scikit-build-core`

## Build and installation

### Option A: Build wheel with Python packaging

The project is configured via `pyproject.toml` using `scikit-build-core`.

```bash
python -m pip install --upgrade pip
python -m pip install build scikit-build-core pybind11
python -m build
python -m pip install dist/*.whl
```

A helper script is available:

```bash
./build.sh
```

### Option B: Build with CMake

```bash
cmake -S . -B build
cmake --build build -j
```

Then make the module importable from the CMake build directory:

```bash
PYTHONPATH=build python -c "import conductor; print(conductor.Scheduler)"
```

### Option C: Build and install a Debian package

The repository includes a packaging helper that creates a `.deb` for the current
Debian architecture:

```bash
./build_deb.sh
```

The package is written under `debpkg/` and named like:

```text
debpkg/python3-conductor_<version>-1_<arch>.deb
```

Install it with `apt` or `dpkg`:

```bash
sudo apt install ./debpkg/python3-conductor_*.deb
```

or:

```bash
sudo dpkg -i ./debpkg/python3-conductor_*.deb
sudo apt-get install -f
```

After installation, verify the module is importable from the system Python:

```bash
python3 -c "import conductor; print(conductor.Scheduler)"
```

## Quick start

```python
import time
import conductor


def task(name, n):
    print("tick", name, n)


s = conductor.Scheduler()
s.create_pool("fast", workers=2)

s.add_task(
    task=task,
    args=("alice", 7),
    period_us=100_000,
    start="next_second",
    offset_us=200,
    count=3,
    pool_id="fast",
    overlap_policy="serial",
)
s.start_engine()

while s.read_stats()["running"]:
    time.sleep(0.01)

print(s.read_stats())
print(s.read_jobs())
```

## API reference

### `Scheduler` class

Construct a new scheduler:

```python
s = conductor.Scheduler(default_pool_workers=1)
```

`default_pool_workers` configures how many worker threads are created for the built-in
`"default"` pool at runtime. It must be `>= 1`.

### `add_task(...)`

```python
task_id = s.add_task(
    task,
    args=None,
    period_us=1000,
    start=None,
    offset_us=0,
    count=None,
    task_id=None,
    pool_id=None,
    overlap_policy=None,
)
```

Adds one task to the scheduler queue.

Parameters:

- `task`: Python callable
- `args`:
  - `None`, tuple, list, or single object
  - normalized internally to a tuple
- `period_us` (`int`): execution period in microseconds, must be `> 0`
- `start`:
  - `None` (current time)
  - string keyword/date-time
  - numeric timestamp
- `offset_us` (`int`): added to resolved start time
- `count`:
  - `None` => run forever
  - positive integer => run exactly that many times
- `task_id` (`str|None`): custom ID; autogenerated as `job_N` when omitted
- `pool_id` (`str|None`): target thread pool; defaults to `"default"`
- `overlap_policy` (`str|None`): one of `"serial"`, `"overlap"`, `"skip"`; defaults to `"serial"`

Returns:

- task ID (`str`)

Errors:

- adding while running raises runtime error
- invalid period/count/start/task raises runtime error

### `create_pool(...)`, `remove_pool(...)`, `list_pools()`, `read_pool_stats(...)`

```python
s.create_pool("fast", workers=4)
s.create_pool("io", workers=2)
print(s.list_pools())
s.remove_pool("io")
```

- `create_pool(pool_id, workers=1)`:
  - creates a named thread pool
  - `workers` must be `>= 1`
- `remove_pool(pool_id)`:
  - removes a pool
  - cannot remove the built-in `"default"` pool
  - cannot remove pools while the scheduler is running
  - cannot remove a pool that is still referenced by queued tasks
- `list_pools()`:
  - returns a list of dicts like `{ "id": "fast", "workers": 4, "queued": 0, "running": 0, "completed": 12, "submitted": 12 }`
- `read_pool_stats(pool_id=None)`:
  - with `pool_id=None`: returns a dict keyed by pool id
  - with `pool_id="fast"`: returns one dict for that pool
  - raises `unknown pool` for non-existing pool ids

### `start_engine()`, `stop()`, `clear_tasks()`

- `start_engine()`:
  - starts worker thread and timer loop
  - requires at least one queued task
- `stop()`:
  - requests stop, cancels active timers, joins worker thread
  - safe to call from destructor/shutdown paths
- `clear_tasks()`:
  - removes all queued jobs and resets accumulated stats
  - cannot be called while running

### `read_stats()`

Returns a dict with global scheduler metrics:

- `running` (`bool`)
- `task_count` (`int`)
- `total_runs` (`int`)
- `late_runs` (`int`)
- `max_late_us` (`float`)
- `mean_abs_error_us` (`float`)
- `stddev_abs_error_us` (`float`)

### `read_jobs()`

Returns a list of dicts, one per task/job:

- `id`
- `kind` (`"python"`)
- `name` (`"<python>"`)
- `pool_id` (`str`)
- `overlap_policy` (`"serial"`, `"overlap"`, or `"skip"`)
- `active` (`bool`)
- `in_flight` (`int`)
- `run_count` (`int`)
- `skipped_count` (`int`)
- `late_count` (`int`)
- `max_late_us` (`float`)
- `mean_abs_error_us` (`float`)
- `queue_wait_mean_us` (`float`)
- `queue_wait_max_us` (`float`)
- `finished_naturally` (`bool`)
- `task_error` (`str`, empty when no error)
- `period_us` (`int`)
- `remaining` (`int` or `"forever"`)
- `suspended` (`bool`)
- `resume_after_slots` (`int` or `"forever"`)

### `suspend(...)`

```python
pending = s.suspend(task_id=None, for_=0)
```

Suspend a task so its scheduled execution slots are skipped until it resumes.

`for_` is used in Python because `for` is a reserved keyword.

Parameters:

- `task_id` (`str|None`):
  - `"job_1"` (or custom id) to suspend a specific task
  - `None` to target the current running task (only valid when called from inside a task callback)
- `for_` (`int`):
  - `0` to suspend until task execution is explicitly resumed
  - `N` to suspend the next `N` scheduled times, then resume automatically

Returns:

- `resume_after_slots` (`int` or `"forever"`) after applying the request

Notes:

- works while scheduler is running or before start
- can be called externally, by one task for another task, or by the task itself
- control is scheduler-wide, not pool-local: a task running in one exec pool can suspend a task in another pool by `task_id`
- suspended slots increment `skipped_count` and keep periodic cadence (the task is not dispatched for those slots)

### `resume(...)`

```python
pending = s.resume(task_id=None, after=0)
```

Resume a suspended task immediately or after a further number of scheduled times.

Parameters:

- `task_id` (`str|None`):
  - `"job_1"` (or custom id) to resume a specific task
  - `None` to target the current running task (only valid when called from inside a task callback)
- `after` (`int`):
/home/fabrizio/inrim/gscv/conductor/src/conductor.cpp  - `0` to resume at the next scheduled time
  - `N` to resume after `N` additional scheduled times have been skipped

Returns:

- `resume_after_slots` (`int` or `"forever"`) after applying the request

Notes:

- works while scheduler is running or before start
- can be called externally, by one task for another task, or by the task itself
- control is scheduler-wide, not pool-local: a task running in one exec pool can resume a task in another pool by `task_id`
- when `after > 0`, the task stays suspended for that many more scheduled times before execution resumes

## Task model and timing semantics

### Execution model

- The scheduler keeps one internal `Boost.Asio` timer/event thread.
- When a task becomes due, the scheduler submits its work to the task's assigned thread pool.
- Different tasks can run in parallel when their pools have available worker threads.
- Python callables execute under the Python GIL, so CPU-bound Python work can still serialize.

### Overlap policies

- `serial`:
  - default behavior
  - next invocation is scheduled only after the previous invocation finishes
- `overlap`:
  - next invocation is armed immediately on each tick
  - multiple invocations of the same task may overlap as queue entries when pool workers are available
- `skip`:
  - keeps a fixed-rate schedule
  - if a tick occurs while a previous invocation of the same task is still running, that tick is skipped
  - skipped ticks are counted in `skipped_count`

### Phase stability of aligned starts

When `start` is set to a boundary alias (`"next_second"`, `"next_minute"`, `"next_hour"`) combined with an `offset_us`, the first deadline is anchored to an exact UTC boundary plus the offset. Every subsequent deadline is computed by **adding** `period_us` to the previous deadline — never recomputed from wall clock — so the phase relative to each boundary remains constant indefinitely.

Example: `start="next_second"`, `offset_us=100_000`, `period_us=1_000_000` fires at T+0.100 s, T+1.100 s, T+2.100 s, … with no drift.

The same guarantee holds for `"next_minute"`, `"next_hour"`, and explicit wall-clock strings.

> **Caveat:** If the scheduler falls more than ~10 periods behind (e.g. after a system suspend), it reanchors to `now + period` to avoid an unbounded backlog. This reanchor shifts the phase. Under normal continuous operation it does not occur.

### Start values

`start` accepts:

- `None` => now
- string aliases:
  - `"asap"`
  - `"now"`
  - `"next_second"`
  - `"next_minute"`
  - `"next_hour"`
- explicit local date-time string:
  - `"YYYY-MM-DD HH:MM:SS"`
  - optional fractional seconds: `"YYYY-MM-DD HH:MM:SS.fffffffff"`
- numeric:
  - if value `< 1e12`: treated as epoch seconds (float/int)
  - otherwise: treated as epoch nanoseconds

### Periodic behavior

For `serial` tasks, next deadline is advanced after completion.

For `overlap` and `skip` tasks, next deadline is advanced when the timer fires, preserving a fixed-rate schedule.

If execution drifts far behind (more than roughly 10 periods), scheduler reanchors future deadline to `now + period` to avoid unbounded backlog.

### Error handling

If a task throws an exception:

- the specific job is deactivated
- its `task_error` is populated
- other jobs continue running

## Examples

Project examples are stored under `examples/`.

- `single_python_task.py`: Python script that schedules a Python callable
- `multiple_task.py`: schedules multiple Python tasks with different rates
- `multi_pool_concurrent.py`: demonstrates custom pools, overlapping schedules, and pool stats
- `one_shot_abs_time.py`: one-shot run at explicit absolute wall clock time

Run examples from CMake targets (if built in `build/`):

```bash
cmake --build build --target run_example_single_python_task
cmake --build build --target run_example_multiple_task
cmake --build build --target run_example_multi_pool_concurrent
```

Or run directly:

```bash
PYTHONPATH=build python examples/multiple_task.py
```

Run regression tests:

```bash
cmake --build build --target run_regression_tests
```

## Troubleshooting

### `ImportError: No module named conductor`

- Ensure the wheel is installed in the current Python environment, or
- set `PYTHONPATH` to your CMake build directory containing the built module.

### `task must be a Python callable`

- Pass a Python function, lambda, or callable object as `task`.

### `cannot add tasks while scheduler is running`

- Stop the scheduler first (`stop()`), or
- use `clear_tasks()` and `add_task(...)` for a new run.

### `unknown pool: name`

- Create the pool first with `create_pool(...)`, or
- omit `pool_id` to use the built-in `"default"` pool.

### `cannot remove pool assigned to existing tasks`

- Clear queued tasks first with `clear_tasks()`, or
- re-add them with a different `pool_id`.

### `overlap_policy must be one of: serial, overlap, skip`

- Use one of the supported strings exactly, or
- omit the parameter to use the default `"serial"` behavior.

### `no tasks scheduled`

- call `add_task(...)` before `start_engine()`.

## Notes and limitations

- Timer scheduling is centralized in one internal `io_context(1)` thread.
- Task execution happens on per-pool worker threads.
- Python callbacks execute with GIL acquired.
- Busy Python callbacks can increase jitter/lateness.
- `overlap` can queue multiple simultaneous invocations of the same task.
- `skip` trades completeness for schedule regularity when a task cannot keep up with its period.
- The built-in `"default"` pool has one worker to preserve conservative default behavior.
- Absolute wall-clock parsing uses local time conversion.

## License

This project is licensed under the GNU General Public License v3.0 or later (GPL-3.0-or-later).

See the [LICENSE](LICENSE) file for the full text.

## Author

Fabrizio Pollastri <mxgbot@gmail.com>

## Copyright

Copyright (C) 2026 Fabrizio Pollastri
