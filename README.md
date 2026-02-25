

# Reliable Background Job Processor

A high-reliability asynchronous task queue built with Python and Redis. This system implements the "Reliable Queue" pattern, ensuring no jobs are lost during worker crashes or network failures.

##  Key Engineering Challenges Solved

### 1. The "At-Least-Once" Delivery Guarantee

Standard queues often use `LPOP`, which removes a job immediately. If the worker crashes a millisecond later, that job is gone forever.

* **Solution:** I implemented the **Reliable Queue Pattern** using Redis `BRPOPLPUSH`. This moves a job to a "processing" list atomically. It only leaves that list once the job is explicitly completed or failed.

### 2. Intelligent Retry Strategy (Exponential Backoff)

Repeatedly slamming a failing API makes recovery harder for the downstream service.

* **Solution:** I built a custom **Scheduler** using Redis Sorted Sets (`ZSET`). If a job fails, it is scheduled for retry using the formula $2^{attempt}$ seconds. This provides a "cool-down" period for the system to stabilize.

### 3. Distributed State Machine

Jobs transition through a strictly defined lifecycle to ensure observability:
`QUEUED` → `RUNNING` → `COMPLETED` | `SCHEDULED` (for retry) | `FAILED` (Dead Letter Queue).

---

##  Tech Stack

* **Language:** Python 3.x
* **Data Store:** Redis (Lists for queues, Hashes for metadata, Sorted Sets for scheduling)
* **Library:** `redis-py`

##  System Architecture

The system consists of three decoupled components:

1. **Producer:** Enqueues tasks and generates unique UUIDs for job tracking.
2. **Worker:** Consumes tasks, manages the state machine, and handles execution logic.
3. **Scheduler:** Acts as a "watchdog" that monitors the retry set and moves ready jobs back to the main queue.

---

##  Getting Started

### 1. Prerequisites

* Python 3.8+
* Redis server (Local or Cloud)

### 2. Installation

```bash
git clone https://github.com/JessicaTemplet/Background-Job-Processor.git
cd job-processor
pip install redis

```

### 3. Running the Demo

Open three terminal windows:

1. **Terminal 1 (The Worker):** `python worker.py`
2. **Terminal 2 (The Scheduler):** `python scheduler.py`
3. **Terminal 3 (The Producer):** `python producer.py`

---

### 4. Monitoring (Optional)

In a fourth terminal, monitor your queues using Redis CLI:

```bash
redis-cli

# Check queue lengths
LLEN default_queue           # Jobs waiting to be processed
LLEN default_queue:processing # Jobs currently running
LLEN dead_letter_queue       # Permanently failed jobs

# Check scheduled retries
ZRANGE retry_set 0 -1 WITHSCORES  # Jobs waiting for retry with timestamps

# Inspect a specific job
HGETALL job:<job-id>  # Replace with actual job ID

# Real-time monitoring
MONITOR  # Watch all Redis commands in real-time
```

##  Future Improvements

* **Priority Queues:** Implement multiple Redis lists (high, medium, low) and have workers poll them in order of importance.
* **Worker Heartbeats:** Add a mechanism to detect if a worker has "hung" without crashing and re-queue its current task.
* **Web Dashboard:** A simple FastAPI front-end to visualize job counts in each state.
