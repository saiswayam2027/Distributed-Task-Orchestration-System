# Task Orchestrator

A distributed task orchestration system built from scratch in Java — a job queue with **at-least-once delivery**, **priority scheduling**, **worker heartbeats**, **exponential-backoff retries with jitter**, and a **dead-letter queue**. Think of it as a from-scratch reimplementation of the core of Celery / AWS SQS, built to understand how these systems provide their guarantees.

## Why I built this

At my internship I use Celery + Redis to run report-generation pipelines in production. This project is my answer to the question: *what would it take to build that machinery myself, and what guarantees does it actually provide?*

## Architecture

```
                 ┌────────────────────────────────────────────┐
   POST /api/tasks   │                Spring Boot app             │
  ───────────────►   │                                            │
                 │  TaskController ──► RedisTaskQueue.enqueue  │
                 │                          │                  │
                 │        ┌─────────────────┼────────────────┐ │
                 │        ▼                 ▼                ▼ │
                 │   worker-0 ...      worker-N        InflightReaper
                 │   (pull loop)      (pull loop)      (@Scheduled)
                 └────────┼─────────────────┼────────────────┼─┘
                          │                 │                │
                 ┌────────▼─────────────────▼────────────────▼─┐
                 │                    Redis                     │
                 │  queue:pending:{high,medium,low}  (ZSET)     │
                 │  queue:inflight                   (ZSET)     │
                 │  queue:dead                       (LIST)     │
                 │  task:{id}                        (STRING)   │
                 └──────────────────────────────────────────────┘
                          │ audit trail (best-effort)
                 ┌────────▼─────────────────────────────────────┐
                 │  PostgreSQL: task_records (durable history)  │
                 └──────────────────────────────────────────────┘
```

## Core design decisions

### At-least-once delivery via visibility timeouts (SQS pattern)
Dequeue does not remove a task — it atomically **moves** it from `pending` to `inflight` with a deadline (Lua script, so two workers can never grab the same task). If the worker acks, the task is deleted. If the worker crashes, the `InflightReaper` finds the expired inflight entry and returns it to `pending`. Crash-safety and graceful shutdown share this single mechanism.

### Priority + delay in one data structure
Each priority level is a Redis ZSET scored by "eligible-at" timestamp. Delayed tasks and backoff-retried tasks are just entries with a future score — no separate delay queue needed.

### Exponential backoff with full jitter
`delay = random(0, min(cap, base * 2^attempt))`. Without jitter, a batch of tasks failing together retries together and hammers the downstream dependency in synchronized waves (thundering herd).

### Pull-based workers = free backpressure
Workers pull when they have capacity. A saturated pool simply stops pulling; tasks wait safely in Redis. No load shedding logic needed.

### Redis for execution state, Postgres for history
Redis holds the hot queue state; Postgres keeps a durable audit trail of every task's lifecycle. Audit writes are deliberately best-effort — a history write failure never fails the task itself.

## Guarantees & non-guarantees (be honest in interviews)

| Property | Status |
|---|---|
| At-least-once delivery | ✅ via visibility timeout + reaper |
| No duplicate *simultaneous* delivery | ✅ atomic Lua dequeue |
| Exactly-once execution | ❌ impossible without idempotent handlers — a worker can crash *after* doing the work but *before* acking. Handlers should be idempotent. |
| Ordering | ❌ priority + eligibility ordering only; no FIFO guarantee across workers |

## Running it

```bash
docker compose up --build
```

Submit tasks:

```bash
# A normal task
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":"hello world","priority":"HIGH"}'

# A flaky task (fails ~50% of the time; watch it retry with backoff)
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"flaky","payload":"{}","maxRetries":5}'

# A doomed task (always fails; ends in the dead-letter queue)
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"doomed","payload":"{}","maxRetries":2}'

# A delayed task (eligible after 30s)
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":"later","delaySeconds":30}'
```

Observe:

```bash
curl localhost:8080/api/monitor          # workers, heartbeats, queue depths, counters
curl localhost:8080/api/tasks            # recent task history
curl "localhost:8080/api/tasks?status=DEAD"   # dead-lettered tasks
```

### Chaos test (the demo that sells the project)

1. Submit 100 `sleep` tasks: `for i in $(seq 100); do curl -s -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' -d '{"type":"sleep","payload":"{}"}' > /dev/null; done`
2. `docker compose kill app` mid-processing.
3. `docker compose up app` — watch the reaper recover the abandoned inflight tasks and complete all 100 with zero loss.

## Tests

```bash
mvn test   # requires Docker for Testcontainers integration tests
```

Highlights:
- **Concurrent dequeue test**: 8 threads racing over 200 tasks — asserts zero duplicate deliveries.
- **Crash recovery test**: simulates a worker crash and asserts the reaper makes the task visible again.
- **Backoff tests**: cap enforcement and jitter variance.

## Tech stack

Java 17, Spring Boot 3, Redis (Jedis, Lua scripting), PostgreSQL + Spring Data JPA, JUnit 5 + Testcontainers, Docker Compose.

## Extension ideas

- WebSocket dashboard for live queue/worker visualization
- Task chaining / DAG workflows (tasks that enqueue dependents on completion)
- Rate limiting per task type (token bucket in Redis)
- Horizontal scaling: run multiple app instances against the same Redis (already safe — the Lua dequeue and reaper races are handled)
