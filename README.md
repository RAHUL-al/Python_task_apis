# Job Scheduler & Execution Engine API

Flask + SQLite backend for scheduling and executing jobs with retry logic and concurrency safety.

## Setup

```bash
pip install -r requirements.txt
python app.py
```

- Server: http://127.0.0.1:5000
- Swagger UI: http://127.0.0.1:5000/apidocs/

## Files

- `models.py` — Database models (Job, JobExecution)
- `app.py` — Flask app, API routes, validation, background worker
- `database.db` — SQLite database (created automatically on first run)
- `requirements.txt` — Dependencies

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/jobs` | Create a new job |
| GET | `/jobs` | List jobs (filter by `status`, `schedule_type`, `next_before`) |
| GET | `/jobs/<id>` | Job details + execution history |
| POST | `/jobs/<id>/pause` | Pause a scheduled job |
| POST | `/jobs/<id>/resume` | Resume a paused job |
| GET | `/jobs/dead-letter` | List permanently failed jobs |

## How It Works

A background worker thread polls the database every 2 seconds. When it finds a job whose `run_at` is due and `status` is `SCHEDULED`, it:

1. Claims the job using an atomic `UPDATE ... WHERE status='SCHEDULED'`
2. Creates a `JobExecution` record
3. Simulates work (sleeps 1-3 seconds)
4. Randomly succeeds (70%) or fails (30%)
5. On failure: retries if `retry_count < max_retries`, otherwise marks `FAILED`
6. On success: marks `COMPLETED` (one-time) or reschedules (interval)

## Concurrency

Same job never runs twice because the worker uses:

```sql
UPDATE job SET status = 'RUNNING' WHERE id = ? AND status = 'SCHEDULED'
```

Only one worker can match this — the rest see `rowcount == 0` and skip.

## Crash Recovery

On startup, the worker finds any jobs stuck in `RUNNING` (from a previous crash) and resets them to `SCHEDULED`.

## Edge Cases

- **Past run_at** — rejected at creation with 400
- **Server crash mid-execution** — startup recovery resets RUNNING → SCHEDULED
- **Max retries reached** — job marked FAILED permanently (dead-letter)
- **Interval drift** — `advance_to_next_interval()` skips past missed times
