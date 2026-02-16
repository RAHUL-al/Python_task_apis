import time
import random
import threading
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify
from flasgger import Swagger
from sqlalchemy import text

from models import Session, Job, JobExecution, generate_uuid, utcnow

app = Flask(__name__)


WORKER_POLL_INTERVAL = 2
EXECUTION_SLEEP_MIN = 1
EXECUTION_SLEEP_MAX = 3
FAILURE_PROBABILITY = 0.30


def parse_iso_datetime(value):
    """Parse ISO 8601 string to timezone-aware UTC datetime."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def job_to_dict(job):
    session = Session()
    last_exec = (
        session.query(JobExecution)
        .filter_by(job_id=job.id)
        .order_by(JobExecution.attempt_number.desc())
        .first()
    )
    session.close()

    last_exec_data = None
    if last_exec:
        last_exec_data = {
            "id": last_exec.id,
            "job_id": last_exec.job_id,
            "attempt_number": last_exec.attempt_number,
            "started_at": last_exec.started_at.isoformat() if last_exec.started_at else None,
            "finished_at": last_exec.finished_at.isoformat() if last_exec.finished_at else None,
            "status": last_exec.status,
            "error_message": last_exec.error_message,
        }

    return {
        "id": job.id,
        "name": job.name,
        "payload": job.payload,
        "schedule_type": job.schedule_type,
        "run_at": job.run_at.isoformat() if job.run_at else None,
        "interval_seconds": job.interval_seconds,
        "max_retries": job.max_retries,
        "retry_count": job.retry_count,
        "status": job.status,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "next_run_at": job.next_run_at.isoformat() if job.next_run_at else None,
        "last_execution": last_exec_data,
    }


def execution_to_dict(execution):
    """Convert a JobExecution object to a dict for JSON response."""
    return {
        "id": execution.id,
        "job_id": execution.job_id,
        "attempt_number": execution.attempt_number,
        "started_at": execution.started_at.isoformat() if execution.started_at else None,
        "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
        "status": execution.status,
        "error_message": execution.error_message,
    }


def validate_job_data(data):
    errors = {}

    name = data.get("name")
    if not name or not isinstance(name, str) or len(name.strip()) == 0:
        errors["name"] = "name is required and must be a non-empty string."

    schedule_type = data.get("schedule_type")
    if schedule_type not in ("one_time", "interval"):
        errors["schedule_type"] = "schedule_type must be 'one_time' or 'interval'."

    run_at_str = data.get("run_at")
    parsed_run_at = None
    if not run_at_str:
        errors["run_at"] = "run_at is required."
    else:
        try:
            parsed_run_at = parse_iso_datetime(run_at_str)
            if parsed_run_at <= utcnow():
                errors["run_at"] = "run_at must be in the future."
        except (ValueError, TypeError):
            errors["run_at"] = "Invalid datetime format. Use ISO 8601, e.g. 2026-03-15T14:30:00Z"

    interval_seconds = data.get("interval_seconds")
    if schedule_type == "interval":
        if interval_seconds is None or not isinstance(interval_seconds, int) or interval_seconds <= 0:
            errors["interval_seconds"] = "interval_seconds is required and must be > 0 for interval jobs."

    max_retries = data.get("max_retries", 0)
    if not isinstance(max_retries, int) or max_retries < 0:
        errors["max_retries"] = "max_retries must be an integer >= 0."

    if errors:
        return errors, None

    return None, {
        "name": name.strip(),
        "payload": data.get("payload"),
        "schedule_type": schedule_type,
        "run_at": parsed_run_at,
        "interval_seconds": interval_seconds,
        "max_retries": max_retries,
    }



@app.route("/jobs", methods=["POST"])
def create_job():
    """
    Create a new scheduled job.
    ---
    tags:
      - Jobs
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - name
            - schedule_type
            - run_at
          properties:
            name:
              type: string
              example: "Send welcome email"
            payload:
              type: object
              example: {"user_id": 42, "template": "welcome"}
            schedule_type:
              type: string
              enum: ["one_time", "interval"]
              example: "one_time"
            run_at:
              type: string
              format: date-time
              example: "2026-03-15T14:30:00Z"
            interval_seconds:
              type: integer
              example: 3600
            max_retries:
              type: integer
              example: 3
    responses:
      201:
        description: Job created successfully
      400:
        description: Validation error
    """
    body = request.get_json()
    if body is None:
        return jsonify({"error": "Request body must be valid JSON."}), 400

    errors, validated = validate_job_data(body)
    if errors:
        return jsonify({"errors": errors}), 400

    session = Session()
    new_job = Job(
        name=validated["name"],
        payload=validated["payload"],
        schedule_type=validated["schedule_type"],
        run_at=validated["run_at"],
        interval_seconds=validated["interval_seconds"],
        max_retries=validated["max_retries"],
        status="SCHEDULED",
        retry_count=0,
    )
    session.add(new_job)
    session.commit()

    result = job_to_dict(new_job)
    session.close()
    return jsonify(result), 201


@app.route("/jobs", methods=["GET"])
def list_jobs():
    """
    List all jobs with optional filters.
    ---
    tags:
      - Jobs
    parameters:
      - name: status
        in: query
        type: string
        required: false
        enum: ["SCHEDULED", "RUNNING", "COMPLETED", "FAILED", "PAUSED"]
      - name: schedule_type
        in: query
        type: string
        required: false
        enum: ["one_time", "interval"]
      - name: next_before
        in: query
        type: string
        format: date-time
        required: false
        description: Only jobs with next run before this time
    responses:
      200:
        description: A list of jobs
    """
    session = Session()
    query = session.query(Job)

    status_filter = request.args.get("status")
    if status_filter:
        query = query.filter(Job.status == status_filter.upper())

    type_filter = request.args.get("schedule_type")
    if type_filter:
        query = query.filter(Job.schedule_type == type_filter.lower())

    next_before = request.args.get("next_before")
    if next_before:
        try:
            cutoff = parse_iso_datetime(next_before)
        except (ValueError, TypeError):
            session.close()
            return jsonify({"error": "Invalid next_before format. Use ISO 8601."}), 400
        query = query.filter(Job.run_at <= cutoff)

    jobs = query.order_by(Job.created_at.desc()).all()
    result = [job_to_dict(j) for j in jobs]
    session.close()
    return jsonify(result), 200


@app.route("/jobs/dead-letter", methods=["GET"])
def list_dead_letter_jobs():
    """
    List dead-letter jobs (permanently failed after all retries).
    ---
    tags:
      - Jobs
    responses:
      200:
        description: List of failed jobs
    """
    session = Session()
    failed = session.query(Job).filter(Job.status == "FAILED").order_by(Job.created_at.desc()).all()
    result = [job_to_dict(j) for j in failed]
    session.close()
    return jsonify(result), 200


@app.route("/jobs/<string:job_id>", methods=["GET"])
def get_job(job_id):
    """
    Get job details including execution history.
    ---
    tags:
      - Jobs
    parameters:
      - name: job_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Job details with execution history
      404:
        description: Job not found
    """
    session = Session()
    job = session.query(Job).get(job_id)
    if job is None:
        session.close()
        return jsonify({"error": f"Job with id '{job_id}' not found."}), 404

    result = job_to_dict(job)

    all_execs = (
        session.query(JobExecution)
        .filter_by(job_id=job.id)
        .order_by(JobExecution.attempt_number.asc())
        .all()
    )
    result["executions"] = [execution_to_dict(e) for e in all_execs]

    session.close()
    return jsonify(result), 200


@app.route("/jobs/<string:job_id>/pause", methods=["POST"])
def pause_job(job_id):
    """
    Pause a scheduled job.
    ---
    tags:
      - Jobs
    parameters:
      - name: job_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Job paused
      400:
        description: Cannot pause (wrong status)
      404:
        description: Not found
    """
    session = Session()
    job = session.query(Job).get(job_id)
    if job is None:
        session.close()
        return jsonify({"error": f"Job with id '{job_id}' not found."}), 404

    if job.status != "SCHEDULED":
        session.close()
        return jsonify({
            "error": f"Cannot pause job with status '{job.status}'. Only SCHEDULED jobs can be paused."
        }), 400

    job.status = "PAUSED"
    session.commit()
    result = job_to_dict(job)
    session.close()
    return jsonify(result), 200


@app.route("/jobs/<string:job_id>/resume", methods=["POST"])
def resume_job(job_id):
    """
    Resume a paused job.
    ---
    tags:
      - Jobs
    parameters:
      - name: job_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Job resumed
      400:
        description: Not paused
      404:
        description: Not found
    """
    session = Session()
    job = session.query(Job).get(job_id)
    if job is None:
        session.close()
        return jsonify({"error": f"Job with id '{job_id}' not found."}), 404

    if job.status != "PAUSED":
        session.close()
        return jsonify({
            "error": f"Cannot resume job with status '{job.status}'. Only PAUSED jobs can be resumed."
        }), 400

    # if the original run_at is now in the past, bump it to now
    now = utcnow()
    stored = job.run_at
    if stored.tzinfo is None:
        stored = stored.replace(tzinfo=timezone.utc)
    if stored <= now:
        job.run_at = now.replace(tzinfo=None)

    job.status = "SCHEDULED"
    session.commit()
    result = job_to_dict(job)
    session.close()
    return jsonify(result), 200


# --- Background Worker ---

def recover_stuck_jobs():
    """Reset any jobs stuck in RUNNING from a previous crash."""
    session = Session()
    stuck = session.query(Job).filter(Job.status == "RUNNING").all()
    for job in stuck:
        job.status = "SCHEDULED"
    if stuck:
        session.commit()
    session.close()


def execute_single_job(job_id):
    session = Session()

    # atomically claim the job
    result = session.execute(
        text("UPDATE job SET status = 'RUNNING' WHERE id = :job_id AND status = 'SCHEDULED'"),
        {"job_id": job_id},
    )
    session.commit()

    if result.rowcount == 0:
        session.close()
        return

    job = session.query(Job).get(job_id)
    if job is None:
        session.close()
        return

    # create execution record
    execution = JobExecution(
        job_id=job.id,
        attempt_number=job.retry_count + 1,
        started_at=utcnow(),
        status=None,
    )
    session.add(execution)
    session.commit()

    # simulate work
    time.sleep(random.uniform(EXECUTION_SLEEP_MIN, EXECUTION_SLEEP_MAX))

    # 30% chance of failure
    did_fail = random.random() < FAILURE_PROBABILITY
    finished_at = utcnow()

    if did_fail:
        execution.status = "FAILED"
        execution.error_message = f"Simulated failure (probability={FAILURE_PROBABILITY})"
        execution.finished_at = finished_at

        if job.retry_count < job.max_retries:
            job.retry_count += 1
            job.status = "SCHEDULED"
        else:
            job.status = "FAILED"
    else:
        execution.status = "SUCCESS"
        execution.finished_at = finished_at

        if job.schedule_type == "interval":
            job.retry_count = 0
            job.advance_to_next_interval()
            job.status = "SCHEDULED"
        else:
            job.status = "COMPLETED"

    session.commit()
    session.close()


def poll_for_due_jobs():
    """Find all due jobs and execute them."""
    session = Session()
    now = utcnow()
    due_jobs = (
        session.query(Job)
        .filter(Job.status == "SCHEDULED")
        .filter(Job.run_at <= now.replace(tzinfo=None))
        .all()
    )
    job_ids = [job.id for job in due_jobs]
    session.close()

    for job_id in job_ids:
        execute_single_job(job_id)


def start_worker_loop():
    """Main worker loop — recovers stuck jobs then polls forever."""
    recover_stuck_jobs()
    while True:
        try:
            poll_for_due_jobs()
        except Exception:
            pass
        time.sleep(WORKER_POLL_INTERVAL)


def start_worker_in_background():
    """Launch the worker in a daemon thread."""
    t = threading.Thread(target=start_worker_loop, daemon=True)
    t.start()

Swagger(app, template={
    "info": {
        "title": "Job Scheduler & Execution Engine API",
        "description": "Schedule jobs with retries, concurrency safety, and interval support.",
        "version": "1.0.0",
    }
})


if __name__ == "__main__":
    start_worker_in_background()

    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
