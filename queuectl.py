import os
import sys
import json
import sqlite3
import subprocess
import time
import signal
import uuid
from datetime import datetime, timezone, timedelta
from multiprocessing import Process
from pathlib import Path
import threading

import click    

DB_FILE = "queuectl.db"
DB_PATH = Path(DB_FILE)
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2
WORKERS_TRACK_FILE = Path("workers.json")
POLL_INTERVAL = 1.0  

# ----- Utility helpers -----------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_iso(ts: str):
    return datetime.fromisoformat(ts)

def ensure_dir_for(path: Path):
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

# ----- Database layer -----------------------------------------------------

def get_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=30, isolation_level=None) 
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the database and create all required tables and fields."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Create jobs table with all fields used by the code
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            worker_id TEXT,
            acquired_at TEXT,
            next_retry_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            last_error TEXT
        )
    """)

    # Create config table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Insert default config values if missing
    cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("max_retries", str(DEFAULT_MAX_RETRIES)))
    cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("backoff_base", str(DEFAULT_BACKOFF_BASE)))

    conn.commit()
    conn.close()
    print("Database initialized.")



def db_get_config():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM config")
    rows = cur.fetchall()
    cfg = {r["key"]: r["value"] for r in rows}
    conn.close()
    # convert some values to ints
    if "backoff_base" in cfg:
        try:
            cfg["backoff_base"] = int(cfg["backoff_base"])
        except:
            cfg["backoff_base"] = DEFAULT_BACKOFF_BASE
    if "default_max_retries" in cfg:
        try:
            cfg["default_max_retries"] = int(cfg["default_max_retries"])
        except:
            cfg["default_max_retries"] = DEFAULT_MAX_RETRIES
    return cfg

def db_set_config(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, str(value)))
    conn.commit()
    conn.close()

def enqueue_job(job_dict):
    """
    Insert job into DB. job_dict must contain id and command; optional max_retries.
    """
    conn = get_conn()
    cur = conn.cursor()
    job_id = job_dict.get("id") or str(uuid.uuid4())
    command = job_dict.get("command")
    if not command:
        raise ValueError("job must include 'command' field")
    max_retries = int(job_dict.get("max_retries") or db_get_config().get("default_max_retries", DEFAULT_MAX_RETRIES))
    now = now_iso()
    cur.execute("""
    INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
    VALUES (?, ?, 'pending', 0, ?, ?, ?)
    """, (job_id, command, max_retries, now, now))
    conn.commit()
    conn.close()
    return job_id

def list_jobs(state=None, limit=100):
    conn = get_conn()
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at LIMIT ?", (state, limit))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def status_summary():
    conn = get_conn()
    cur = conn.cursor()
    counts = {}
    for s in ("pending", "processing", "completed", "failed", "dead"):
        cur.execute("SELECT COUNT(*) as cnt FROM jobs WHERE state = ?", (s,))
        counts[s] = cur.fetchone()["cnt"]
    workers = []
    if WORKERS_TRACK_FILE.exists():
        try:
            with open(WORKERS_TRACK_FILE, "r") as f:
                workers = json.load(f)
        except:
            workers = []
    conn.close()
    return counts, workers

def claim_next_job(worker_id):
    """
    Attempt to claim a job. This is done in a small transaction:
     - find a pending job with next_retry_at <= now (or null)
     - attempt to atomically set state='processing' for that job
    If the UPDATE affects 1 row, claim succeeded and we return full job row.
    """
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    cur.execute("BEGIN IMMEDIATE")
    try:
        cur.execute("""
        SELECT id FROM jobs
        WHERE state='pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
        ORDER BY created_at
        LIMIT 1
        """, (now,))
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        job_id = row["id"]
        cur.execute("""
        UPDATE jobs
        SET state='processing', worker_id=?, acquired_at=?, updated_at=?
        WHERE id=? AND state='pending'
        """, (worker_id, now, now, job_id))
        if cur.rowcount != 1:
            conn.commit()
            return None
        cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        job = dict(cur.fetchone())
        conn.commit()
        return job
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def mark_job_completed(job_id, stdout_text=None, stderr_text=None):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    cur.execute("""
    UPDATE jobs
    SET state='completed', updated_at=?, last_error=?
    WHERE id=?
    """, (now, (stderr_text or "")[:1000], job_id))
    conn.commit()
    conn.close()

def record_failed_attempt(job_id, error_message, attempts, max_retries, backoff_base):
    """
    Update job after a failed run. If attempts > max_retries => mark dead.
    Else set next_retry_at to now + backoff_base ** attempts seconds and state back to pending.
    """
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    if attempts > max_retries:
        cur.execute("""
        UPDATE jobs
        SET state = 'dead', attempts = ?, updated_at = ?, last_error = ?
        WHERE id = ?
        """, (attempts, now.isoformat(), error_message[:1000], job_id))
    else:
        delay_seconds = backoff_base ** attempts
        next_retry = (now + timedelta(seconds=delay_seconds)).isoformat()
        cur.execute("""
        UPDATE jobs
        SET state = 'pending', attempts = ?, updated_at = ?, last_error = ?, next_retry_at = ?
        WHERE id = ?
        """, (attempts, now.isoformat(), error_message[:1000], next_retry, job_id))
    conn.commit()
    conn.close()

def reset_job_from_dlq(job_id):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    cur.execute("""
    UPDATE jobs
    SET state='pending', attempts=0, updated_at=?, last_error=NULL, next_retry_at=NULL
    WHERE id=? AND state='dead'
    """, (now, job_id))
    changed = cur.rowcount
    conn.commit()
    conn.close()
    return changed == 1

# ----- Worker logic ------------------------------------------------------

_worker_should_stop = False

def _worker_signal_handler(signum, frame):
    global _worker_should_stop
    _worker_should_stop = True

def worker_process_loop(worker_name, poll_interval=POLL_INTERVAL, job_timeout=None):
    """
    Loop executed inside each worker process.
    """

    signal.signal(signal.SIGINT, _worker_signal_handler)
    signal.signal(signal.SIGTERM, _worker_signal_handler)

    cfg = db_get_config()
    backoff_base = cfg.get("backoff_base", DEFAULT_BACKOFF_BASE)

    while not _worker_should_stop:
        try:
            job = claim_next_job(worker_name)
            if job is None:
                time.sleep(poll_interval)
                continue
            job_id = job["id"]
            command = job["command"]
            attempts = int(job["attempts"]) + 1
            max_retries = int(job["max_retries"])
            click.echo(f"[{worker_name}] Claimed job {job_id}: {command} (attempt {attempts}/{max_retries})")
            try:
                completed = subprocess.run(command, shell=True, capture_output=True, timeout=job_timeout)
                stdout_text = (completed.stdout.decode(errors='ignore') if completed.stdout else "")
                stderr_text = (completed.stderr.decode(errors='ignore') if completed.stderr else "")
                if completed.returncode == 0:
                    mark_job_completed(job_id, stdout_text=stdout_text, stderr_text=stderr_text)
                    click.echo(f"[{worker_name}] Job {job_id} succeeded.")
                else:
                    record_failed_attempt(job_id, f"exit:{completed.returncode} {stderr_text}", attempts, max_retries, backoff_base)
                    click.echo(f"[{worker_name}] Job {job_id} failed with code {completed.returncode}.")
            except subprocess.TimeoutExpired as e:
                record_failed_attempt(job_id, f"timeout: {e}", attempts, max_retries, backoff_base)
                click.echo(f"[{worker_name}] Job {job_id} timed out.")
            except Exception as e:
                record_failed_attempt(job_id, f"exception: {e}", attempts, max_retries, backoff_base)
                click.echo(f"[{worker_name}] Job {job_id} crashed: {e}")
        except Exception as exc:
            click.echo(f"[{worker_name}] Worker loop error: {exc}", err=True)
            time.sleep(1.0)

    click.echo(f"[{worker_name}] Received stop signal, exiting.")

# ----- Worker manager (start/stop) ---------------------------------------

def spawn_workers(count, base_name="worker"):
    procs = []
    for i in range(count):
        name = f"{base_name}-{int(time.time())}-{i}"
        p = Process(target=worker_process_loop, args=(name,), daemon=False)
        p.start()
        procs.append({"pid": p.pid, "name": name})
    try:
        with open(WORKERS_TRACK_FILE, "w") as f:
            json.dump(procs, f)
    except Exception as e:
        click.echo(f"Warning: could not write workers file: {e}", err=True)
    click.echo(f"Started {len(procs)} workers. PIDs: {[p['pid'] for p in procs]}")
    return procs

def stop_workers(grace_seconds=10):
    if not WORKERS_TRACK_FILE.exists():
        click.echo("No workers pid file found.")
        return
    try:
        with open(WORKERS_TRACK_FILE, "r") as f:
            procs = json.load(f)
    except Exception as e:
        click.echo(f"Could not read workers file: {e}", err=True)
        return
    for rec in procs:
        pid = rec.get("pid")
        try:
            os.kill(pid, signal.SIGINT)
            click.echo(f"Sent SIGINT to pid {pid}")
        except ProcessLookupError:
            click.echo(f"Process {pid} not found")
        except Exception as e:
            click.echo(f"Failed to signal pid {pid}: {e}", err=True)
    t0 = time.time()
    while time.time() - t0 < grace_seconds:
        running = []
        for rec in procs:
            pid = rec.get("pid")
            try:
                os.kill(pid, 0)
                running.append(pid)
            except ProcessLookupError:
                pass
            except Exception:
                running.append(pid)
        if not running:
            break
        time.sleep(0.5)
    try:
        WORKERS_TRACK_FILE.unlink()
    except Exception:
        pass
    click.echo("Stop command complete.")

# ----- CLI using Click ----------------------------------------------------

@click.group()
def cli():
    init_db()

@cli.command(help="Enqueue a new job. Provide a JSON string or use --id and --command.")
@click.argument("job_json", required=False)
@click.option("--id", "job_id", required=False, help="Job ID (optional, will be generated if missing).")
@click.option("--command", required=False, help="Command to run (e.g. \"sleep 2\").")
@click.option("--max-retries", type=int, required=False, help="Per-job max retries.")
def enqueue(job_json, job_id, command, max_retries):
    if job_json:
        try:
            job = json.loads(job_json)
        except Exception as e:
            click.echo(f"Invalid JSON: {e}", err=True)
            sys.exit(2)
    else:
        if not command:
            click.echo("Either provide a JSON job or --command", err=True)
            sys.exit(2)
        job = {"id": job_id, "command": command}
        if max_retries is not None:
            job["max_retries"] = max_retries
    try:
        jid = enqueue_job(job)
        click.echo(f"Enqueued job {jid}")
    except Exception as e:
        click.echo(f"Failed to enqueue: {e}", err=True)
        sys.exit(1)

@cli.group(help="Worker operations")
def worker():
    pass

@worker.command("start", help="Start worker processes. They run in background as separate processes.")
@click.option("--count", type=int, default=1, help="Number of worker processes to start.")
def worker_start(count):
    if count < 1:
        click.echo("Count must be >= 1")
        sys.exit(2)
    spawn_workers(count)

@worker.command("stop", help="Stop running workers (graceful).")
@click.option("--grace", type=int, default=10, help="Seconds to wait for graceful stop.")
def worker_stop(grace):
    stop_workers(grace_seconds=grace)

@cli.command(help="Show summary counts and active workers")
def status():
    counts, workers = status_summary()
    click.echo("Job counts:")
    for k, v in counts.items():
        click.echo(f"  {k}: {v}")
    click.echo("Tracked workers (from workers.json):")
    if not workers:
        click.echo("  none")
    else:
        for w in workers:
            click.echo(f"  pid={w.get('pid')} name={w.get('name')}")

@cli.command(help="List jobs; optionally filter by --state")
@click.option("--state", type=click.Choice(["pending", "processing", "completed", "failed", "dead"]), required=False)
@click.option("--limit", type=int, default=100)
def list(state, limit):
    rows = list_jobs(state=state, limit=limit)
    if not rows:
        click.echo("No jobs.")
        return
    for r in rows:
        click.echo(f"- id: {r['id']}")
        click.echo(f"    command: {r['command']}")
        click.echo(f"    state: {r['state']} attempts: {r['attempts']} max_retries: {r['max_retries']}")
        click.echo(f"    created: {r['created_at']} updated: {r['updated_at']}")
        if r.get("next_retry_at"):
            click.echo(f"    next_retry_at: {r['next_retry_at']}")
        if r.get("last_error"):
            click.echo(f"    last_error: {r['last_error'][:200]}")

@cli.group(help="Dead Letter Queue operations")
def dlq():
    pass

@dlq.command("list", help="List jobs in DLQ (state=dead)")
def dlq_list():
    rows = list_jobs(state="dead", limit=200)
    if not rows:
        click.echo("DLQ is empty.")
        return
    for r in rows:
        click.echo(f"- id: {r['id']} command: {r['command']} attempts: {r['attempts']} last_error: {r['last_error'][:200]}")

@dlq.command("retry", help="Retry a job from DLQ by id (resets attempts).")
@click.argument("job_id")
def dlq_retry(job_id):
    success = reset_job_from_dlq(job_id)
    if success:
        click.echo(f"Job {job_id} reset to pending.")
    else:
        click.echo(f"Job {job_id} not found in DLQ (or not dead).", err=True)
        sys.exit(1)

@cli.group(help="Configuration settings")
def config():
    pass

@config.command("set", help="Set a configuration key")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    db_set_config(key, value)
    click.echo(f"Config {key} set to {value}")

@config.command("get", help="Show configuration")
def config_get():
    cfg = db_get_config()
    for k, v in cfg.items():
        click.echo(f"{k} = {v}")

@cli.command(help="Initialize database (runs automatically at startup)")
def init():
    init_db()
    click.echo(f"Database initialized at {DB_PATH}")


if __name__ == "__main__":
    cli()
