#  QueueCTL — A Lightweight Python Job Queue System

QueueCTL is a **CLI-based job queue manager** written in Python.  
It provides a simple yet robust way to **enqueue shell commands**, **run them in the background**, and **handle retries and failures** — all using a lightweight **SQLite database** as a backend.




##  Features

✅ **Persistent Queue** — backed by SQLite (`queuectl.db`)  
✅ **Multiple Workers** — run concurrent jobs in parallel  
✅ **Job Retries** — with exponential backoff strategy  
✅ **Dead Letter Queue (DLQ)** — stores permanently failed jobs  
✅ **Simple CLI Interface** 



##  Project Structure
QueueCTL/<br>
│<br>
├── queuectl.py <br>
├── requirements.txt <br>
├── README.md<br>
└── queuectl.db <br>



##  Installation

Clone this repository and install dependencies.

```bash
git clone https://github.com/anuvindmp/QueueCTL.git
cd QueueCTL
pip install -r requirements.txt
```



##  Example Usage — Test All Functionalities

Follow these steps to test **QueueCTL** end-to-end:

```bash
# 1. Start fresh — delete old database
del queuectl.db

# 2. Initialize a new database
python queuectl.py init

# 3.  Enqueue a successful job
python queuectl.py enqueue --command "echo hello from job-ok"

# 4.  Enqueue a failing job (with 2 retries)
python queuectl.py enqueue --command "not_a_real_command" --max-retries 2

# 5.  List all jobs currently in the queue
python queuectl.py list

# 6.  Start two workers to process jobs
python queuectl.py worker start --count 2

# 7.  View dead (failed) jobs in the Dead Letter Queue
python queuectl.py dlq list

# 8 Retry DLQ
python queuectl.py dlq retry <job_id>


```

## Working video :
https://drive.google.com/file/d/12jFIISSRHttM57Qh9x_WWTz3QGpGUGSc/view?usp=sharing
