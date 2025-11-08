# ⚙️ QueueCTL — A Lightweight Python Job Queue System

QueueCTL is a **CLI-based job queue manager** written in Python.  
It provides a simple yet robust way to **enqueue shell commands**, **run them in the background**, and **handle retries and failures** — all using a lightweight **SQLite database** as a backend.


---

## 🧩 Features

✅ **Persistent Queue** — backed by SQLite (`queuectl.db`)  
✅ **Multiple Workers** — run concurrent jobs in parallel  
✅ **Job Retries** — with exponential backoff strategy  
✅ **Dead Letter Queue (DLQ)** — stores permanently failed jobs  
✅ **Simple CLI Interface** 

---

## 📁 Project Structure
QueueCTL/
│<br>
├── queuectl.py <br>
├── requirements.txt <br>
├── README.md<br>
└── queuectl.db <br>

---

## ⚙️ Installation

Clone this repository and install dependencies.

```bash
git clone https://github.com/anuvindmp/QueueCTL.git
cd QueueCTL
pip install -r requirements.txt
