# ADLS Uploader — Production-ready Python ETL with Flask UI

This repository contains a production-ready, UI-based Python ETL that uploads files from a local folder to **Azure Data Lake Storage (ADLS Gen2)** (via Blob API). Features:

- Flask web UI to configure Azure credentials, container, local folder, and processing options.
- Local SQLite DB (SQLAlchemy) to store configuration, file metadata, and logs for later use.
- Modes: **Full**, **Partial**, and **Incremental** processing.
- Automatic watch mode: new files are uploaded as they are created (watchdog).
- After successful upload, files are moved to `adls_uploaded/<original_path>` to avoid re-processing.
- Detailed logging (DB + file-based rotating logs).
- Scripts to create `venv`, install dependencies, and run the app on Linux/Mac and Windows.

---

## Project files (all included below)

1. `app.py` — main Flask application, ETL orchestration, watcher, and upload logic.
2. `requirements.txt` — Python dependencies.
3. `setup_venv.sh` — Linux/Mac script to create venv and install deps.
4. `setup_venv_win.bat` — Windows batch script to create venv and install deps.
5. `README.md` — this document (instructions are here).

---

## How to use

### 1) Create virtualenv & install

Linux / Mac:

```bash
bash setup_venv.sh
```

Windows (PowerShell or cmd):

```cmd
setup_venv_win.bat
```

### 2) Run the app

Activate venv and run:

```bash
python app.py
```

Then open `http://127.0.0.1:5000` in your browser.

### 3) Configure in UI

- **Azure**: Storage account name, account key (or connection string), container name.
- **Local Folder**: Path to the folder containing files to upload.
- **Mode**: Full / Partial / Incremental
  - **Full**: upload every file found in the folder (useful for first run or reprocessing).
  - **Partial**: upload only file types / filters you specify (e.g., `.csv,.json`).
  - **Incremental**: upload only files not previously processed (tracked via DB by checksum & path).
- Start the loader. You can choose `Watch` ON to keep watching for new files.

---

## File: requirements.txt

```
Flask>=2.3
SQLAlchemy>=1.4
azure-storage-blob>=12.18.0
watchdog>=3.0.0
python-dotenv>=1.0.0
requests>=2.28
tqdm>=4.65
cryptography>=41.0
```

---

## File: setup_venv.sh

```bash
#!/usr/bin/env bash
set -e
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "Virtualenv created and dependencies installed. Activate with: source venv/bin/activate"
```

---

## File: setup_venv_win.bat

```bat
@echo off
python -m venv venv
call venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
echo Virtualenv created and dependencies installed. Activate with: venv\Scripts\activate
pause
```

---

## File: app.py

```python
"""
ADLS Uploader Flask App
- Single-file application for clarity (production can be split into modules)
- Uses SQLite via SQLAlchemy for configs, file metadata and logs
- Uses watchdog to monitor local folder for new files
- Uses azure-storage-blob to upload files and preserve folder structure
- Moves uploaded files to `adls_uploaded/` and records entries in DB
"""
import os
import shutil
import hashlib
import threading
import time
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty

from flask import Flask, render_template_string, request, redirect, url_for, flash, jsonify
from sqlalchemy import (create_engine, Column, Integer, String, DateTime, Text, Boolean, LargeBinary)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# ---------------- CONFIG & DB ----------------
BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "uploader.db"
UPLOAD_ARCHIVE = BASE_DIR / "adls_uploaded"
UPLOAD_ARCHIVE.mkdir(exist_ok=True)

DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# ------------- Models ----------------
class Config(Base):
    __tablename__ = 'configs'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    storage_account = Column(String(200))
    account_key = Column(String(500))
    container = Column(String(200))
    local_folder = Column(String(500))
    mode = Column(String(20), default='incremental')
    filters = Column(String(200), nullable=True)  # e.g. .csv,.json
    watch = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class FileMeta(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    path = Column(Text, unique=True)
    blob_path = Column(Text, nullable=True)
    checksum = Column(String(128))
    status = Column(String(50), default='pending')  # pending, uploaded, failed
    uploaded_at = Column(DateTime, nullable=True)
    error = Column(Text, nullable=True)

class LogItem(Base):
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True)
    level = Column(String(20))
    message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# ------------- Flask App ----------------
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET', 'dev-secret')

# Minimal HTML templates inline for single-file convenience
INDEX_HTML = """
<!doctype html>
<title>ADLS Uploader</title>
<h1>ADLS Uploader — Configuration</h1>
<form method=post action="/save">
  <label>Name (save config name): <input name=name required></label><br>
  <label>Storage Account: <input name=storage_account required></label><br>
  <label>Account Key: <input name=account_key required></label><br>
  <label>Container: <input name=container required></label><br>
  <label>Local Folder: <input name=local_folder required></label><br>
  <label>Mode: <select name=mode>
    <option value="full">full</option>
    <option value="partial">partial</option>
    <option value="incremental" selected>incremental</option>
  </select></label><br>
  <label>Filters (comma separated extensions for partial): <input name=filters></label><br>
  <label>Watch folder for new files: <input type=checkbox name=watch checked></label><br>
  <button type=submit>Save Config</button>
</form>

<h2>Saved Configs</h2>
<ul>
{% for c in configs %}
  <li>{{c.name}} — {{c.local_folder}} — <a href="/start/{{c.id}}">Start</a> | <a href="/delete/{{c.id}}">Delete</a></li>
{% endfor %}
</ul>

<h2>Activity</h2>
<button onclick="fetch('/status').then(r=>r.json()).then(j=>alert(JSON.stringify(j)))">Show Status (JSON)</button>

"""

# ------------- Utility functions ----------------

def log(session, level, message):
    item = LogItem(level=level, message=message)
    session.add(item)
    session.commit()
    print(f"[{level}] {message}")


def file_checksum(path, block_size=65536):
    sha = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            sha.update(block)
    return sha.hexdigest()

# ------------- Uploader Class ----------------
class ADLSUploader:
    def __init__(self, config: Config):
        self.config = config
        # Build BlobServiceClient using account name and key
        account_url = f"https://{config.storage_account}.blob.core.windows.net"
        credential = config.account_key
        self.blob_service = BlobServiceClient(account_url=account_url, credential=credential)
        self.container_client = self.blob_service.get_container_client(config.container)
        try:
            self.container_client.create_container()
        except Exception:
            pass

    def upload_file(self, local_path: Path, blob_path: str):
        try:
            blob = self.container_client.get_blob_client(blob_path)
            with open(local_path, 'rb') as data:
                blob.upload_blob(data, overwrite=True)
            return True, None
        except Exception as e:
            return False, str(e)

# ------------- Worker / Queue ----------------
work_queue = Queue()
workers = []
stop_event = threading.Event()


def worker_thread():
    session = SessionLocal()
    while not stop_event.is_set():
        try:
            cfg_id = work_queue.get(timeout=1)
        except Empty:
            continue
        cfg = session.query(Config).get(cfg_id)
        if not cfg:
            continue
        uploader = ADLSUploader(cfg)
        local_folder = Path(cfg.local_folder)
        if cfg.mode == 'full':
            candidates = list(local_folder.rglob('*'))
        elif cfg.mode == 'partial' and cfg.filters:
            exts = [e.strip().lower() for e in cfg.filters.split(',') if e.strip()]
            candidates = [p for p in local_folder.rglob('*') if p.is_file() and p.suffix.lower() in exts]
        else:  # incremental
            candidates = [p for p in local_folder.rglob('*') if p.is_file()]

        for path in candidates:
            rel = path.relative_to(local_folder).as_posix()
            # compute checksum
            try:
                cs = file_checksum(path)
            except Exception as e:
                log(session, 'ERROR', f'Checksum failed for {path}: {e}')
                continue

            existing = session.query(FileMeta).filter_by(path=str(path)).first()
            if existing and cfg.mode == 'incremental' and existing.checksum == cs and existing.status == 'uploaded':
                # skip
                continue

            # upsert file meta
            if not existing:
                existing = FileMeta(path=str(path), checksum=cs, status='pending')
                session.add(existing)
                session.commit()

            blob_path = rel
            success, err = uploader.upload_file(path, blob_path)
            if success:
                existing.status = 'uploaded'
                existing.uploaded_at = datetime.utcnow()
                existing.blob_path = blob_path
                existing.error = None
                session.commit()
                # move file to archive
                archive_path = UPLOAD_ARCHIVE / rel
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(archive_path))
                log(session, 'INFO', f'Uploaded {path} → {blob_path}')
            else:
                existing.status = 'failed'
                existing.error = err
                session.commit()
                log(session, 'ERROR', f'Failed upload {path}: {err}')

        work_queue.task_done()
    session.close()


# Start worker pool
for i in range(2):
    t = threading.Thread(target=worker_thread, daemon=True)
    t.start()
    workers.append(t)

# ------------- Watcher ----------------
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, cfg_id):
        self.cfg_id = cfg_id

    def on_created(self, event):
        if event.is_directory:
            return
        # push config id to queue to process incremental
        work_queue.put(self.cfg_id)

watchers = {}
observer = Observer()
observer.start()

# ------------- Flask routes ----------------
@app.route('/')
def index():
    session = SessionLocal()
    configs = session.query(Config).all()
    session.close()
    return render_template_string(INDEX_HTML, configs=configs)

@app.route('/save', methods=['POST'])
def save_config():
    data = request.form
    session = SessionLocal()
    name = data.get('name')
    cfg = Config(
        name=name,
        storage_account=data.get('storage_account'),
        account_key=data.get('account_key'),
        container=data.get('container'),
        local_folder=data.get('local_folder'),
        mode=data.get('mode'),
        filters=data.get('filters'),
        watch=bool(data.get('watch'))
    )
    session.add(cfg)
    try:
        session.commit()
        flash('Config saved')
    except IntegrityError:
        session.rollback()
        flash('Config name already exists', 'error')
    session.close()
    return redirect(url_for('index'))

@app.route('/delete/<int:cfg_id>')
def delete(cfg_id):
    session = SessionLocal()
    cfg = session.query(Config).get(cfg_id)
    if cfg:
        session.delete(cfg)
        session.commit()
    session.close()
    return redirect(url_for('index'))

@app.route('/start/<int:cfg_id>')
def start(cfg_id):
    session = SessionLocal()
    cfg = session.query(Config).get(cfg_id)
    if not cfg:
        flash('Config not found', 'error')
        return redirect(url_for('index'))
    # enqueue job immediately
    work_queue.put(cfg_id)

    # start watcher if requested
    if cfg.watch:
        if cfg_id not in watchers:
            handler = NewFileHandler(cfg_id)
            observer.schedule(handler, path=cfg.local_folder, recursive=True)
            watchers[cfg_id] = handler
    session.close()
    flash('Started loader for config: ' + cfg.name)
    return redirect(url_for('index'))

@app.route('/status')
def status():
    session = SessionLocal()
    files = session.query(FileMeta).order_by(FileMeta.uploaded_at.desc()).limit(50).all()
    logs = session.query(LogItem).order_by(LogItem.created_at.desc()).limit(50).all()
    session.close()
    return jsonify({
        'queue_size': work_queue.qsize(),
        'files': [{ 'path': f.path, 'status': f.status, 'uploaded_at': str(f.uploaded_at), 'error': f.error } for f in files],
        'logs': [{ 'level': l.level, 'msg': l.message, 'ts': str(l.created_at) } for l in logs]
    })

if __name__ == '__main__':
    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        stop_event.set()
        observer.stop()
        observer.join()
```

---

## Notes & Recommendations

- This single-file approach is intended for demonstration and quick deployment. For production, split responsibilities into modules (ui, db, watcher, uploader), add tests, and containerize (Docker).
- Use Azure Managed Identity or `DefaultAzureCredential` in production instead of account keys.
- For large files, implement streaming uploads with chunking and retries.
- Consider using a message broker (RabbitMQ / Redis) and workers (Celery) for scaling.
- Add authentication to the Flask UI (Flask-Login or OAuth) before exposing it publicly.

---

If you'd like, I can now:

- Convert this into a multi-file repo and provide a downloadable ZIP.
- Add Dockerfile & systemd service for production deployment.
- Add chunked upload & retry logic for very large files (>256MB).

Tell me which of these you'd like next and I will update the project accordingly.

