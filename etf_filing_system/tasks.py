import os
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from celery import Celery
from sqlmodel import Session
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

try:
    from .data_imports.n_port_importer import NPortImporter
    from .models import ImportJob, get_engine
    from .status import ImportJobStatus
except ImportError:
    # Allow running as a top-level module (e.g. from etf_filing_system/ directory).
    import sys

    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from etf_filing_system.data_imports.n_port_importer import NPortImporter
    from etf_filing_system.models import ImportJob, get_engine
    from etf_filing_system.status import ImportJobStatus

broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
result_backend = os.getenv("CELERY_RESULT_BACKEND", broker_url)
celery_app = Celery("tasks", broker=broker_url, backend=result_backend)
celery_app.conf.update(
    # Windows: avoid billiard prefork PermissionError (WinError 5).
    worker_pool=os.getenv("CELERY_WORKER_POOL", "solo"),
    worker_concurrency=int(os.getenv("CELERY_WORKER_CONCURRENCY", "1")),
)


@celery_app.task(name="etf_filing_system.process_nport_files")
def process_nport_files(job_id:int):
    filepath = None
    with Session(get_engine()) as session:
        job_obj = session.get(ImportJob, job_id)
        job_obj.status = ImportJobStatus.RUNNING.value
        job_obj.started_at = datetime.now(timezone.utc)
        filepath = job_obj.stored_file
        session.add(job_obj)
        session.commit()
    
    try:
        if not filepath:
            raise FileExistsError("Unable to find filepth")
        importer = NPortImporter()
        asyncio.run(importer.parsefile(filepath))
        importer.import_to_db()

        with Session(get_engine()) as session:
            job_obj = session.get(ImportJob, job_id)
            job_obj.status = ImportJobStatus.COMPLETE.value
            job_obj.finished_at = datetime.now(timezone.utc)
            session.add(job_obj)
            session.commit()
    
    except Exception as err:
        with Session(get_engine()) as session:
            job_obj = session.get(ImportJob, job_id)
            job_obj.error_message = str(err)
            job_obj.status = ImportJobStatus.FAILED.value
            job_obj.finished_at = datetime.now(timezone.utc)
            session.add(job_obj)
            session.commit()
        raise
