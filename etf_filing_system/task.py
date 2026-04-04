from celery import Celery, Task
from sqlmodel import Session, select
from datetime import datetime

from .data_imports.n_port_importer import NPortImporter
from .models import ImportJob, get_engine
from .status import ImportJobStatus

celery_app = Celery('tasks', broker='redis://localhost:6379')


@celery_app.task
def process_nport_files(job_id:int):
    filepath = None
    with Session(get_engine) as session:
        job_obj = session.get(ImportJob, job_id)
        job_obj.status = ImportJobStatus.RUNNING
        job_obj.started_at = datetime.now(datetime.timezone.utc)
        filepath = job_obj.stored_file
        session.add(job_obj)
        session.commit()
    
    try:
        if not filepath:
            raise FileExistsError("Unable to find filepth")
        importer = NPortImporter()
        importer.parsefile(filepath)
        importer.import_to_db()

        with Session(get_engine) as session:
            job_obj = session.get(ImportJob, job_id)
            job_obj.status = ImportJobStatus.COMPLETE
            job_obj.finished_at = datetime.now(datetime.timezone.utc)
            session.add(job_obj)
            session.commit()
    
    except Exception as err:
        with Session(get_engine) as session:
            job_obj = session.get(ImportJob, job_id)
            job_obj.error_message = str(err)
            job_obj.status = ImportJobStatus.FAILED
            job_obj.finished_at = datetime.now(datetime.timezone.utc)
            session.add(job_obj)
            session.commit()
        raise