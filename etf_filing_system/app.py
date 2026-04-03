from fastapi import FastAPI, HTTPException, Depends
from fastapi import UploadFile
from contextlib import asynccontextmanager
from sqlmodel import Session
from .models import ImportJob, save_file_to_store, get_session
from .schema import ETFFundFormData
from .data_imports.import_factory import ImportFactory
from .status import ImportJobStatus
from .task import process_nport_files


@asynccontextmanager
async def on_startup(app: FastAPI, debug=True):
    print("Startup Actions")
    yield
    print("Shutdown")

app = FastAPI(lifespan=on_startup)

@app.get("/")
async def root():
    pass

@app.post("/fundtype")
async def create_fund_type(fund_data: ETFFundFormData):
    try:
        pass
    except Exception:
        print("")
    pass

@app.post('/upload/n-port')
async def create_import(file: UploadFile, session: Session=Depends(get_session)):
    try:
        stored_path = save_file_to_store(file)
        job = ImportJob(
            source_filename=file.filename,
            stored_file=stored_path, 
            status=ImportJobStatus.QUEUED.value
        )
        session.add(job)
        session.commit()
        session.refresh(job)

        process_nport_files.delay(job.id)

    except Exception as er:
        return HTTPException(status_code=400, detail="Import Job failed to import N-Port File.")

    return {"job_id": job.id, "status": job.status}


@app.post("/upload")
async def upload_file(file: UploadFile, fund_type:str):
    try:
        importer = ImportFactory.get_importer(fund_type=fund_type)
        await importer.parsefile(file)
        importer.import_to_db()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to parse/import file: {str(e)}")
    return {"status": "ok"}
 
@app.post('/approve')
async def approve(fund_id:int):
    pass

@app.get("/auditlogs")
async def get_audit_logs():
    pass

@app.put("/records/{recordId}")
async def update_records(record_Id):
    pass
