from fastapi import FastAPI
from fastapi import UploadFile
from contextlib import asynccontextmanager

try:
    from .data_imports.n_port_importer import NPortImporter
except ImportError:
    from data_imports.n_port_importer import NPortImporter

@asynccontextmanager
async def on_startup(app: FastAPI):
    print("Startup Actions")
    yield
    print("Shutdown")

app = FastAPI(lifespan=on_startup)

@app.get("/")
async def root():
    pass

@app.post("/upload")
async def upload_file(file: UploadFile):
    try:
        importer = NPortImporter()
        importer.parsefile(file)
    except Exception:
        print("File unable to import")
    return

@app.post('/approve')
async def approve(fund_id:int):
    pass

@app.get("/auditlogs")
async def get_audit_logs():
    pass

@app.put("/records/{recordId}")
async def update_records(record_Id):
    pass
