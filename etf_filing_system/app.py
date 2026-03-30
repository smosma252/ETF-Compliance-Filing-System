from fastapi import FastAPI, HTTPException
from fastapi import UploadFile
from contextlib import asynccontextmanager
from .schema import ETFFundFormData
from .data_imports.import_factory import ImportFactory

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
