from sqlmodel import Field, SQLModel, create_engine, Session
from datetime import datetime, timezone
import os
from sqlalchemy import UniqueConstraint, Index
from fastapi import UploadFile

_engine = None

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class Funds(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    cik: str | None = None
    series_id: str | None = None
    series_lei: str | None = None
    fund_name: str
    ticker: str | None = None  # optional
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

class Filings(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    __table_args__ = (
        UniqueConstraint(
            "accession_number", 
            "fund_id",
            name="uq_filings_accession_fundid"
        ),
        Index("accession_number"),
    )
    accession_number: str
    fund_id: int = Field(foreign_key="funds.id")
    total_assets: float | None = None
    total_liabilities: float | None = None 
    net_assets: float | None = None
    status: str = "draft"
    created_at: datetime = Field(default_factory=utc_now)

class Holdings(SQLModel, table=True):
    __table_args__ = (
        UniqueConstraint(
            "filing_id",
            "issuer_name",
            "cusip",
            name="uq_holdings_filing_issuer_cusip",
        ),
        Index("ix_holdings_filing_id", "filing_id"),
    )

    id: int | None = Field(default=None, primary_key=True)
    filing_id: int = Field(foreign_key="filings.id")
    issuer_name: str
    cusip: str | None = None
    lei: str | None = None
    market_value: float
    weight_pct: float | None = None
    asset_category: str | None = None
    issuer_type: str | None = None
    country: str | None = None


class ExceptionImports(SQLModel, table=True):
    id : int | None = Field(default=None, primary_key=True)
    filing_id: int | None
    source_file: str | None
    rule_code: str = Field(index=True) # MISSING ACCESSION NUMBER, MISSING FUND NAME, etc. 
    message : str | None
    raw_payload: str | None = None
    status: str = Field(index=True, default="OPEN") # CLOSED, DISMISSED
    resolved_by: str | None
    resolved_at: datetime | None


class ImportJob(SQLModel, table=True):
    id: int | None = Field(default=True, primary_key=True)
    job_type: str | None
    status: str = Field(default="QUEUED", index=True) # QUEUED, RUNNING, COMPLETED, FAILED
    source_filename : str | None
    stored_file: str # url

    total_rows: int = 0
    processed_rows: int = 0
    exception_rows: int = 0

    error_message: str | None = None

    created_at: datetime = Field(default=datetime.now())
    started_at: datetime | None = None
    finished_at: datetime | None = None


def create_db_and_tables():
    SQLModel.metadata.create_all(get_engine())

def get_engine():
    global _engine
    if _engine is None:
        postgres_url = os.getenv("POSTGRES_URL")
        if not postgres_url:
            raise ValueError("POSTGRES_URL is not set.")
        _engine = create_engine(postgres_url, echo=True)
    return _engine

def get_session():
    with Session(get_engine()) as session:
        yield session

def save_file_to_store(file: UploadFile):
    storage_path = os.getenv("FILE_STORE_FILEPATH")
    os.path.join(storage_path, file.filename, datetime.now().strftime("%d%m%Y"))