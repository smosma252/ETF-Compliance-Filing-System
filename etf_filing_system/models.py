from sqlmodel import Field, SQLModel, create_engine
from datetime import datetime, date


class Funds(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    cik: str
    series_id: str
    class_id: str
    ticker: str
    fund_name: str
    fund_type: str
    status: str
    created_at: datetime
    updated_at: datetime

class Filings(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    fund_id: int | None
    filing_type: str
    reporting_period_end: datetime
    filing_date: date
    amendment_no:int 
    status: str
    source_format: str
    source_reference: str
    created_by: str
    approved_by: str
    approved_at: datetime
    created_at: datetime 
    updated_at: datetime


postgres_db_name = "etf_filing"
postgres_url = "postgresql+psycopg2://postgres:osama@127.0.0.1:5432/etf_filing"

connection_args = {}
engine = create_engine(postgres_url, echo=True)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

