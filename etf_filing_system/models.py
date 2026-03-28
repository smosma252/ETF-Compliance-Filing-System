from sqlmodel import Field, SQLModel, create_engine
from datetime import datetime, date


class Funds(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    cik: str | None = None
    series_id: str
    series_lei: str | None = None
    fund_name: str
    ticker: str | None = None  # optional
    created_at: datetime = Field(default_factory=datetime)
    updated_at: datetime = Field(default_factory=datetime)

class Filings(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    accession_number: str
    fund_id: int = Field(foreign_key="funds.id")
    total_assets: float | None = None
    total_liabilities: float | None = None
    net_assets: float | None = None
    status: str = "draft"
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Holdings(SQLModel, table=True):
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


postgres_db_name = "etf_filing"
postgres_url = "${POSTGRES_URL}"

connection_args = {}
engine = create_engine(postgres_url, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

