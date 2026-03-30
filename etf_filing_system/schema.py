from pydantic import BaseModel

class ETFFundFormData(BaseModel):
    series_id: str
    series_lei: str | None = None
    fund_name: str
    ticker: str | None = None
