from .importer import DataImporter
import pandas as pd
from fastapi import UploadFile
from sqlmodel import Session, select
from ..models import get_engine, Holdings, Filings

class NPortImporter(DataImporter):
    COLUMN_MAP = {
        "ISSUER_NAME": "issuer_name",
        "ISSUER_CUSIP": "cusip",
        "ISSUER_LEI": "lei",
        "CURRENCY_VALUE": "market_value",
        "PERCENTAGE": "weight_pct",
        "ASSET_CAT": "asset_category",
        "ISSUER_TYPE": "issuer_type",
        "INVESTMENT_COUNTRY": "country",
    }

    HOLDINGS_COLUMNS = [
        "filing_id", *COLUMN_MAP.values()
    ]

    def __init__(self):
        self.df = None

    async def parsefile(self, file: UploadFile):
        await file.seek(0)
        df = pd.read_csv(file.file, delimiter="\t")
        self.df = self.normalize(df)
        return self.df

    def normalize(self, df: pd.DataFrame):
        missing = [column for column in self.COLUMN_MAP if column not in df.columns]
        if missing:
            raise ValueError(f"Missing required N-PORT columns: {missing}")

        standardized = df.rename(columns=self.COLUMN_MAP).copy()

        standardized["filing_id"] = None

        standardized["market_value"] = pd.to_numeric(
            standardized["market_value"].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )

        standardized["weight_pct"] = pd.to_numeric(
            standardized["weight_pct"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False),
            errors="coerce",
        )
        standardized = standardized[self.HOLDINGS_COLUMNS]
        return standardized

    def import_to_db(self):
        with Session(get_engine()) as session:
            for record in self.df.to_records('dict'):
                accession_number = record.get('ACCESSION_NUMBER', None)

                filing = session.exec(
                    select(Filings).where(Filings.accession_number == accession_number)
                ).first()

                if filing is None:
                    continue

                holdings = session.exec(
                    select(Holdings).where(Holdings.filing_id == filing.id)
                ).first()

                if holdings is None:
                    holdings = Holdings(
                        filing_id=filing.id,
                        issuer_name=record.get('issuer_name', None),
                        cusip=record.get('cusip', None),
                        lei=record.get('lei', None),
                        market_value=record.get('market_value', 0.0),
                        weight_pct=record.get('weight_pct', 0.0),
                        asset_category=record.get('asset_category', None),
                        issuer_type=record.get('issuer_type', None),
                        country=record.get('country', None)
                    )
                
                session.add(holdings)
            session.commit()
