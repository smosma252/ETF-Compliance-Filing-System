from .importer import DataImporter
import pandas as pd
import inspect
from fastapi import UploadFile
from sqlmodel import Session, select
from ..models import get_engine, Holdings, Filings

class NPortImporter(DataImporter):
    COLUMN_MAP = {
        "ACCESSION_NUMBER": "accession_number",
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
        *COLUMN_MAP.values(),
        "filing_id"
    ]

    def __init__(self):
        self.df = None

    async def parsefile(self, file: UploadFile):
        seek_result = file.seek(0)
        if inspect.isawaitable(seek_result):
            await seek_result

        file_obj = file.file if hasattr(file, "file") else file
        df = pd.read_csv(file_obj, delimiter="\t")
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
        if self.df is None:
            raise ValueError("No N-PORT data loaded. Run parsefile first.")

        with Session(get_engine()) as session:
            for record in self.df.to_dict(orient="records"):
                accession_number = self._clean_string(record.get("accession_number"))
                if not accession_number:
                    continue

                filing = session.exec(
                    select(Filings).where(Filings.accession_number == accession_number)
                ).first()

                if filing is None:
                    continue

                issuer_name = self._clean_string(record.get("issuer_name"))
                market_value = record.get("market_value")
                if not issuer_name or pd.isna(market_value):
                    continue
                weight_pct = self._clean_float(record.get("weight_pct"))

                holdings = session.exec(
                    select(Holdings).where(
                        Holdings.filing_id == filing.id,
                        Holdings.issuer_name == issuer_name,
                        Holdings.cusip == self._clean_string(record.get("cusip")),
                    )
                ).first()

                if holdings is None:
                    holdings = Holdings(
                        filing_id=filing.id,
                        issuer_name=issuer_name,
                        cusip=self._clean_string(record.get("cusip")),
                        lei=self._clean_string(record.get("lei")),
                        market_value=float(market_value),
                        weight_pct=weight_pct,
                        asset_category=self._clean_string(record.get("asset_category")),
                        issuer_type=self._clean_string(record.get("issuer_type")),
                        country=self._clean_string(record.get("country")),
                    )
                else:
                    holdings.lei = self._clean_string(record.get("lei"))
                    holdings.market_value = float(market_value)
                    holdings.weight_pct = weight_pct
                    holdings.asset_category = self._clean_string(record.get("asset_category"))
                    holdings.issuer_type = self._clean_string(record.get("issuer_type"))
                    holdings.country = self._clean_string(record.get("country"))

                session.add(holdings)
            session.commit()

    @staticmethod
    def _clean_string(value):
        if pd.isna(value):
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @staticmethod
    def _clean_float(value):
        if pd.isna(value):
            return None
        return float(value)
