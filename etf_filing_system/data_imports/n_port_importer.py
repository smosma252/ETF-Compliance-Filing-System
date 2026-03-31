from .importer import DataImporter
import pandas as pd
import inspect
from fastapi import UploadFile
from sqlmodel import Session, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import ProgrammingError
from ..models import get_engine, Holdings, Filings

class NPortImporter(DataImporter):
    CHUNK_SIZE = 50_000

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
        "accession_number",
        "filing_id",
        "issuer_name",
        "cusip",
        "lei",
        "market_value",
        "weight_pct",
        "asset_category",
        "issuer_type",
        "country",
    ]

    def __init__(self):
        self.df = None
        self.file = None

    async def parsefile(self, file: UploadFile):
        # Large files are streamed in chunks during import_to_db.
        self.file = file
        seek_result = file.seek(0)
        if inspect.isawaitable(seek_result):
            await seek_result
        return None

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
        standardized = standardized[self.HOLDINGS_COLUMNS].copy()
        return standardized

    def import_to_db(self):
        if self.file is None:
            raise ValueError("No file loaded. Run parsefile first.")

        with Session(get_engine()) as session:
            filing_mapping = {
                self._clean_string(filing.accession_number): filing.id
                for filing in session.exec(select(Filings)).all()
                if self._clean_string(filing.accession_number)
            }

            if not filing_mapping:
                raise ValueError("No filings found. Import fund_info before N-PORT holdings.")

            if hasattr(self.file, "file"):
                self.file.file.seek(0)
                file_obj = self.file.file
            else:
                file_obj = self.file

            for chunk in pd.read_csv(file_obj, delimiter="\t", chunksize=self.CHUNK_SIZE):
                df = self.normalize(chunk)

                
                df["filing_id"] = df["accession_number"].map(filing_mapping)
                df["accession_number"] = df["accession_number"].apply(self._clean_string)
                
                df = df[df["filing_id"].notna()].copy()
                df["filing_id"] = df["filing_id"].astype(int)
                df["issuer_name"] = df["issuer_name"].apply(self._clean_string)
                df["cusip"] = df["cusip"].apply(self._clean_string)
                df["lei"] = df["lei"].apply(self._clean_string)
                df["asset_category"] = df["asset_category"].apply(self._clean_string)
                df["issuer_type"] = df["issuer_type"].apply(self._clean_string)
                df["country"] = df["country"].apply(self._clean_string)
                df["weight_pct"] = df["weight_pct"].apply(self._clean_float)

                df = df[df["issuer_name"].notna() & df["market_value"].notna()].copy()

                df = df.drop_duplicates(
                    subset=["filing_id", "issuer_name", "cusip"],
                    keep="last",
                )
                if df.empty:
                    continue

                payload = df[
                    [
                        "filing_id",
                        "issuer_name",
                        "cusip",
                        "lei",
                        "market_value",
                        "weight_pct",
                        "asset_category",
                        "issuer_type",
                        "country",
                    ]
                ].to_dict(orient="records")

                stmt = pg_insert(Holdings).values(payload)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["filing_id", "issuer_name", "cusip"],
                    set_={
                        "lei": stmt.excluded.lei,
                        "market_value": stmt.excluded.market_value,
                        "weight_pct": stmt.excluded.weight_pct,
                        "asset_category": stmt.excluded.asset_category,
                        "issuer_type": stmt.excluded.issuer_type,
                        "country": stmt.excluded.country,
                    },
                )

                try:
                    session.execute(stmt)
                except ProgrammingError as exc:
                    message = str(exc).lower()
                    if "no unique or exclusion constraint" in message:
                        raise ValueError(
                            "Missing unique constraint on holdings(filing_id, issuer_name, cusip). "
                            "Run migrations before importing."
                        ) from exc
                    raise

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
