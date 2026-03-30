from .importer import DataImporter
import pandas as pd
import inspect
from fastapi import UploadFile
from sqlmodel import Session, select
from ..models import Filings, Funds, get_engine

class FundInfoImporter(DataImporter):
    COLUMN_MAP = {
        "ACCESSION_NUMBER": "accession_number",
        "SERIES_ID": "series_id",
        "SERIES_LEI": "series_lei",
        "SERIES_NAME": "fund_name",
        "TOTAL_ASSETS": "total_assets",
        "TOTAL_LIABILITIES": "total_liabilities",
        "NET_ASSETS": "net_assets",
    }

    REQUIRED_COLUMNS = list(COLUMN_MAP.keys())
    FILINGS_COLUMNS = [
        "accession_number",
        "series_id",
        "series_lei",
        "fund_name",
        "total_assets",
        "total_liabilities",
        "net_assets",
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
        missing = [column for column in self.REQUIRED_COLUMNS if column not in df.columns]
        if missing:
            raise ValueError(f"Missing required fund info columns: {missing}")

        standardized = df.rename(columns=self.COLUMN_MAP).copy()

        for numeric_column in ("total_assets", "total_liabilities", "net_assets"):
            standardized[numeric_column] = pd.to_numeric(
                standardized[numeric_column].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

        standardized = standardized[self.FILINGS_COLUMNS]
        return standardized

    def import_to_db(self):
        if self.df is None:
            raise ValueError("No fund info data loaded. Run parsefile first.")

        with Session(get_engine()) as session:
            for row in self.df.to_dict(orient="records"):
                series_id = self._clean_string(row["series_id"])
                series_lei = self._clean_string(row["series_lei"])
                fund_name = self._clean_string(row["fund_name"])
                accession_number = self._clean_string(row["accession_number"])

                if not accession_number:
                    raise ValueError(
                        "Each row must include SERIES_ID, SERIES_NAME, and ACCESSION_NUMBER."
                    )
                # TODO: Fix this later on to handle better and avoid skipping. Possibly fix the models
                elif not series_id or not fund_name:
                    continue

                fund = session.exec(
                    select(Funds).where(Funds.series_id == series_id)
                ).first()

                if fund is None:
                    fund = Funds(
                        series_id=series_id,
                        series_lei=series_lei,
                        fund_name=fund_name,
                    )
                    session.add(fund)
                    session.flush()
                else:
                    # Keep existing funds current if incoming fund metadata changes.
                    if series_lei and fund.series_lei != series_lei:
                        fund.series_lei = series_lei
                    if fund_name and fund.fund_name != fund_name:
                        fund.fund_name = fund_name

                filing = session.exec(
                    select(Filings).where(
                        Filings.accession_number == accession_number,
                        Filings.fund_id == fund.id,
                    )
                ).first()

                if filing is None:
                    filing = Filings(
                        accession_number=accession_number,
                        fund_id=fund.id,
                    )
                    session.add(filing)

                filing.total_assets = row["total_assets"]
                filing.total_liabilities = row["total_liabilities"]
                filing.net_assets = row["net_assets"]

            session.commit()

    @staticmethod
    def _clean_string(value):
        if pd.isna(value):
            return None
        cleaned = str(value).strip()
        return cleaned or None
