import requests_cache
import yfinance as yf
import pandas as pd
from vaultdb import VaultDB

# Set up the logger
import logging

logger = logging.getLogger()


class Quotes(VaultDB):

    def extract(self, symbol: str, period: str = "1d") -> pd.DataFrame:
        """ """
        session = requests_cache.CachedSession(
            f"{self.database_name}.{symbol}.quotes.yfinance.cache",
        )
        session.headers["User-agent"] = "my-program/1.0"
        ticker = yf.Ticker(symbol.upper(), session=session)
        hist = ticker.history(period=period)
        return hist

    def transform_and_insert(self, df: pd.DataFrame, table_name: str) -> yf.Ticker:
        """ """
        df.reset_index(inplace=True)
        self.sync_load_and_merge(df, table_name, ["symbol", "date"], "symbol")
        return df

    def load(self, symbol: str, period: str = "1d", **additionalvalues):
        """ """
        history = self.extract(symbol, period=period)
        history["symbol"] = symbol.upper()
        for k, v in additionalvalues.items():
            history[k] = v
        self.transform_and_insert(history, "quote")


if __name__ == "__main__":
    import os
    from vaultdb import clone

    database_name = "test"
    connection = clone("vaultdb", "test123", f"/workspace/{database_name}.db", aws_region="us-east-1")
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    load("MSFT240628C00220000", None)
