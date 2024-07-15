from finance.core import get_tickers as gt
import pandas as pd
import duckdb
from finance.core import VaultDB

# from vaultdb import VaultDB

# Set up the logger
import logging

logger = logging.getLogger()


class Tickers(VaultDB):

    def extract(self) -> pd.DataFrame:
        df = gt.get_tickers_dataframe()
        logger.debug(df.head())
        return df

    def transform(self, tickers_df: pd.DataFrame) -> pd.DataFrame:
        tickers_df["lastsale"] = pd.to_numeric(tickers_df["lastsale"].str.replace("$", ""))
        tickers_df["netchange"] = pd.to_numeric(tickers_df["netchange"])
        tickers_df["pctchange"] = pd.to_numeric(tickers_df["pctchange"].str.replace("%", ""))
        tickers_df["marketCap"] = pd.to_numeric(tickers_df["marketCap"])
        tickers_df["ipoyear"] = pd.to_numeric(tickers_df["ipoyear"])
        tickers_df["volume"] = pd.to_numeric(tickers_df["volume"])
        return tickers_df

    def load(self):
        tickers_df = self.extract()
        tickers_df = self.transform(tickers_df)

        self.sync_load_and_merge(tickers_df, database_name, "tickers", ["exchange", "symbol"], "exchange")


if __name__ == "__main__":
    database_name = "test"    
    tickers = Tickers(database_name)
    tickers.login("vaultdb", "test123")    
    tickers.connection.execute(f"TRUNCATE DATABASE {database_name};")
    tickers.load()
