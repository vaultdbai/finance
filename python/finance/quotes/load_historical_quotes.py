import requests_cache
import yfinance as yf
import pandas as pd
import duckdb

from vaultdb import sync_and_load

# Set up the logger
import logging

logger = logging.getLogger()


def extract(symbol: str, period) -> pd.DataFrame:
    session = requests_cache.CachedSession("yfinance.cache")
    session.headers["User-agent"] = "my-program/1.0"
    ticker = yf.Ticker(symbol.upper(), session=session)
    hist = ticker.history(period=period)
    return hist


def transform_and_insert(
    connection: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    symbol: str,
) -> yf.Ticker:
    df["symbol"] = symbol.upper()
    df.reset_index(inplace=True)
    sync_and_load(connection, df, table_name, ["symbol", "date"], "symbol")
    try:
        connection.query(f"ALTER TABLE quote PARTITION BY symbol;")
    except:
        pass

    return df


def load(
    connection: duckdb.DuckDBPyConnection,
    symbol: str,
    period: str = "1d",
    **additionalvalues
):
    history = extract(symbol, period=period)
    for k, v in additionalvalues.items():
        history[k] = v
    transform_and_insert(connection, history, "quote", symbol)


if __name__ == "__main__":
    import os
    from vaultdb import clone
    database_name = "test"
    connection = clone("vaultdb", "test123", f"/workspace/{database_name}.db", aws_region="us-east-1")    
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    load(connection, "MSFT240628C00220000", None)
