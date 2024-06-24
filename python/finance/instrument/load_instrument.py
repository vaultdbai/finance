import requests_cache
import yfinance as yf
import pandas as pd
import duckdb

from vaultdb import sync_and_load

# Set up the logger
import logging

logger = logging.getLogger()

def extract(symbol:str) -> pd.DataFrame:
    session = requests_cache.CachedSession('yfinance.cache')
    session.headers['User-agent'] = 'my-program/1.0'
    ticker = yf.Ticker(symbol.upper(), session=session)
    logger.debug(ticker.isin)
    return ticker

def transform_and_insert(connection: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name:str, symbol:str) -> yf.Ticker:
    df["symbol"] = symbol    
    sync_and_load(connection, df, table_name, ['symbol'])
    return df


def load(connection: duckdb.DuckDBPyConnection, symbol:str, database_name: str):
    ticker = extract(symbol)
    # show actions (dividends, splits, capital gains)
    transform_and_insert(connection, ticker.actions, "actions", symbol)
    transform_and_insert(connection, ticker.dividends, "dividends", symbol)
    transform_and_insert(connection, ticker.splits, "splits", symbol)
    transform_and_insert(connection, ticker.capital_gains, "capital_gains", symbol) # only for mutual funds & etfs

    # show financials:
    # - income statement
    # see `Ticker.get_income_stmt()` for more options    
    transform_and_insert(connection, ticker.income_stmt, "income_stmt", symbol)
    transform_and_insert(connection, ticker.quarterly_income_stmt, "quarterly_income_stmt", symbol)
    # - balance sheet
    transform_and_insert(connection, ticker.balance_sheet, "balance_sheet", symbol)
    transform_and_insert(connection, ticker.quarterly_balance_sheet, "quarterly_balance_sheet", symbol)
    # - cash flow statement
    transform_and_insert(connection, ticker.cashflow, "cashflow", symbol)
    transform_and_insert(connection, ticker.quarterly_cashflow, "quarterly_cashflow", symbol)
    # show holders
    transform_and_insert(connection, ticker.major_holders, "major_holders", symbol)
    transform_and_insert(connection, ticker.institutional_holders, "institutional_holders", symbol)
    transform_and_insert(connection, ticker.mutualfund_holders, "mutualfund_holders", symbol)
    transform_and_insert(connection, ticker.insider_transactions, "insider_transactions", symbol)
    transform_and_insert(connection, ticker.insider_purchases, "insider_purchases", symbol)
    transform_and_insert(connection, ticker.insider_roster_holders, "insider_roster_holders", symbol)

    # show recommendations
    transform_and_insert(connection, ticker.recommendations, "recommendations", symbol)
    transform_and_insert(connection, ticker.recommendations_summary, "recommendations_summary", symbol)
    transform_and_insert(connection, ticker.upgrades_downgrades, "upgrades_downgrades", symbol)

    # show share count
    shares_full = ticker.get_shares_full(start="2022-01-01", end=None)
    transform_and_insert(connection, shares_full, "shares_full", symbol)

    # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
    # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
    transform_and_insert(connection, ticker.earnings_dates, "earnings_dates", symbol)

    # show ISIN code - *experimental*
    # ISIN = International Securities Identification Number
    #msft.isin

    # show options expirations
    transform_and_insert(connection, ticker.options, "options", symbol)

    # show news
    transform_and_insert(connection, ticker.news, "news", symbol)

    # get option chain for specific expiration
    opt = ticker.option_chain('YYYY-MM-DD')
    transform_and_insert(connection, opt, "option_chain", symbol)
    # data available via: opt.calls, opt.puts    
  
    connection.execute(f"PUSH DATABASE {database_name};")

    connection.execute(f"TRUNCATE DATABASE {database_name};")


if __name__ == "__main__":
    import os
    from vaultdb import download
    from duckdb import login
    database_name = "test"
    filename = f"/workspace/{database_name}.db"
    if not os.path.isfile(filename):
        url = f"http://test-public-storage-440955376164.s3-website.us-east-1.amazonaws.com/catalogs/{database_name}.db"
        filename = download(url, filename)    
    connection = login.cognito("vaultdb","test123", filename, aws_region="us-east-1")
    load(connection, "msft", database_name)
