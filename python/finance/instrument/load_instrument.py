import requests_cache
import yfinance as yf
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from vaultdb import sync_and_load

# Set up the logger
import logging

logger = logging.getLogger()

yesterday = datetime.today() - timedelta(days = 1)

def extract(symbol:str) -> yf.Ticker:
    session = requests_cache.CachedSession('yfinance.cache')
    session.headers['User-agent'] = 'my-program/1.0'
    ticker = yf.Ticker(symbol.upper(), session=session)
    logger.debug(ticker.isin)
    return ticker

def transform_and_insert(connection: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name:str, symbol:str, primary_keys:list[str], partition_by:str=None) -> yf.Ticker:
    if not df.empty:
        if isinstance(df,pd.Series):
            df = df.to_frame()
        if symbol:
            df["symbol"] = symbol.upper()
        df.reset_index(inplace=True)    
        sync_and_load(connection, df, table_name, primary_keys, partition_by)
    return df


def load(connection: duckdb.DuckDBPyConnection, database_name: str, symbol:str, option_quote_period:str="1d"):
    ticker = extract(symbol)
    # show actions (dividends, splits, capital gains)
    transform_and_insert(connection, ticker.actions, "actions", symbol, ['symbol', 'date'])
    transform_and_insert(connection, ticker.dividends, "dividends", symbol, ['symbol', 'date'])
    transform_and_insert(connection, ticker.capital_gains, "capital_gains", symbol, ['symbol', 'date']) # only for mutual funds & etfs
    transform_and_insert(connection, ticker.splits, "splits", symbol, ['symbol', 'date'])

    # show financials:
    # - income statement
    # see `Ticker.get_income_stmt()` for more options    
    transform_and_insert(connection, ticker.income_stmt, "income_stmt", symbol, ['symbol'])
    transform_and_insert(connection, ticker.quarterly_income_stmt, "quarterly_income_stmt", symbol, ['symbol'])
    # - balance sheet
    transform_and_insert(connection, ticker.balance_sheet, "balance_sheet", symbol, ['symbol'])
    transform_and_insert(connection, ticker.quarterly_balance_sheet, "quarterly_balance_sheet", symbol, ['symbol'])
    # - cash flow statement
    transform_and_insert(connection, ticker.cashflow, "cashflow", symbol, ['symbol'])
    transform_and_insert(connection, ticker.quarterly_cashflow, "quarterly_cashflow", symbol, ['symbol'])
    # show holders
    major_holders = ticker.major_holders
    major_holders["Date Reported"] = datetime.today()
    transform_and_insert(connection, major_holders, "major_holders", symbol, ['symbol', "date_reported"])
    transform_and_insert(connection, ticker.institutional_holders, "institutional_holders", symbol, ['symbol', "date_reported"])
    transform_and_insert(connection, ticker.mutualfund_holders, "mutualfund_holders", symbol, ['symbol', "date_reported"])
    transform_and_insert(connection, ticker.insider_transactions, "insider_transactions", symbol, ['symbol', "start_date"])
    insider_purchases = ticker.insider_purchases
    insider_purchases["Date Reported"] = datetime.today()
    transform_and_insert(connection, insider_purchases, "insider_purchases", symbol, ['symbol', "date_reported"])
    insider_roster_holders = ticker.insider_roster_holders
    insider_roster_holders["Date Reported"] = datetime.today()
    transform_and_insert(connection, insider_roster_holders, "insider_roster_holders", symbol, [], 'symbol') # Partion this table no need of primary key

    # show recommendations
    recommendations = ticker.recommendations
    recommendations["Date Reported"] = datetime.today()
    transform_and_insert(connection, recommendations, "recommendations", symbol, ['symbol', "date_reported", "period"])
    recommendations_summary = ticker.recommendations_summary
    recommendations_summary["Date Reported"] = datetime.today()
    transform_and_insert(connection, recommendations_summary, "recommendations_summary", symbol, ['symbol', "date_reported", "period"])
    transform_and_insert(connection, ticker.upgrades_downgrades, "upgrades_downgrades", symbol, ['symbol', "gradedate", "firm"])

    # show share count
    shares_full = ticker.get_shares_full(start=yesterday.strftime("%Y-%m-%d"), end=None)
    transform_and_insert(connection, shares_full, "shares_full", symbol, ['symbol', 'date'])

    # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
    # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
    transform_and_insert(connection, ticker.earnings_dates, "earnings_dates", symbol, ['symbol', "earnings_date"])

    # show ISIN code - *experimental*
    # ISIN = International Securities Identification Number
    #msft.isin
    # get option chain for specific expiration
    for exprity_date in ticker.options:
        opt = ticker.option_chain(exprity_date)
        load_option_chain(connection, database_name, opt.calls, exprity_date, "call", symbol, option_quote_period)
        load_option_chain(connection, database_name, opt.puts, exprity_date, "put", symbol, option_quote_period)

    # show news
    transform_and_insert(connection, ticker.news, "news", symbol, ['symbol'])

def load_option_chain(connection: duckdb.DuckDBPyConnection, database_name: str, options: pd.DataFrame, exprity_date:str, option_type:str, symbol:str, option_quote_period:str="1d"):
    contracts = options[['contractSymbol', "type", "exprity_date", 'strike', 'currency']]
    contracts = contracts.rename(columns={"contractSymbol": "symbol"})
    contracts['type'] = option_type
    contracts['underlying'] = symbol
    contracts['exprity_date'] = datetime.strptime(str(exprity_date), "%Y-%m-%d")
    transform_and_insert(connection, contracts, "option_chain", None, ['symbol'])
    from ..quotes import load_historical_quotes    
    contract_price = options[['contractSymbol', "volume", "openInterest", 'impliedVolatility']]
    for row in contract_price.itertuples(index=False):
        load_historical_quotes.load(connection, database_name=database_name, 
                                    symbol=row.contractSymbol, period=option_quote_period,
                                    volume=row.volume, 
                                    openinterest=row.openInterest, 
                                    impliedvolatility=row.impliedVolatility)
            
    # data available via: opt.calls, opt.puts    
  

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
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    load(connection, database_name, "msft")
