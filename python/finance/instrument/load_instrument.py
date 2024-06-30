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

def transform_and_insert(connection: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name:str, symbol:str, primary_keys:list[str], partition_by:str=None, reset_index=True) -> yf.Ticker:
    if not df.empty:
        if isinstance(df,pd.Series):
            df = df.to_frame()
        if symbol:
            df["symbol"] = symbol.upper()
        if reset_index:
            df.reset_index(inplace=True)    
            if df.columns[0] == 'index':
                df = df.rename(columns={"index": "Date"})
        sync_and_load(connection, df, table_name, primary_keys, partition_by)
    return df


def load(connection: duckdb.DuckDBPyConnection, symbol:str):
    ticker = extract(symbol)
    # show holders
    major_holders = ticker.major_holders
    major_holders["Date Reported"] = datetime.today()
    transform_and_insert(connection, major_holders, "major_holders", symbol, [], 'symbol', reset_index=False)
    transform_and_insert(connection, ticker.institutional_holders, "institutional_holders", symbol, [], 'symbol', reset_index=False)
    transform_and_insert(connection, ticker.mutualfund_holders, "mutualfund_holders", symbol, [], 'symbol', reset_index=False)
    transform_and_insert(connection, ticker.insider_transactions, "insider_transactions", symbol, [], 'symbol', reset_index=False)
    insider_purchases = ticker.insider_purchases
    insider_purchases["Date Reported"] = datetime.today()
    transform_and_insert(connection, insider_purchases, "insider_purchases", symbol, [], 'symbol', reset_index=False)
    insider_roster_holders = ticker.insider_roster_holders
    insider_roster_holders["Date Reported"] = datetime.today()
    transform_and_insert(connection, insider_roster_holders, "insider_roster_holders", symbol, [], 'symbol', reset_index=False) # Partion this table no need of primary key

    # show actions (dividends, splits, capital gains)
    transform_and_insert(connection, ticker.actions, "actions", symbol, ['symbol', 'date'])
    transform_and_insert(connection, ticker.dividends, "dividends", symbol, ['symbol', 'date'])
    transform_and_insert(connection, ticker.capital_gains, "capital_gains", symbol, ['symbol', 'date']) # only for mutual funds & etfs
    transform_and_insert(connection, ticker.splits, "splits", symbol, ['symbol', 'date'])

    # show financials:
    # - income statement
    # see `Ticker.get_income_stmt()` for more options    
    transform_and_insert(connection, ticker.income_stmt.transpose(), "income_stmt", symbol, [], 'symbol')
    transform_and_insert(connection, ticker.quarterly_income_stmt.transpose(), "quarterly_income_stmt", symbol, [], 'symbol')
    # - balance sheet
    transform_and_insert(connection, ticker.balance_sheet.transpose(), "balance_sheet", symbol, [], 'symbol')
    transform_and_insert(connection, ticker.quarterly_balance_sheet.transpose(), "quarterly_balance_sheet", symbol, [], 'symbol')
    # - cash flow statement
    transform_and_insert(connection, ticker.cashflow.transpose(), "cashflow", symbol, [], 'symbol')
    transform_and_insert(connection, ticker.quarterly_cashflow.transpose(), "quarterly_cashflow", symbol, [], 'symbol')

    # show recommendations
    recommendations = ticker.recommendations
    recommendations["Date Reported"] = datetime.today()
    transform_and_insert(connection, recommendations, "recommendations", symbol, [], 'symbol', reset_index=False)
    recommendations_summary = ticker.recommendations_summary
    recommendations_summary["Date Reported"] = datetime.today()
    transform_and_insert(connection, recommendations_summary, "recommendations_summary", symbol, [], 'symbol', reset_index=False)
    transform_and_insert(connection, ticker.upgrades_downgrades, "upgrades_downgrades", symbol, [], 'symbol', reset_index=False)

    # show share count
    shares_full = ticker.get_shares_full(start='1901-01-01', end=datetime.today().strftime("%Y-%m-%d"))
    shares_full = shares_full.to_frame()
    shares_full.reset_index(inplace=True) 
    shares_full = shares_full.rename(columns={0: "shares", "index": "Date"})    
    shares_full = shares_full.drop_duplicates(subset=['Date'])
    transform_and_insert(connection, shares_full, "shares_outstanding", symbol, ['symbol', 'date'], 'symbol', reset_index=False)

    # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
    # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
    earnings_dates = ticker.earnings_dates
    earnings_dates = earnings_dates.rename(columns={"Surprise(%)": "Surprise_Percent"})    
    transform_and_insert(connection, earnings_dates, "earnings_dates", symbol, ['symbol', "earnings_date"])

def load_news(connection: duckdb.DuckDBPyConnection, symbol:str):
    ticker = extract(symbol)
    # show news
    news = pd.DataFrame(ticker.news)
    news["Date Reported"] = datetime.today()    
    transform_and_insert(connection, news, "news_links", symbol, [], 'symbol', reset_index=False)

def load_options_and_quotes(connection: duckdb.DuckDBPyConnection, symbol:str, period:str):
    ticker = extract(symbol)
    
    # get option chain for specific expiration
    for expiry_date in ticker.options:
        opt = ticker.option_chain(expiry_date)
        load_option_chain(connection, opt.calls, expiry_date, "call", symbol, period)
        load_option_chain(connection, opt.puts, expiry_date, "put", symbol, period)

def load_option_chain(connection: duckdb.DuckDBPyConnection, options: pd.DataFrame, expiry_date:str, option_type:str, symbol:str, option_quote_period:str="1d"):
    contracts = options[['contractSymbol', 'strike', 'currency']]
    contracts = contracts.rename(columns={"contractSymbol": "symbol"})
    contracts['type'] = option_type
    contracts['underlying'] = symbol
    contracts['expiry_date'] = datetime.strptime(str(expiry_date), "%Y-%m-%d")
    sync_and_load(connection, contracts, "option_chain", ['symbol'])
    from finance.quotes import load_historical_quotes    
    contract_price = options[['contractSymbol', "openInterest", 'impliedVolatility']]
    for row in contract_price.itertuples(index=False):
        load_historical_quotes.load(connection, symbol=row.contractSymbol, period=option_quote_period,
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
    #connection.execute(f"DROP TABLE insider_purchases;")
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    #load(connection, "msft")
    #load_news(connection, "msft")
    load_options_and_quotes(connection, "msft", period="max")
