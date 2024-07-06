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
    session = requests_cache.CachedSession(f"{__file__}.yfinance.cache", )
    session.headers['User-agent'] = 'my-program/1.0'
    ticker = yf.Ticker(symbol.upper(), session=session)
    logger.debug(ticker.isin)
    return ticker

def transform_and_insert(connection: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name:str, symbol:str, primary_keys:list[str], partition_by:str=None, reset_index=True):
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

def try_transform_and_insert(connection: duckdb.DuckDBPyConnection, df, table_name:str, symbol:str, primary_keys:list[str], partition_by:str=None, reset_index=True):
    try:
        if isinstance(df, yf.Ticker):
            df = getattr(df, table_name)
        transform_and_insert(connection, df, table_name, symbol, primary_keys, partition_by, reset_index=reset_index)
    except Exception as ex:
        logger.error(ex)

def load(connection: duckdb.DuckDBPyConnection, symbol:str):
    ticker = extract(symbol)
    # show holders
    try:
        major_holders = ticker.major_holders
        major_holders["Date Reported"] = datetime.today()
        transform_and_insert(connection, major_holders, "major_holders", symbol, [], 'symbol', reset_index=False)
    except Exception as ex:
        logger.error(ex)
        
    try_transform_and_insert(connection, ticker, "institutional_holders", symbol, [], 'symbol', reset_index=False)
    try_transform_and_insert(connection, ticker, "mutualfund_holders", symbol, [], 'symbol', reset_index=False)
    try_transform_and_insert(connection, ticker, "insider_transactions", symbol, [], 'symbol', reset_index=False)
    
    try:
        insider_purchases = ticker.insider_purchases
        insider_purchases["Date Reported"] = datetime.today()
        try_transform_and_insert(connection, insider_purchases, "insider_purchases", symbol, [], 'symbol', reset_index=False)
    except Exception as ex:
        logger.error(ex)
        
    try:
        insider_roster_holders = ticker.insider_roster_holders
        insider_roster_holders["Date Reported"] = datetime.today()
        try_transform_and_insert(connection, insider_roster_holders, "insider_roster_holders", symbol, [], 'symbol', reset_index=False) # Partion this table no need of primary key
    except Exception as ex:
        logger.error(ex)

    # show actions (dividends, splits, capital gains)
    try_transform_and_insert(connection, ticker, "actions", symbol, ['symbol', 'date'])
    try_transform_and_insert(connection, ticker, "dividends", symbol, ['symbol', 'date'])
    try_transform_and_insert(connection, ticker, "capital_gains", symbol, ['symbol', 'date']) # only for mutual funds & etfs
    try_transform_and_insert(connection, ticker, "splits", symbol, ['symbol', 'date'])

    # show financials:
    # - income statement
    # see `Ticker.get_income_stmt()` for more options    
    try:
        try_transform_and_insert(connection, ticker.income_stmt.transpose(), "income_statement", symbol, [], 'symbol')
    except Exception as ex:
        logger.error(ex)
        
    try:
        try_transform_and_insert(connection, ticker.quarterly_income_stmt.transpose(), "quarterly_income_statement", symbol, [], 'symbol')
    except Exception as ex:
        logger.error(ex)
# - balance sheet
    try:
        try_transform_and_insert(connection, ticker.balance_sheet.transpose(), "balance_sheet", symbol, [], 'symbol')
    except Exception as ex:
        logger.error(ex)
    try:
        try_transform_and_insert(connection, ticker.quarterly_balance_sheet.transpose(), "quarterly_balance_sheet", symbol, [], 'symbol')
    except Exception as ex:
        logger.error(ex)
    # - cash flow statement
    try:
        try_transform_and_insert(connection, ticker.cashflow.transpose(), "cashflow", symbol, [], 'symbol')
    except Exception as ex:
        logger.error(ex)
    try:
        try_transform_and_insert(connection, ticker.quarterly_cashflow.transpose(), "quarterly_cashflow", symbol, [], 'symbol')
    except Exception as ex:
        logger.error(ex)

    # show recommendations
    try:
        recommendations = ticker.recommendations
        recommendations["Date Reported"] = datetime.today()
        try_transform_and_insert(connection, recommendations, "recommendations", symbol, [], 'symbol', reset_index=False)
    except Exception as ex:
        logger.error(ex)
        
    try:
        recommendations_summary = ticker.recommendations_summary
        recommendations_summary["Date Reported"] = datetime.today()
        try_transform_and_insert(connection, recommendations_summary, "recommendations_summary", symbol, [], 'symbol', reset_index=False)
    except Exception as ex:
        logger.error(ex)
        
    try_transform_and_insert(connection, ticker, "upgrades_downgrades", symbol, [], 'symbol', reset_index=False)

    # show share count
    try:
        shares_full = ticker.get_shares_full(start='1901-01-01', end=datetime.today().strftime("%Y-%m-%d"))
        shares_full = shares_full.to_frame()
        shares_full.reset_index(inplace=True) 
        shares_full = shares_full.rename(columns={0: "shares", "index": "Date"})    
        shares_full = shares_full.drop_duplicates(subset=['Date'])
        try_transform_and_insert(connection, shares_full, "shares_outstanding", symbol, ['symbol', 'date'], 'symbol', reset_index=False)
    except Exception as ex:
        logger.error(ex)

    # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
    # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
    try:
        earnings_dates = ticker.earnings_dates
        earnings_dates = earnings_dates.rename(columns={"Surprise(%)": "Surprise_Percent"})    
        try_transform_and_insert(connection, earnings_dates, "earnings_dates", symbol, ['symbol', "earnings_date"])
    except Exception as ex:
        logger.error(ex)

def load_news(connection: duckdb.DuckDBPyConnection, symbol:str):
    ticker = extract(symbol)
    # show news
    news = pd.DataFrame(ticker.news)
    news["Date Reported"] = datetime.today()    
    try_transform_and_insert(connection, news, "news_links", symbol, [], 'symbol', reset_index=False)

def load_options_and_quotes(connection: duckdb.DuckDBPyConnection, symbol:str, period:str):    
    try:
        ticker = extract(symbol)
        # get option chain for specific expiration
        for expiry_date in ticker.options:
            opt = ticker.option_chain(expiry_date)
            load_option_chain(connection, opt.calls, expiry_date, "call", symbol, period)
            load_option_chain(connection, opt.puts, expiry_date, "put", symbol, period)
    except Exception as ex:
        logger.error(ex)

def load_option_chain(connection: duckdb.DuckDBPyConnection, options: pd.DataFrame, expiry_date:str, option_type:str, symbol:str, option_quote_period:str="1d"):
    try:
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
    except Exception as ex:
        logger.error(ex)
  

if __name__ == "__main__":
    import os
    from vaultdb import clone
    database_name = "test"
    connection = clone("vaultdb", "test123", f"/workspace/{database_name}.db", aws_region="us-east-1")    
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    #connection.execute(f"DROP TABLE insider_purchases;")
    #load(connection, "msft")
    #load_news(connection, "msft")
    load_options_and_quotes(connection, "msft", period="max")
