from finance.core import get_tickers as gt
import pandas as pd
import duckdb

# Set up the logger
import logging

logger = logging.getLogger()

def extract() -> pd.DataFrame:
    df = gt.get_tickers_dataframe()
    logger.debug(df.head())
    return df


def transform(tickers_df: pd.DataFrame) -> pd.DataFrame:
    tickers_df["lastsale"] = pd.to_numeric(tickers_df["lastsale"].str.replace('$', ''))
    tickers_df["netchange"] = pd.to_numeric(tickers_df["netchange"])
    tickers_df["pctchange"] = pd.to_numeric(tickers_df["pctchange"].str.replace('%', ''))
    tickers_df["marketCap"] = pd.to_numeric(tickers_df["marketCap"])
    tickers_df["ipoyear"] = pd.to_numeric(tickers_df["ipoyear"])
    tickers_df["volume"] = pd.to_numeric(tickers_df["volume"])
    return tickers_df


def load(connection: duckdb.DuckDBPyConnection):
    tickers_df = extract()
    tickers_df = transform(tickers_df)
    
    """
    # create the table "my_table" from the DataFrame "my_df"
    # Note: duckdb.sql connects to the default in-memory database connection
    connection.sql("CREATE OR REPLACE TABLE temp_ticket AS SELECT * FROM tickers_df")
    df = connection.sql("SHOW temp_ticket;").fetchdf()
    
    create_stmt = "CREATE OR REPLACE TABLE ticker("
    for row in df.itertuples(index=False):
        create_stmt += f"{row.column_name} {row.column_type}, "
    create_stmt += " PRIMARY KEY(exchange_name, symbol))"
    connection.sql("DROP TABLE temp_ticket;")
    """
   
    create_stmt = "CREATE OR REPLACE TABLE tickers(exchange VARCHAR, symbol VARCHAR, name VARCHAR, lastsale VARCHAR, netchange DOUBLE, pctchange DOUBLE, marketCap BIGINT, country VARCHAR, ipoyear INTEGER, volume BIGINT, sector VARCHAR, industry VARCHAR, url VARCHAR,  PRIMARY KEY(exchange, symbol))"
    
    logger.debug(create_stmt)   
    
    connection.sql(create_stmt)
     
    connection.sql("ALTER TABLE tickers PARTITION BY exchange;")
        
    # insert into the table "my_table" from the DataFrame "my_df"
    connection.sql("INSERT INTO tickers(exchange, symbol, name, lastsale, netchange, pctchange, marketCap, country, ipoyear, volume, sector, industry, url) SELECT exchange, symbol, name, lastsale, netchange, pctchange, marketCap, country, ipoyear, volume, sector, industry, url FROM tickers_df")

if __name__ == "__main__":
    import os
    from vaultdb import clone
    database_name = "test"
    connection = clone("vaultdb", "test123", f"/workspace/{database_name}.db", aws_region="us-east-1")    
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    load(connection)
