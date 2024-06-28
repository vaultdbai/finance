import os
import time
from vaultdb import download
from duckdb import login

from celery import Celery

celery_app = Celery("tasks", broker="redis://redis/0", backend="redis://redis/0")

from .finance.instrument import all_tickers_load, load_instrument
from .finance.quotes import load_historical_quotes


@celery_app.task()
def load_all_tickers(database_name="test"):
    filename = f"/workspace/{database_name}.db"
    if not os.path.isfile(filename):
        url = f"http://test-public-storage-440955376164.s3-website.us-east-1.amazonaws.com/catalogs/{database_name}.db"
        filename = download(url, filename)
    connection = login.cognito("vaultdb", "test123", filename, aws_region="us-east-1")
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    all_tickers_load.load(connection)
    connection.execute(f"PUSH DATABASE {database_name};")


@celery_app.task()
def load_quotes(database_name="test", period="1d"):
    filename = f"/workspace/{database_name}.db"
    if not os.path.isfile(filename):
        url = f"http://test-public-storage-440955376164.s3-website.us-east-1.amazonaws.com/catalogs/{database_name}.db"
        filename = download(url, filename)
    connection = login.cognito("vaultdb", "test123", filename, aws_region="us-east-1")
    tickers = connection.execute(f"select exchange, symbol from tickers;")
    for row in tickers.itertuples(index=False):
        load_historical_quotes.load(connection, row.symbol, period=period)
        time.sleep(60)

    connection.execute(f"PUSH DATABASE {database_name};")
    connection.execute(f"TRUNCATE DATABASE {database_name};")


@celery_app.task()
def load_instrument_details(database_name="test"):
    filename = f"/workspace/{database_name}.db"
    if not os.path.isfile(filename):
        url = f"http://test-public-storage-440955376164.s3-website.us-east-1.amazonaws.com/catalogs/{database_name}.db"
        filename = download(url, filename)
    connection = login.cognito("vaultdb", "test123", filename, aws_region="us-east-1")
    tickers = connection.execute(f"select exchange, symbol from tickers;")
    for row in tickers.itertuples(index=False):
        load_instrument.load(connection, row.symbol)
        time.sleep(60)
    connection.execute(f"PUSH DATABASE {database_name};")
    connection.execute(f"TRUNCATE DATABASE {database_name};")


@celery_app.task()
def load_options_and_quotes(database_name="test", period="1d"):
    filename = f"/workspace/{database_name}.db"
    if not os.path.isfile(filename):
        url = f"http://test-public-storage-440955376164.s3-website.us-east-1.amazonaws.com/catalogs/{database_name}.db"
        filename = download(url, filename)
    connection = login.cognito("vaultdb", "test123", filename, aws_region="us-east-1")
    tickers = connection.execute(f"select exchange, symbol from tickers;")
    for row in tickers.itertuples(index=False):
        load_instrument.load_options_and_quotes(connection, row.symbol, period=period)
        time.sleep(60)

    connection.execute(f"PUSH DATABASE {database_name};")
    connection.execute(f"TRUNCATE DATABASE {database_name};")


if __name__ == "__main__":
    load_all_tickers()
    load_quotes(period="max")
    load_quotes(period="1d")
    load_options_and_quotes()
