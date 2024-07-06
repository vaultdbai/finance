import os
import time
import vaultdb
import shutil

# Set up the logger
import logging

logger = logging.getLogger()

from vaultdb.compute import App

vaultdb_user = os.getenv("vaultdb_user")
vaultdb_password = os.getenv("vaultdb_password")

from finance.instrument import all_tickers_load, load_instrument
from finance.quotes import load_historical_quotes

WAIT_TIME = 20

@App.task()
def load_all_tickers(database_name: str = "finance"):
    clone_path = "/tmp/tickers"
    shutil.rmtree(clone_path, ignore_errors=True)
    os.makedirs(clone_path)
    connection = vaultdb.clone(vaultdb_user, vaultdb_password, database_name, path=clone_path)
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    all_tickers_load.load(connection)
    connection.execute(f"PUSH DATABASE {database_name};")


@App.task()
def load_quotes(database_name: str ="finance", period: str ="1d", symbol_prefix:str=None):
    clone_path = "/tmp/quotes" 
    shutil.rmtree(clone_path, ignore_errors=True)
    os.makedirs(clone_path)
    connection = vaultdb.clone(vaultdb_user, vaultdb_password, database_name, path=clone_path)
    connection.execute(f"PRAGMA enable_data_inheritance;;")
    if symbol_prefix:        
        tickers = connection.execute(f"select exchange, symbol from tickers where symbol like '{symbol_prefix}%';").fetchdf()
    else:
        tickers = connection.execute(f"select exchange, symbol from tickers;").fetchdf()
    connection.execute(f"PRAGMA disable_data_inheritance;")
    for row in tickers.itertuples(index=False):
        try:
            load_historical_quotes.load(connection, row.symbol, period=period)
            connection.execute(f"PUSH DATABASE {database_name};")
            connection.execute(f"TRUNCATE DATABASE {database_name};")
        except Exception as ex:
            logger.error(ex)


@App.task()
def load_instrument_details(database_name: str ="finance"):
    clone_path = "/tmp/instrument" 
    shutil.rmtree(clone_path, ignore_errors=True)
    os.makedirs(clone_path)
    connection = vaultdb.clone(vaultdb_user, vaultdb_password, database_name, path=clone_path)
    connection.execute(f"PRAGMA enable_data_inheritance;;")
    tickers = connection.execute(f"select exchange, symbol from tickers;").fetchdf()
    for row in tickers.itertuples(index=False):
        try:
            load_instrument.load(connection, row.symbol)
            connection.execute(f"PUSH DATABASE {database_name};")
            connection.execute(f"TRUNCATE DATABASE {database_name};")
        except Exception as ex:
            logger.error(ex)


@App.task()
def load_options_and_quotes(database_name: str ="finance", period: str ="1d"):
    clone_path = "/tmp/options" 
    shutil.rmtree(clone_path, ignore_errors=True)
    os.makedirs(clone_path)
    connection = vaultdb.clone(vaultdb_user, vaultdb_password, database_name, path=clone_path)
    connection.execute(f"PRAGMA enable_data_inheritance;;")
    tickers = connection.execute(f"select exchange, symbol from tickers;").fetchdf()
    for row in tickers.itertuples(index=False):
        try:
            load_instrument.load_options_and_quotes(connection, row.symbol, period=period)
            connection.execute(f"PUSH DATABASE {database_name};")
            connection.execute(f"TRUNCATE DATABASE {database_name};")
        except Exception as ex:
            logger.error(ex)


from vaultdb.compute.schedules import crontab

App.conf.beat_schedule = {
    # Executes every Monday morning at 7:30 a.m.
    'load_quotes_daily': {
        'task': 'tasks.load_quotes',
        'schedule': crontab(hour=5, minute=30, day_of_week=1),
        'args': ("finance", "1d"),
    },
}

if __name__ == "__main__":
    load_instrument_details()
    # load_quotes(period="max")
    # load_quotes(period="1d")
    # load_options_and_quotes()
    # import sys
    # from celery.__main__ import main
    # sys.argv=["celery", "-A", "tasks", "worker", "--pool=solo", "--loglevel=info"]
    # main()
