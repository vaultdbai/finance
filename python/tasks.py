import os
import time
import vaultdb
from duckdb import login

from celery import Celery

redis_broker = os.getenv("redis_broker", "redis:6379")

celery_app = Celery(
    "tasks", broker=f"redis://{redis_broker}/0", backend=f"redis://{redis_broker}/0"
)

from finance.instrument import all_tickers_load, load_instrument
from finance.quotes import load_historical_quotes


@celery_app.task()
def load_all_tickers(database_name: str = "test"):
    connection = vaultdb.clone("vaultdb", "test123", database_name)
    connection.execute(f"TRUNCATE DATABASE {database_name};")
    all_tickers_load.load(connection)
    connection.execute(f"PUSH DATABASE {database_name};")


@celery_app.task()
def load_quotes(database_name: str ="test", period: str ="1d", symbol_prefix:str=None):
    connection = vaultdb.clone("vaultdb", "test123", database_name)
    connection.execute(f"PRAGMA enable_data_inheritance;;")
    if symbol_prefix:        
        tickers = connection.execute(f"select exchange, symbol from tickers where symbol like '{symbol_prefix}%';").fetchdf()
    else:
        tickers = connection.execute(f"select exchange, symbol from tickers;").fetchdf()
    connection.execute(f"PRAGMA disable_data_inheritance;")
    for row in tickers.itertuples(index=False):
        load_historical_quotes.load(connection, row.symbol, period=period)
        time.sleep(60)

    connection.execute(f"PUSH DATABASE {database_name};")
    connection.execute(f"TRUNCATE DATABASE {database_name};")


@celery_app.task()
def load_instrument_details(database_name: str ="test"):
    connection = vaultdb.clone("vaultdb", "test123", database_name)
    tickers = connection.execute(f"select exchange, symbol from tickers;").fetchdf()
    for row in tickers.itertuples(index=False):
        load_instrument.load(connection, row.symbol)
        time.sleep(60)
    connection.execute(f"PUSH DATABASE {database_name};")
    connection.execute(f"TRUNCATE DATABASE {database_name};")


@celery_app.task()
def load_options_and_quotes(database_name: str ="test", period: str ="1d"):
    connection = vaultdb.clone("vaultdb", "test123", database_name)
    tickers = connection.execute(f"select exchange, symbol from tickers;").fetchdf()
    for row in tickers.itertuples(index=False):
        load_instrument.load_options_and_quotes(connection, row.symbol, period=period)
        time.sleep(60)

    connection.execute(f"PUSH DATABASE {database_name};")
    connection.execute(f"TRUNCATE DATABASE {database_name};")

from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    # Executes every Monday morning at 7:30 a.m.
    'load_quotes_daily': {
        'task': 'tasks.load_quotes',
        'schedule': crontab(hour=5, minute=30, day_of_week=1),
        'args': ("test", "1d"),
    },
}

if __name__ == "__main__":
    load_all_tickers()
    # load_quotes(period="max")
    # load_quotes(period="1d")
    # load_options_and_quotes()
    # import sys
    # from celery.__main__ import main
    # sys.argv=["celery", "-A", "tasks", "worker", "--pool=solo", "--loglevel=info"]
    # main()
