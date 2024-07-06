# Imports
import logging
import os
import duckdb

# Set up the logger
import logging

from vaultdb import auth, commitlog_directory

logger = logging.getLogger()


def lambda_handler(event, context):
    logger.info(f"event: {event}!")
    connection = None
    catalog = None
    try:
        preferred_role = auth.GetAuthorizedRole(event["token"])
        logger.debug(f"role: {preferred_role}")

        catalog = event["catalog"]
        logger.debug(f"catalog: {catalog}")
        payload = event["payload"]
        logger.debug(f"payload: {payload}")

        test_db_path = f"{commitlog_directory}/{catalog}.db"
        if os.path.isfile(test_db_path):
            connection = duckdb.connect(f"{commitlog_directory}/{catalog}.db", True, role=preferred_role)

            if payload.trim().upper() == "LOAD_TICKERS":
                from finance.instrument import all_tickers_load

                all_tickers_load.load(connection, catalog)
                return {
                    "result": "Success",
                    "data": {"result": "finance ticker load ran successfully."},
                }
            elif payload.trim().upper() == "LOAD_INSTRUMENTS":
                ticker = event["ticker"]
                logger.debug(f"ticker: {ticker}")
                from finance.instrument import load_instrument

                load_instrument.load(connection, catalog, ticker)
                return {
                    "result": "Success",
                    "data": {"result": "finance ticker load ran successfully."},
                }
            elif payload.trim().upper() == "LOAD_QUOTES":
                ticker = event["ticker"]
                logger.debug(f"ticker: {ticker}")
                period = event["period"] if "period" in event else "1d"
                logger.debug(f"ticker: {ticker}")
                from finance.quotes import load_historical_quotes

                load_historical_quotes.load(connection, catalog, ticker, period)
                return {
                    "result": "Success",
                    "data": {"result": "finance ticker load ran successfully."},
                }
            else:
                return {
                    "result": "Error",
                    "message": f"Invalid Payload {payload.trim().upper()} allowed values are LOAD_TICKERS, LOAD_INSTRUMENTS, LOAD_QUOTES.",
                }

        return {"result": "Error", "message": f"Catalog {catalog} does not exist."}

    except Exception as ex:
        logger.error(ex)
        return {"result": "Error", "message": str(ex)}
    finally:
        if connection:
            connection.execute(f"PUSH DATABASE {catalog};")
            connection.execute(f"TRUNCATE DATABASE {catalog};")
            connection.close()


if __name__ == "__main__":
    # for testing locally you can enter the JWT ID Token here
    event = {}
    event["token"] = ""
    event["RequestType"] = "fetch-catalogues"
    event["database"] = "dev"
    event["catalog"] = "dev"
    event["payload"] = "SELECT * FROM another_T"
    # event['payload'] = "SELECT * FROM vaultdb_configs()"
    # event['payload'] = "SELECT * FROM 's3://dev-data-440955376164/jwks.json'"
    context = {"identity": {"cognito_identity_id": "", "cognito_identity_pool_id": ""}}
    result = lambda_handler(event, context)
    print(result)
