# Imports
import logging

# Set up the logger
import logging

from vaultdb import auth

logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info(f"event: {event}!")
    connection = None
    try:
        preferred_role = auth.GetAuthorizedRole(event["token"])
        logger.debug(f"role: {preferred_role}")

        catalog = event["catalog"]
        logger.debug(f"catalog: {catalog}")
        payload = event["payload"]
        logger.debug(f"payload: {payload}")

        return {"result": "Success", "data": catalog}

    except Exception as ex:
        logger.error(ex)
        return {"result": "Error", "message": str(ex)}
    finally:
        if connection:
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
