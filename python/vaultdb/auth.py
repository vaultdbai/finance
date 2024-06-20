# Imports
import os
import logging
import json
import glob
import boto3

# Set up the logger
import logging

logger = logging.getLogger()

application_name = (
    os.environ["application_name"] if "application_name" in os.environ else ""
)
commitlog_directory = (
    os.environ["commitlog_directory"] if "commitlog_directory" in os.environ else "/tmp"
)
public_bucket = os.environ["public_bucket"] if "public_bucket" in os.environ else None
data_store = os.environ["data_store"] if "data_store" in os.environ else None


def get_keys():
    from datetime import datetime

    cachepath = (
        f"{commitlog_directory}/jwks{datetime.today().strftime('%Y-%m-%d')}.json"
    )
    if os.path.isfile(cachepath):
        with open(cachepath) as f:
            data = json.load(f)
        local_keys = data["keys"]
        logger.debug(f"keys: {local_keys}")
        return local_keys
    else:
        jwks = glob.glob(f"{commitlog_directory}/jwks*.json")
        for jwk in jwks:
            os.remove(jwk)

    s3 = boto3.resource("s3")
    obj = s3.meta.client.get_object(Bucket=data_store, Key="jwks.json")
    local_keys = json.loads(obj["Body"].read())
    with open(cachepath, "w") as f:
        json.dump(local_keys, f)
    logger.debug(f"keys: {local_keys}")
    return local_keys["keys"]


keys = (
    get_keys()
)  # Download Public Keys for token verification ahead as we need them for security


def verify_token(token, user_pool_client_id):
    import time
    from jose import jwk, jwt
    from jose.utils import base64url_decode

    # get the kid from the headers prior to verification
    headers = jwt.get_unverified_headers(token)
    kid = headers["kid"]
    # search for the kid in the downloaded public keys
    key_index = -1
    valid_keys = keys or get_keys()
    for i in range(len(valid_keys)):
        if kid == valid_keys[i]["kid"]:
            key_index = i
            break
    if key_index == -1:
        raise Exception("Public key not found in jwks.json")
    # construct the public key
    public_key = jwk.construct(keys[key_index])
    # get the last two sections of the token,
    # message and signature (encoded in base64)
    message, encoded_signature = str(token).rsplit(".", 1)
    # decode the signature
    decoded_signature = base64url_decode(encoded_signature.encode("utf-8"))
    # verify the signature
    if not public_key.verify(message.encode("utf8"), decoded_signature):
        raise Exception("Signature verification failed")
    logger.debug("Signature successfully verified")
    # since we passed the verification, we can now safely
    # use the unverified claims
    claims = jwt.get_unverified_claims(token)
    # additionally we can verify the token expiration
    if time.time() > claims["exp"]:
        raise Exception("Token is expired")
    # and the Audience  (use claims['client_id'] if verifying an access token)
    if claims["aud"] != user_pool_client_id:
        raise Exception("Token was not issued for this audience")
    # now we can use the claims
    logger.debug(claims)
    return claims


def GetAuthorizedRole(token: str) -> str:
    user_pool_client_id = (
        os.environ["user_pool_client_id"]
        if "user_pool_client_id" in os.environ
        else None
    )
    if token:
        verified_claims = verify_token(token, user_pool_client_id)
        preferred_role = str(verified_claims["cognito:preferred_role"]).split(":role/")[
            -1
        ]
    else:
        preferred_role = "vaultdb"

    logger.debug(f"preferred_role: {preferred_role}")

    if application_name and preferred_role.startswith(application_name):
        preferred_role = preferred_role[len(application_name) + 1 :]

    if preferred_role.endswith("-AdminRole"):
        preferred_role = preferred_role[:-10]

    logger.debug(f"role: {preferred_role}")

    return preferred_role


def sample_lambda_handler(event, context):
    logger.info(f"event: {event}!")
    connection = None
    try:
        preferred_role = GetAuthorizedRole(event["token"])
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
    result = sample_lambda_handler(event, context)
    print(result)
