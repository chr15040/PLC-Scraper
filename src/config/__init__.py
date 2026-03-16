import os
import json
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

if os.environ.get("ENV", "local") == "local":
    load_dotenv()


def get_secret():
    secret_name = os.environ["AWS_SECRETS_ID"]
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        return json.loads(get_secret_value_response["SecretString"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print("The requested secret " + secret_name + " was not found")
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            print("The request was invalid due to:", e)
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            print("The request had invalid params:", e)
        elif e.response["Error"]["Code"] == "DecryptionFailure":
            print(
                "The requested secret can't be decrypted using the provided KMS key:", e
            )
        elif e.response["Error"]["Code"] == "InternalServiceError":
            print("An error occurred on service side:", e)


secrets_values = get_secret()

LOG_LEVEL_UVICORN = secrets_values["LOG_LEVEL_UVICORN"]
ISSUER = secrets_values["oktaOAuth2EndPoint"]

NEW_CLIENT_ID_SUPPORTAICHATBOT = secrets_values["NEW_CLIENT_ID_SUPPORTAICHATBOT"]
NEW_AUDIENCE_ID_SUPPORTAICHATBOT = secrets_values["NEW_AUDIENCE_ID_SUPPORTAICHATBOT"]

DEFAULT_CLIENT_ID = secrets_values["DEFAULT_CLIENT_ID"]
DEFAULT_AUDIENCE_ID = secrets_values["DEFAULT_AUDIENCE_ID"]
JWKS = secrets_values["JWKS"]

OPENAI_API_TYPE = secrets_values["OPENAI_API_TYPE"]
OPENAI_API_BASE = secrets_values["OPENAI_API_BASE"]
OPENAI_API_KEY = secrets_values["OPENAI_API_KEY"]
OPENAI_API_VERSION = secrets_values["OPENAI_API_VERSION"]
DEPLOYMENT_NAME = secrets_values["DEPLOYMENT_NAME"]
MODEL_NAME = secrets_values["MODEL_NAME"]

OPENAI_API_TYPE_52 = secrets_values["OPENAI_API_TYPE"]
OPENAI_API_BASE_52 = secrets_values["OPENAI_API_BASE"]
OPENAI_API_VERSION_52 = secrets_values["OPENAI_API_VERSION"]
DEPLOYMENT_NAM_52E = secrets_values["DEPLOYMENT_NAME"]
MODEL_NAME_52 = secrets_values["MODEL_NAME"]

MONGODB_URI = secrets_values["MONGODB_URI"]
STG_MONGODB_URI = secrets_values["STG_MONGODB_URI"]
PRD_MONGODB_URI = secrets_values["PRD_MONGODB_URI"]

EXT_RESOURCES_DB_NAME = secrets_values["EXT_RESOURCES_DB_NAME"]
EXT_RESOURCES_DB_NAME_STG = secrets_values["EXT_RESOURCES_DB_NAME_STG"]
EXT_RESOURCES_DB_NAME_PRD = secrets_values["EXT_RESOURCES_DB_NAME_PRD"]

EXT_RESOURCES_COLLECTION = secrets_values["EXT_RESOURCES_COLLECTION"]
EXT_RESOURCES_INDEX = secrets_values["EXT_RESOURCES_INDEX"]
EXT_CHAT_HISTORY = secrets_values["EXT_CHAT_HISTORY"]

INT_RESOURCES_DB_NAME = secrets_values["INT_RESOURCES_DB_NAME"]
INT_RESOURCES_DB_NAME_STG = secrets_values["INT_RESOURCES_DB_NAME_STG"]
INT_RESOURCES_DB_NAME_PRD = secrets_values["INT_RESOURCES_DB_NAME_PRD"]

INT_RESOURCES_COLLECTION = secrets_values["INT_RESOURCES_COLLECTION"]
INT_RESOURCES_INDEX = secrets_values["INT_RESOURCES_INDEX"]
INT_CHAT_HISTORY = secrets_values["INT_CHAT_HISTORY"]

MONGO_URI_PLC = secrets_values["MONGO_URI_PLC"]
MONGO_DB_NAME_PLC = secrets_values["MONGO_DB_NAME_PLC"]
MONGO_COLL_NAME_PLC = secrets_values["MONGO_COLL_NAME_PLC"]
INDEX_NAME_PLC = secrets_values["INDEX_NAME_PLC"]

MONGO_URI_PLC_STG = secrets_values["MONGO_URI_PLC_STG"]
MONGO_DB_NAME_PLC_STG = secrets_values["MONGO_DB_NAME_PLC_STG"]
MONGO_URI_PLC_PRD = secrets_values["MONGO_URI_PLC_PRD"]
MONGO_DB_NAME_PLC_PRD = secrets_values["MONGO_DB_NAME_PLC_PRD"]
