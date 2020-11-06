import functools
import json

import boto3
from elasticsearch import Elasticsearch


sts_client = boto3.client("sts")


@functools.lru_cache()
def get_aws_client(resource, *, role_arn):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.client(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_secret(secret_id):
    secretsmanager_client = get_aws_client(
         resource="secretsmanager",
         role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    response = secretsmanager_client.get_secret_value(SecretId=secret_id)

    try:
        # The secret response may be a JSON string of the form
        # {"username": "…", "password": "…", "endpoint": "…"}
        secret = json.loads(response["SecretString"])
    except ValueError:
        secret = response["SecretString"]

    return secret


def create_elastic_client():
    hostname = get_secret("storage_bags_reindex_script/es_hostname")
    username = get_secret("storage_bags_reindex_script/es_username")
    password = get_secret("storage_bags_reindex_script/es_password")

    return Elasticsearch(
        [hostname],
        http_auth=(username, password),
        scheme="https",
        port=9243,
    )
