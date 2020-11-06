import functools

from aws import get_secret
from elasticsearch import Elasticsearch
from wellcome_storage_service import RequestsOAuthStorageServiceClient


BAGS_INDEX = "storage_stage_bags"

API_URL = "https://api-stage.wellcomecollection.org/storage/v1"


@functools.lru_cache()
def _create_elastic_client():
    """
    Returns an ``Elasticsearch`` instance with read permissions for the bags index
    in the reporting cluster.
    """
    hostname = get_secret("storage_bags_reindex_script/es_hostname")
    username = get_secret("storage_bags_reindex_script/es_username")
    password = get_secret("storage_bags_reindex_script/es_password")

    return Elasticsearch(
        [hostname], http_auth=(username, password), scheme="https", port=9243,
    )


@functools.lru_cache()
def _create_storage_client():
    """
    Returns a client that can talk to the storage service.
    """
    return RequestsOAuthStorageServiceClient.from_path(api_url=API_URL)


def get_list_of_spaces():
    """
    Returns a list of all the spaces in the storage service.
    """
    elastic_client = _create_elastic_client()

    resp = elastic_client.search(
        index=BAGS_INDEX,
        body={"aggs": {"spaces": {"terms": {"field": "space"}}}, "size": 0},
    )
    buckets = resp["aggregations"]["spaces"]["buckets"]
    return [b["key"] for b in buckets]


def get_external_identifiers_in_space(space):
    """
    Returns a list of all the external identifiers in a given space.
    """
    elastic_client = _create_elastic_client()

    # Note: this only returns the first 10000 external identifiers.  A space
    # can have many more -- the digitised space in the prod service has ~260k bags --
    # but it's good enough for an experiment.
    resp = elastic_client.search(
        index=BAGS_INDEX,
        body={
            "query": {"bool": {"must": [{"term": {"space": {"value": space}}}]}},
            "_source": "info.externalIdentifier",
            "size": 10000,
        },
    )

    return [h["_source"]["info"]["externalIdentifier"] for h in resp["hits"]["hits"]]


def get_latest_version_of_bag(space, externalIdentifier):
    """
    Returns the latest version of a bag in the storage service.
    """
    elastic_client = _create_elastic_client()

    resp = elastic_client.get(
        index="storage_stage_bags",
        id=f"{space}/{externalIdentifier}",
        params={"_source": "version"},
    )
    return resp["_source"]["version"]


@functools.lru_cache()
def get_bag(space, externalIdentifier, version):
    """
    Retrieves a bag from the storage service.
    """
    storage_client = _create_storage_client()
    return storage_client.get_bag(space, externalIdentifier, version)
