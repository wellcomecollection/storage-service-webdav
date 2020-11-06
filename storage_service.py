import functools

from aws import get_secret
from elasticsearch import Elasticsearch


BAGS_INDEX = "storage_stage_bags"


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
