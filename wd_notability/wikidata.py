from time import time

from wd_notability.async_limiters import WIKIDATA_ENTITYDATA_LIMITER
from wd_notability.wikidata_api import WIKIDATA_ENTITY_URL, wikidata_session


class EntityDeletedError(Exception):
    def __init__(self, qid: str) -> None:
        super().__init__(f"Entity {qid} is deleted")
        self.qid = qid


async def _fetch_with_retry(url):
    return await wikidata_session.get(url, limiter=WIKIDATA_ENTITYDATA_LIMITER)


async def fetch_item(qid: str) -> dict:
    start_time = time()
    url = WIKIDATA_ENTITY_URL.format(qid=qid)
    resp = await _fetch_with_retry(url)
    end_time = time()
    print(f"Fetched {qid} in {end_time - start_time:.2f} seconds with status code {resp.status_code}")
    if resp.status_code == 404:
        raise EntityDeletedError(qid)
    resp.raise_for_status()
    return resp.json()
