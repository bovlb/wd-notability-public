from wd_notability.wikidata_api import wikidata_session


class EntityDeletedError(Exception):
    def __init__(self, qid: str) -> None:
        super().__init__(f"Entity {qid} is deleted")
        self.qid = qid
