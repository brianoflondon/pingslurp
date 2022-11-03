import json
from datetime import datetime
from typing import Any, List, Literal

from pydantic import BaseModel, validator

mediums = {
    "mixed",
    "podcast",
    "music",
    "video",
    "film",
    "audiobook",
    "newsletter",
    "blog",
    "podcastL",
    "musicL",
    "videoL",
    "filmL",
    "audiobookL",
    "newsletterL",
    "blogL",
}
reasons = {"update", "live", "liveEnd", "newIRI"}


def utf8len(s):
    return len(s.encode("utf-8"))


class HiveTrx(BaseModel):
    trx_id: str
    block_num: int
    timestamp: datetime
    trx_num: int


class PodpingMeta(BaseModel):
    required_posting_auths: List[str]
    json_size: int
    num_iris: int = 0
    id: str
    live_test: bool = False
    server_account: str = None
    message: str = None
    uuid: str = None
    hive: str = None
    v: str = None
    stored_meta: bool = False

    def __init__(__pydantic_self__, **data: Any) -> None:
        if data.get("id").startswith("pplt_"):
            data["live_test"] = True
        super().__init__(**data)

    @property
    def metadata(self):
        return {
            "metadata": {"posting_auth": self.required_posting_auths[0], "id": self.id},
            "json_size": self.json_size,
            "num_iris": self.num_iris,
        }


class Podping(HiveTrx, PodpingMeta, BaseModel):
    """Dataclass for on-chain podping schema"""

    version: Literal["1.0"] = "1.0"
    medium: str = ""
    reason: str = ""
    iris: List[str] = []

    def __init__(__pydantic_self__, **data: Any) -> None:
        # Lighthive post format parser:
        # try:
        #     custom_json = json.loads(data["op"][1]["json"])
        #     hive_trx = data
        #     podping_meta = data["op"][1]
        #     podping_meta["json_size"] = utf8len(data["op"][1]["json"])
        #     podping_meta["num_iris"] = len(custom_json["iris"])
        # except KeyError:
        #     pass
        custom_json = json.loads(data.get("json"))
        podping_meta = data
        hive_trx = {}
        podping_meta["json_size"] = utf8len(data.get("json"))
        if iris := custom_json.get("iris"):
            podping_meta["num_iris"] = len(iris)
        super().__init__(**custom_json, **hive_trx, **podping_meta)

    @validator("medium")
    def medium_exists(cls, v):
        """Make sure the given medium matches what's available"""
        if v not in mediums:
            raise ValueError(f"medium must be one of {str(', '.join(mediums))}")
        return v

    @validator("reason")
    def reason_exists(cls, v):
        """Make sure the given reason matches what's available"""
        if v not in reasons:
            raise ValueError(f"reason must be one of {str(', '.join(reasons))}")
        return v

    @validator("iris")
    def iris_at_least_one_element(cls, v):
        """Make sure the list contains at least one element"""
        if len(v) == 0:
            raise ValueError("iris must contain at least one element")

        return v

    def db_format(self) -> dict:
        return self.dict(exclude_unset=True)

    def db_format_meta(self) -> dict:
        db_meta = self.metadata
        db_meta["timestamp"] = self.timestamp
        db_meta["trx_id"] = self.trx_id
        db_meta["block_num"] = self.block_num
        return db_meta
