import json
from datetime import datetime
from enum import Enum, auto
from typing import Any, List, Literal

from pydantic import BaseModel, validator


class PodpingMediums(str, Enum):
    mixed = "mixed"
    podcast = "podcast"
    music = "music"
    video = "video"
    film = "film"
    audiobook = "audiobook"
    newsletter = "newsletter"
    blog = "blog"
    podcastL = "podcastL"
    musicL = "musicL"
    videoL = "videoL"
    filmL = "filmL"
    audiobookL = "audiobookL"
    newsletterL = "newsletterL"
    blogL = "blogL"


class PodpingReasons(str, Enum):
    update = "update"
    live = "live"
    liveEnd = "liveEnd"
    newIRI = "newIRI"


class PodpingVersions(str, Enum):
    v1_0 = "1.0"
    v1_1 = "1.1"


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
    op_id: int = 1  # Counter for multiple operations in one trx_id

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

    version: PodpingVersions = PodpingVersions.v1_0
    medium: PodpingMediums = None
    reason: PodpingReasons = None
    iris: List[str] = []
    timestampNs: datetime = None
    sessionId: str = None

    def __init__(__pydantic_self__, **data: Any) -> None:
        custom_json = json.loads(data.get("json"))
        podping_meta = data
        hive_trx = {}
        podping_meta["json_size"] = utf8len(data.get("json"))
        if iris := custom_json.get("iris"):
            podping_meta["num_iris"] = len(iris)
        super().__init__(**custom_json, **hive_trx, **podping_meta)

    @validator("iris")
    def iris_at_least_one_element(cls, v):
        """Make sure the list contains at least one element"""
        if len(v) == 0:
            raise ValueError("iris must contain at least one element")

        return v

    def db_format(self) -> dict:
        ans = self.dict(exclude_unset=True)
        ans["op_id"] = self.op_id
        return ans

    def db_format_meta(self) -> dict:
        db_meta = self.metadata
        db_meta["timestamp"] = self.timestamp
        db_meta["trx_id"] = self.trx_id
        db_meta["op_id"] = self.op_id
        db_meta["block_num"] = self.block_num
        return db_meta
