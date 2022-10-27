from datetime import datetime
import json
from typing import Any, List, Literal

from pydantic import BaseModel, validator

mediums = {
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
    num_iris: int
    id: str


class Podping(HiveTrx, PodpingMeta, BaseModel):
    """Dataclass for on-chain podping schema"""

    version: Literal["1.0"] = "1.0"
    medium: str
    reason: str
    iris: List[str]

    def __init__(__pydantic_self__, **data: Any) -> None:
        custom_json = json.loads(data["op"][1]["json"])
        hive_trx = data
        podping_meta = data["op"][1]
        podping_meta["json_size"] = utf8len(data["op"][1]["json"])
        podping_meta["num_iris"] = len(custom_json["iris"])
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
