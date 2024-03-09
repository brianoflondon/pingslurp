import json
import logging
import re
from datetime import datetime
from enum import Enum
from typing import Any, List

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
    stored_hosts: bool = False
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
            "timestamp": self.timestamp,
        }


known_hosts: dict[str, str] = {
    "Buzzsprout": r".*buzzsprout.com\/.*",
    "RSS.com": r".*rss.com\/.*",
    "Spreaker": r".*spreaker.com\/.*",
    "Transistor": r".*transistor.fm\/.*",
    "Podserve": r".*podserve.fm\/.*",
    "Captivate": r".*captivate.fm\/.*",
    "Acast": r".*rss.acast.com\/.*",
    "redcircle.com": r".*redcircle.com\/.*",
    "Feedlayer": r".*feedlayer.com\/.*",
    "3speak": r".*3speak.tv\/.*",
    "Newsradio1620": r".*newsradio1620.com\/.*",
    "StrivingForEternity": r".*strivingforeternity.org\/.*",
    "Heliumradio": r".*heliumradio.com\/.*",
    "Blubrry": r".*blubrry.com\/.*",
    "example.com": r".*example.com\/.*",
    # "Other": r".*",
}

# resp = httpx.get('https://raw.githubusercontent.com/Podcastindex-org/podping/main/README.md')
# if resp.status_code == 200:
#     st.markdown(resp.text, unsafe_allow_html=True)

# with open("known_hosts.json","r") as f:
#     known_hosts_json = json.load(f)


all_hosts = ""
for host in known_hosts.values():
    all_hosts += f"({host})|"
all_hosts = all_hosts[:-1]


class PodpingIri(str):
    """Model for each IRI in a podping"""

    def __new__(cls, content: str, **data: Any):
        return super().__new__(cls, content)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)}), {self.host})"

    @property
    def host(self) -> str:
        if re.match(all_hosts, self):
            for key, value in known_hosts.items():
                if re.match(value, self):
                    return key
        return "Other"


class Podping(HiveTrx, PodpingMeta, BaseModel):
    """Dataclass for on-chain podping schema"""

    version: PodpingVersions = PodpingVersions.v1_0
    medium: PodpingMediums = None
    reason: PodpingReasons = None
    iris: List[PodpingIri] = []
    timestampNs: datetime = None
    sessionId: str = None

    def __init__(__pydantic_self__, **data: Any) -> None:
        try:
            if data.get("json"):
                podping_meta = data
                custom_json = json.loads(data.get("json"))
                podping_meta["json_size"] = utf8len(data.get("json"))
                if iris := custom_json.get("iris"):
                    podping_meta["num_iris"] = len(iris)
                    custom_json["iris"] = [PodpingIri(iri) for iri in iris]
                super().__init__(**custom_json, **podping_meta)
            elif iris := data.get("iris"):
                data["iris"] = [PodpingIri(iri) for iri in iris]
                super().__init__(**data)
            else:
                super().__init__(**data)

        except Exception as ex:
            logging.exception(ex)

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

    def db_format_hosts_ts(self) -> List[dict]:
        ans = []
        for iri in self.iris:
            db = {
                "metadata": {"host": iri.host, "timestamp": self.timestamp},
                "timestamp": self.timestamp,
                "iri": iri,
                "trx_id": self.trx_id,
            }
            ans.append(db)
        return ans
