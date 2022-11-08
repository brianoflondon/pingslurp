from enum import Enum, auto


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
    