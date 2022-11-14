import pytest

from pingslurp.database import setup_mongo_db
from pingslurp.podping_schemas import PodpingIri


def test_setup_mongo_db():
    setup_mongo_db()
    assert True

def test_podping_iri_host():
    iri = PodpingIri('https://unknonwn.url.com/')
    assert iri.host == "Other"
    iri = PodpingIri('https://feeds.buzzsprout.com/1878047.rss')
    assert iri.host == "Buzzsprout"
    iri = PodpingIri('https://3speak.tv/rss')
    assert iri.host == "3speak"
    iri = PodpingIri('https://example.com/')
    assert iri.host == "example.com"
    iri = PodpingIri('https://example.com/')
    assert iri.host == "example.com"
