import pytest

from podping_hive.database import setup_mongo_db


def test_setup_mongo_db():
    setup_mongo_db()
    assert True