import pytest

from pingslurp.hive_sql import hive_sql_podping


def test_hive_sqlpodping():
    SQLStatement = """
SELECT*

FROM Txcustoms

WHERE
CONVERT(DATE,timestamp) BETWEEN '2022-10-25' AND '2022-10-25'
AND [tid] LIKE '%pp_video%'
    """
    for ans in hive_sql_podping(SQLStatement):
        assert ans
