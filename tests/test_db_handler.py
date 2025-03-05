# tests/test_db_handler.py

import pytest
from sqlalchemy import create_engine, text
from utils.db_handler import DBHandler


@pytest.fixture(scope="module")
def db_handler():
    handler = DBHandler()
    return handler


def test_get_all_tables(db_handler):
    tables = db_handler.get_all_tables()
    assert "Observation" in tables


def test_get_table_columns(db_handler):
    columns = db_handler.get_table_columns("Observation")
    assert "survived" in columns
    assert "pclass" in columns


def test_get_values(db_handler):
    values = db_handler.get_values("Observation", ["parch"])
    assert len(values) == 891


def test_get_distinct_values(db_handler):
    values = db_handler.get_distinct_values("Observation", ["parch"])
    assert {"parch": 6} in values
    assert len(values) == 7


def test_get_count_all(db_handler):
    count = db_handler.get_count("Observation")
    assert count == [{"COUNT(*)": 891}]


def test_get_count_column(db_handler):
    count = db_handler.get_count("Observation", "parch")
    assert count == [{"COUNT(parch)": 891}]


def test_get_sum(db_handler):
    total = db_handler.get_sum("Observation", "parch")
    assert total == [{"SUM(parch)": 340}]


def test_get_min(db_handler):
    minimum = db_handler.get_min("Observation", "parch")
    assert minimum == [{"MIN(parch)": 0}]


def test_get_max(db_handler):
    maximum = db_handler.get_max("Observation", "parch")
    assert maximum == [{"MAX(parch)": 6}]


def test_get_mean(db_handler):
    mean = db_handler.get_mean("Observation", "parch")
    assert mean == [{"AVG(parch)": .38159371492704824}]


def test_get_all(db_handler):
    all_data = db_handler.get_all("Class")
    assert len(all_data) == 3
    assert {"class_id": 0, "class": "First"} in all_data
