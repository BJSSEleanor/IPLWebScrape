from iplwebscraper import __version__
from iplwebscraper.extract import setup, obtain_columns, pick_game, obtain_rows
import pytest


def test_version():
    assert __version__ == "0.1.0"


@pytest.fixture
def browser():
    browser = setup()
    return browser


def test_setup(browser):
    assert browser is not None


@pytest.fixture
def batting(browser):
    batting = pick_game(browser)
    return batting


def test_obtain_columns(batting):
    expected_columns = ["BATTING", " ", "R", "B", "M", "4s", "6s", "SR"]
    actual_columns = []
    obtain_columns(batting, actual_columns)
    assert actual_columns == expected_columns


def test_obtain_rows_correct_columns_per_record(batting):
    actual_rows = []
    obtain_rows(batting, actual_rows, 1)
    flag = True
    for row in actual_rows:
        if len(row) != 8:
            flag = False
    assert flag is True


def test_obtain_rows_correct_no_records(batting):
    actual_rows = []
    obtain_rows(batting, actual_rows, 1)
    assert len(actual_rows) == 10
