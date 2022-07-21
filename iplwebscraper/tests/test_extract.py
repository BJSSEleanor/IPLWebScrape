from iplwebscraper.extract import (
    obtain_batters,
    obtain_batting_results,
    setup,
    obtain_games,
)
import pytest


@pytest.fixture
def browser():
    browser = setup()
    return browser


def test_setup(browser):
    assert browser is not None


def test_obtain_games(browser):
    games = obtain_games(browser)
    assert len(games) > 0


@pytest.fixture
def game(browser):
    games = obtain_games(browser)
    game = games[0]
    return game


def test_obtain_batters(browser, game):
    batters = obtain_batters(browser, game)
    assert batters.shape == (35, 10)


def test_obtain_correct_size():
    batting_results = obtain_batting_results()
    assert len(batting_results) > 0
