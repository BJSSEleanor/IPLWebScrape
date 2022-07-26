"""Tests for the extract function. Calls the actual main page and the first game found."""
# pylint: disable=redefined-outer-name
import pytest
from iplwebscraper.extract import (
    obtain_batters,
    setup,
    obtain_games,
)


@pytest.fixture
def browser():
    """Fixture for calling the main page"""
    browser = setup()
    return browser


def test_setup(browser):
    """Test the setup function and check Chrome webdriver and main IPL call page doesn't fail"""
    assert browser is not None


def test_obtain_games_game_info_found(browser):
    """Test that at least something is obtained when calling obtain_games"""
    games = obtain_games(browser)
    assert len(games) > 0


@pytest.fixture
def game(browser):
    """Fixture for obtaining the href for the first game."""
    games = obtain_games(browser)
    game = games[0]
    return game


def test_obtain_batters(browser, game):
    """Tests if a dataframe with the expected shape is obtained from the first href."""
    batters = obtain_batters(browser, game)
    assert batters.shape == (35, 10)
