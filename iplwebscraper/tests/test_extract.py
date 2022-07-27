"""
Tests for the extract function.

Fixtures:
    browser(None) -> webdriver
    game(webdriver) -> str

Functions:
    test_setup(webdriver) -> None
    test_obtain_games_game_info_found(webdriver) -> None
    test_obtain_batters(None) -> None
"""
# pylint: disable=redefined-outer-name
import pytest
from src.extract import (
    obtain_batters,
    setup,
    obtain_games
)


@pytest.fixture
def browser():
    """Sets a fixture webdriver call set on the main IPL page.

    Args:
        None.

    Returns:
        webdriver: A Chrome webdriver set on the main IPL page.
    """
    browser = setup()
    return browser


def test_setup(browser):
    """Tests the fixture webdriver get does not fail.

    Args:
        browser(webdriver) -> A Chrome webdriver set on the main IPL page.

    Returns:
        None.
    """
    assert browser is not None


def test_obtain_games_game_info_found(browser):
    """Tests the calls for individual games obtains some hrefs.

    Args:
        browser(webdriver) -> A Chrome webdriver set on the main IPL page.

    Returns:
        None.
    """
    games = obtain_games(browser)
    assert len(games) > 0


@pytest.fixture
def game(browser):
    """Sets a fixture webdriver call set on the first game page.

    Args:
        None.

    Returns:
        webdriver: A Chrome webdriver set on the first game page.
    """
    games = obtain_games(browser)
    game = games[0]
    return game


def test_obtain_batters(browser, game):
    """Tests if a dataframe with the expected shape is obtained from the first href.

    Args:
        browser(webdriver) -> A Chrome webdriver set on the first IPL game.

    Returns:
        None.
    """
    batters = obtain_batters(browser, game)
    assert batters.shape == (35, 10)
