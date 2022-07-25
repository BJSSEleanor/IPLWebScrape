from iplwebscraper.extract import (
    obtain_batters,
    extract,
    setup,
    obtain_games,
)
import pytest


@pytest.fixture
def browser():
    """Fixture for setting up a call to the main page"""
    browser = setup()
    return browser


def test_setup(browser):
    """Test the fixture & function Chrome webdriver and main IPL call page doesn't fail"""
    assert browser is not None


def test_obtain_games_game_info_found(browser):
    """Test that at least something is obtained when calling obtain_games"""
    games = obtain_games(browser)
    assert len(games) >0


def test_obtain_games_first_input_as_expected(browser):
    """Test that the first game obtained matches the first expected href"""
    games = obtain_games(browser)
    first_game_href = "/series/indian-premier-league-2022-1298423/gujarat-titans-vs-rajasthan-royals-final-1312200/full-scorecard" class=""><div class="ds-text-compact-xxs"><div class=""><div class="ds-flex ds-justify-between"><div class="ds-truncate ds-w-[90%]"><span class="ds-text-tight-xs ds-font-bold ds-uppercase ds-leading-5">RESULT</span></div></div><div class="ds-text-tight-xs ds-truncate ds-text-ui-typo-mid">Final (N), Ahmedabad, May 29, 2022, <span class="ds-inline-flex ds-items-center ds-leading-none !ds-inline"><a href="/series/indian-premier-league-2022-1298423" target="_blank" rel="noopener noreferrer" class="!ds-inline !ds-underline-offset-1 ds-text-ui-typo ds-underline ds-underline-offset-4 ds-decoration-ui-stroke hover:ds-text-ui-typo-primary hover:ds-decoration-ui-stroke-primary ds-block"><span class="ds-text-tight-xs">Indian Premier League</span></a></span></div><div class=""><div class="ds-flex ds-flex-col ds-mt-2 ds-mb-2"><div class="ci-team-score ds-flex ds-justify-between ds-items-center ds-text-typo-title ds-opacity-50 ds-mt-1 ds-mb-1"><div class="ds-flex ds-items-center"><img width="20" height="20" alt="Rajasthan Royals Flag" class="ds-mr-2" src="https://img1.hscicdn.com/image/upload/f_auto,t_ds_square_w_160,q_50/lsci/db/PICTURES/CMS/313400/313423.logo.png" style="width: 20px; height: 20px;"><p class="ds-text-tight-m ds-font-bold ds-capitalize">Rajasthan Royals</p></div><div class="ds-text-compact-s ds-text-typo-title"><span class="ds-text-compact-xs ds-mr-0.5"></span><strong class="">130/9</strong></div></div><div class="ci-team-score ds-flex ds-justify-between ds-items-center ds-text-typo-title ds-mt-1 ds-mb-1"><div class="ds-flex ds-items-center"><img width="20" height="20" alt="Gujarat Titans Flag" class="ds-mr-2" src="https://img1.hscicdn.com/image/upload/f_auto,t_ds_square_w_160,q_50/lsci/db/PICTURES/CMS/334700/334707.png" style="width: 20px; height: 20px;"><p class="ds-text-tight-m ds-font-bold ds-capitalize">Gujarat Titans</p></div><div class="ds-text-compact-s ds-text-typo-title"><span class="ds-text-compact-xs ds-mr-0.5">(18.1/20 ov, T:131) </span><strong class="">133/3</strong></div></div></div></div><p class="ds-text-tight-s ds-font-regular ds-truncate ds-text-typo-title"
    assert games[0] == first_game_href


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
