"""
Constructs a batter dataframe from multiple sites. Saves the dataframe to a csv in the data folder.

Functions:
    setup(None) -> webdriver
    obtain_games(webdriver) -> list
    obtain_batters(webdriver, str) -> Dataframe
    extract(None) -> Dataframe

"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
import pandas as pd


def setup() -> webdriver:
    """Sets up the Chrome webdriver and calls the main IPL page.

    Args:
        None.

    Returns:
        webdriver: A webdriver set on the main IPL page.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    url = "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
    browser.get(url)
    return browser


def obtain_games(browser: webdriver) -> list:
    """Obtains the href for each game element on the main IPL page.

    Args:
        browser(webdriver): A webdriver set on the main IPL page.

    Returns:
        list[str]: A list of all IPL game hrefs.
    """
    games: list = browser.find_elements(
        By.CSS_SELECTOR,
        "div[class='ds-px-4 ds-py-3']",
    )
    hrefs: list[str] = [game.find_element(By.CSS_SELECTOR, "*").get_attribute("href") for game in games]
    return hrefs


def obtain_batters(browser: webdriver, game: str) -> pd.DataFrame:
    """Obtains the specific game page and converts into a single dataframe.

    Args:
        browser(webdriver): A Chrome webdriver.
        game (str): A string href for the specific game page.

    Returns:
        Dataframe: A dataframe with all the batter data from that game. Empty if href cannot be called.
    """
    tries = 3
    while tries > 0:
        try:
            browser.get(game)
            batting = pd.concat(
            [pd.read_html(game)[0], pd.read_html(game)[2]], ignore_index=True
            )
            tries = 0
        except WebDriverException:
            tries -= 1
            if tries <= 0:
                batting = pd.DataFrame()    
    return batting


def obtain_batting_results(browser: webdriver, games: list[str]) -> pd.DataFrame:
    """Loops through game pages and combines the data into a singular dataframe.

    Args:
        browser (webdriver): A Chrome webdriver.
        games (list): A list of game hrefs.

    Returns:
        Dataframe: A dataframe of batting results.
    """
    batting_results = pd.DataFrame()
    for game in games:
        batters = obtain_batters(browser, game)
        if batting_results.empty is True:
            batting_results = batters
        else:
            batting_results = pd.concat([batting_results, batters], ignore_index=True)
    return batting_results


def extract() -> pd.DataFrame:
    """Obtains the game pages and creates a singular dataframe.

    Args:
        None.

    Returns:
        Dataframe: A dataframe of batting results.
    """
    browser = setup()
    games = obtain_games(browser)
    batting_results = obtain_batting_results(browser, games)
    browser.quit()
    return batting_results
