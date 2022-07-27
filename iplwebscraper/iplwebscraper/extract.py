"""
Constructs a batter dataframe from multiple sites. Saves the dataframe to a csv in the data folder.

Functions:
    setup(None) -> webdriver
    obtain_games(webdriver) -> list
    obtain_batters(webdriver, str) -> Dataframe
    extract(None) -> None

"""
from selenium import webdriver
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
    browser = webdriver.Chrome()
    url = "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
    browser.get(url)
    return browser


def obtain_games(browser: webdriver) -> list:
    """Obtains the href for each game element on the main IPL page.

    Args:
        browser(webdriver): A webdriver set on the main IPL page.

    Returns:
        list: A list of all IPL game hrefs.
    """
    games = browser.find_elements(
        By.CSS_SELECTOR,
        "div[class='ds-px-4 ds-py-3']",
    )
    hrefs = []
    for game in games:
        href = (game.find_element(By.CSS_SELECTOR, "*")).get_attribute("href")
        hrefs.append(href)
    return hrefs


def obtain_batters(browser: webdriver, game: str) -> pd.DataFrame:
    """Obtains the specific game page and converts into a single dataframe.

    Args:
        browser(webdriver): A Chrome webdriver.
        game (str): A string href for the specific game page.

    Raises:
        WebDriverException: webdriver fails to get the game href.

    Returns:
        Dataframe: A dataframe with all the batter data from that game.
    """
    failed_get = True
    while failed_get:
        try:
            browser.get(game)
            failed_get = False
        except WebDriverException:
            failed_get = True
    batting = pd.concat(
        [pd.read_html(game)[0], pd.read_html(game)[2]], ignore_index=True
    )
    return batting


def obtain_batting_results(browser: webdriver, games: list) -> pd.Dataframe:
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


def extract():
    """Obtains the game pages and creates a singular dataframe which is saved to a csv.

    Args:
        None.

    Returns:
        None.
    """
    browser = setup()
    games = obtain_games(browser)
    batting_results = obtain_batting_results(browser, games)
    browser.quit()
    batting_results.to_csv("./data/extracted_data.csv", index=False)


if __name__ == "__main__":
    extract()
