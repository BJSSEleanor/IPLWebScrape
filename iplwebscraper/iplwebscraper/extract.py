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
    """
    Sets up the Chrome webdriver and calls the main IPL page.
        Parameters:
                None.
        Returns:
                browser (webdriver): A webdriver set on the main IPL page.
    """
    browser = webdriver.Chrome()
    url = "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
    browser.get(url)
    return browser


def obtain_games(browser: webdriver) -> list:
    """
    Obtains the href for each game element on the main IPL page.
        Parameters:
                browser(webdriver): A webdriver set on the main IPL page.
        Returns:
                hrefs (list): A list of all IPL game hrefs.
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
    """
    Obtains the specific game page and converts into a single dataframe.
        Parameters:
                browser(webdriver): A Chrome webdriver.
                game (str): A string href for the specific game page.
        Returns:
                batting (Dataframe): A dataframe with all the batter data from that game.
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


def extract():
    """
    Obtains the game pages and creates a singular dataframe which is saved to a csv.
        Parameters:
                None.
        Returns:
                None.
    """
    browser = setup()
    batting_results = pd.DataFrame()
    games = obtain_games(browser)
    for game in games:
        batters = obtain_batters(browser, game)
        if batting_results.empty is True:
            batting_results = batters
        else:
            batting_results = pd.concat([batting_results, batters], ignore_index=True)
    browser.quit()
    batting_results.to_csv("./data/extracted_data.csv", index=False)


if __name__ == "__main__":
    extract()
