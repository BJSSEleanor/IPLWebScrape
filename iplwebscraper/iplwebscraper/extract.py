"""Extract.py obtains the batting data from the webpage,
saves it in a dataframe and exports it as a csv"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
import pandas as pd


def setup():
    """Sets up the Chrome webdriver and calls the main IPL page.
    Returns the driver"""
    browser = webdriver.Chrome()
    url = "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
    browser.get(url)
    return browser


def obtain_games(browser):
    """Makes a list of all of the game elements and obtains the href for each one.
    Returns the list of href strings for all IPL games"""
    games = browser.find_elements(
        By.CSS_SELECTOR,
        "div[class='ds-px-4 ds-py-3']",
    )
    hrefs = []
    for game in games:
        href = (game.find_element(By.CSS_SELECTOR, "*")).get_attribute("href")
        hrefs.append(href)
    return hrefs


def obtain_batters(browser, game):
    """Gets the specific game page via the href passed into the function.
    While loop will make sure the game page is loaded correctly.
    Pandas reads the html and the two team batter lists are concatenated in a singular dataframe."""
    failed_get = True
    while failed_get:
        try:
            browser.get(game)
            failed_get = False
        except WebDriverException:
            failed_get = True
    batting = pd.concat([pd.read_html(game)[0], pd.read_html(game)[2]], ignore_index=True)
    return batting


def obtain_batting_results():
    """Calls the setup function. Makes an empty dataframe to hold all of the game information.
    Gets the list of game page hrefs and for each game,
    obtains a dataframe with the two team's batters.
    This dataframe is added to the main dataframe.
    Returns main dataframe."""
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
    return batting_results


if __name__ == "__main__":
    results = obtain_batting_results()
    print(results)
