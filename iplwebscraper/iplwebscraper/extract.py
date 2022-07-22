from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
import pandas as pd


def setup():
    browser = webdriver.Chrome()
    url = "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
    browser.get(url)
    return browser


def obtain_games(browser):
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
