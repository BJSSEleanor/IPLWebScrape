from selenium import webdriver
from selenium.webdriver.common.by import By

#to run:
#poetry shell
#poetry install
#poetry run python.exe .\iplwebscraper\extract.py

browser = webdriver.Chrome()
url =  "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
browser.get(url)
game = browser.find_element(By.CSS_SELECTOR, "a[href = '/series/indian-premier-league-2022-1298423/gujarat-titans-vs-rajasthan-royals-final-1312200/full-scorecard']")
browser.get(game)
#batting = browser.find_element(By.CSS_SELECTOR, "table[class='']")
browser.quit()