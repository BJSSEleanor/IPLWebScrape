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
browser.get(game.get_attribute("href"))

#obtain game information
batting = browser.find_elements(By.CSS_SELECTOR, "table[class='ds-w-full ds-table ds-table-xs ds-table-fixed ci-scorecard-table']")
#gain columns - only needs to be done once
columns_list = []
batting_and_blank = batting[0].find_elements(By.CSS_SELECTOR, "th[class='ds-min-w-max ds-font-bold ds-w-[25%]']")
for col in batting_and_blank: 
    columns_list.append(col.text)
r_b_m_4_6s = batting[0].find_elements(By.CSS_SELECTOR, "th[class='ds-min-w-max ds-font-bold ds-text-right ds-w-[8%]']")
for col in r_b_m_4_6s: 
    columns_list.append(col.text)
sr = batting[0].find_element(By.CSS_SELECTOR, "th[class='ds-min-w-max ds-font-bold ds-text-right ds-w-[10%]']")
columns_list.append(sr.text)
print(columns_list)

#rows - actual data
rows_list = []
team1_rows = batting[0].find_elements(By.CSS_SELECTOR, "tr[class='ds-border-b ds-border-line ds-text-tight-s']")
team2_rows = batting[1].find_elements(By.CSS_SELECTOR, "tr[class='ds-border-b ds-border-line ds-text-tight-s']")
#browser.quit()
eg_row = team1_rows[1].text.split("\n")
row = eg_row[0:2] + eg_row[2].split()
print(row)