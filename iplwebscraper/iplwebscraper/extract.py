from selenium import webdriver
from selenium.webdriver.common.by import By


def setup():
    browser = webdriver.Chrome()
    url = "https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results"
    browser.get(url)
    return browser


def obtain_columns(batting, columns_list):
    batting_and_blank = batting[0].find_elements(
        By.CSS_SELECTOR, "th[class='ds-min-w-max ds-font-bold ds-w-[25%]']"
    )
    for col in batting_and_blank:
        columns_list.append(col.text)
    r_b_m_4_6s = batting[0].find_elements(
        By.CSS_SELECTOR, "th[class='ds-min-w-max ds-font-bold ds-text-right ds-w-[8%]']"
    )
    for col in r_b_m_4_6s:
        columns_list.append(col.text)
    sr = batting[0].find_element(
        By.CSS_SELECTOR,
        "th[class='ds-min-w-max ds-font-bold ds-text-right ds-w-[10%]']",
    )
    columns_list.append(sr.text)


def obtain_rows(batting, rows_list, team_no):
    team_rows = batting[team_no - 1].find_elements(
        By.CSS_SELECTOR, "tr[class='ds-border-b ds-border-line ds-text-tight-s']"
    )
    for row in team_rows:
        if "not out" in row.text:
            split_text = row.text.split("\n")
            breakdown = [split_text[0]]
            breakdown.append("not out")
            breakdown.extend(split_text[1].split()[2:])
            rows_list.append(breakdown)
        else:
            try:
                breakdown_1 = row.text.split("\n")
                breakdown_2 = breakdown_1[0:2] + breakdown_1[2].split()
                rows_list.append(breakdown_2)
            except:
                continue


def pick_game(browser):
    game = browser.find_element(
        By.CSS_SELECTOR,
        "a[href = '/series/indian-premier-league-2022-1298423/gujarat-titans-vs-rajasthan-royals-final-1312200/full-scorecard']",
    )
    browser.get(game.get_attribute("href"))
    batting = browser.find_elements(
        By.CSS_SELECTOR,
        "table[class='ds-w-full ds-table ds-table-xs ds-table-fixed ci-scorecard-table']",
    )
    return batting


if __name__ == "__main__":
    try:
        browser = setup()

    except:
        print("Driver failure")

    else:
        batting = pick_game(browser)
        columns_list = []
        obtain_columns(batting, columns_list)
        print(columns_list)
        rows_list = []
        obtain_rows(batting, rows_list, 1)
        obtain_rows(batting, rows_list, 2)
        print(rows_list)

        browser.quit()
