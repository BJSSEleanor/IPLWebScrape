# IPLWebScrape
We want to build a web scrape that can collect data from the current IPL tournament. We want to collect the following on the Batsmen: Runs, Balls faced, 4's, 6's, Are they out?

The following page lists every match for the current tournament - https://www.espncricinfo.com/series/indian-premier-league-2022-1298423/match-results
From here, you need to find a way to loop through each match and scrape the match page for the above information. You can safely assume that every match page has the same layout.

Once we have the stats, we want to create a ‘DWH’ that we can use to make a dashboard from. This DWH should contain a table: Total of each column scored by each batsman

We can then later make dashboards containing the highest run scorer etc.
All of this should be orchestrated with airflow!
