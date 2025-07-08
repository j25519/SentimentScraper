# SentimentScraper
Custom Python script to aggregate public data from online discussion boards regarding home EV chargers.

Takes an input of a list of URLs and outputs a CSV.

Utilises different logic for generic forum posts and Reddit posts. Converts Reddit links to old.reddit.com URLs for easier scraping.

# Usage
Place the URLs of Reddit threads and/or forum posts in a text file named `urls.txt` (one URL per line).

Run the script:

``
python ev_charger_scraper.py
``

A CSV file named `ev_charger_data.csv` will be created in the same directory.
