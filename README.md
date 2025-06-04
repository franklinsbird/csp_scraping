# CSP Scraping Project

I'm compiling data from a number of different sites to help put together an application on college soccer recruiting. The result will be up-to-date data on both men's and women's college universities, coaches, camps, and scholarships.

## Files:
`nsr_inc_scraper.py` - This file scrapes data from:
- https://www.nsr-inc.com/sport/soccer/mens-college-soccer-camps.php (mens)
- https://www.nsr-inc.com/sport/soccer/womens-college-soccer-camps.php (womens)

These sites generally link to other university-specific sites that are both extensive and highly variable. This has been the most challenging part of scraping data. Other sites are generally ones like Nike or IMG that are predictable in how they structure camps on their site and limited in how many they offer.

`ncsasports_scraper.py` - This is a new file (not completed yet) that will be used to scrape data from:
- https://www.ncsasports.org/mens-soccer/tournaments-camps-showcases (mens)
- https://www.ncsasports.org/womens-soccer/tournaments-camps (womens)

This project relies on a few environment variables before running the scraper:

* `GOOGLE_API_KEY` – Google Maps API key used for geocoding requests.
* `OPENROUTER_API_KEY` – API key for OpenRouter used to extract structured data from pages.
* `SHEET_ID` – ID of the Google Sheet where results are written.

Ensure these variables are set in your environment so `camp_scraper.py` can access them. Authentication uses a service account key stored in `gcreds.json`. This file should be present locally but must **not** be committed to version control.
