# CSP Scraping Project

I'm compiling data from a number of different sites to help put together an application on college soccer recruiting. The result will be up-to-date data on both men's and women's college universities, coaches, camps, and scholarships.

This project relies on a couple of API keys that must be provided via environment variables before running the scraper:

* `GOOGLE_API_KEY` – Google Maps API key used for geocoding requests.
* `OPENROUTER_API_KEY` – API key for OpenRouter used to extract structured data from pages.

Ensure these variables are set in your environment so `camp_scraper.py` can access them.
