import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from camp_scraper import get_llm_data

URLS = [
    "https://men.collegesoccerprospects.com/",
    "https://women.collegesoccerprospects.com/",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def scrape_csp_pages(urls: List[str] = URLS) -> List[Dict[str, str]]:
    """Scrape camp data from College Soccer Prospects sites."""
    camps: List[Dict[str, str]] = []
    for url in urls:
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
        except Exception:
            continue
        camp = {
            "Camp Info URL": url,
            "Camp Found?": "",
            "Event Details": "",
            "start_date": "",
            "end_date": "",
            "Ages / Grade Level": "",
            "Cost": "",
        }
        addl = get_llm_data(res, "div.dt-box")
        camps.append(camp)
        if addl:
            camps.extend(addl)
    return camps

if __name__ == "__main__":
    data = scrape_csp_pages()
    for item in data:
        print("CSP Camps Found:", item)
