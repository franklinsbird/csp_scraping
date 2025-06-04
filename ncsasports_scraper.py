import requests
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import parser as dateparser
import json
import re
from typing import List, Dict, Any

URL = "https://www.ncsasports.org/mens-soccer/tournaments-camps-showcases"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def to_unix(dt_str: str) -> int:
    """Convert a date string to a unix timestamp."""
    try:
        dt = dateparser.parse(dt_str)
        return int(dt.timestamp())
    except Exception:
        return 0

def extract_division(text: str) -> str:
    """Return division info from text."""
    text = text.lower()
    if "naia" in text:
        return "NAIA"
    for d in ("d1", "d2", "d3", "division i", "division ii", "division iii"):
        if d in text:
            if "d1" in d or "division i" in d:
                return "D1"
            if "d2" in d or "division ii" in d:
                return "D2"
            if "d3" in d or "division iii" in d:
                return "D3"
    return ""

def scrape_events(url: str) -> List[Dict[str, Any]]:
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    events = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        data_list = data if isinstance(data, list) else [data]
        for item in data_list:
            if item.get("@type") not in {"Event", "SportsEvent"}:
                continue
            name = item.get("name", "")
            organizer = item.get("organizer", {}).get("name", "")
            url = item.get("url", "")
            start_date = to_unix(item.get("startDate", ""))
            end_date = to_unix(item.get("endDate", ""))
            location = item.get("location", {})
            address = location.get("address", {}) if isinstance(location, dict) else {}
            lat = location.get("geo", {}).get("latitude") if isinstance(location, dict) else None
            lng = location.get("geo", {}).get("longitude") if isinstance(location, dict) else None

            city = address.get("addressLocality", "")
            state = address.get("addressRegion", "")

            description = item.get("description", "")
            division = extract_division(description)
            offers = item.get("offers", {})
            cost = offers.get("price") if isinstance(offers, dict) else ""
            ages = re.search(r"ages?\s*([0-9\-\+]+)", description, re.I)
            ages = ages.group(1) if ages else ""

            events.append({
                "Event Details": name,
                "Organiser": organizer,
                "Camp Info URL": url,
                "lat": lat,
                "long": lng,
                "start_date": start_date,
                "end_date": end_date,
                "city": city,
                "state": state,
                "Ages / Grade Level": ages,
                "division": division,
                "cost": cost,
                "gender": "Men"  # based on URL
            })
    return events

if __name__ == "__main__":
    data = scrape_events(URL)
    df = pd.DataFrame(data)
    print(df.head())
