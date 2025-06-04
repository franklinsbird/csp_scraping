import os
import requests
import pandas as pd
from lxml import html
from bs4 import BeautifulSoup
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from geocode_utils import get_lat_long

URL = "https://exactsports.com/soccer/#tve-jump-1896baece7f"
XPATH = "/html/body/div[2]/div[1]/div/div/div[10]/div[2]/div[2]/div[2]/div/div/div[2]/table"

SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    raise EnvironmentError("SHEET_ID environment variable not set")
TAB_NAME = "Camps"
CREDS_FILE = "gcreds.json"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def scrape_exact_sports():
    res = requests.get(URL, headers=HEADERS)
    res.raise_for_status()
    tree = html.fromstring(res.content)
    table = tree.xpath(XPATH)
    if not table:
        raise ValueError("Could not locate camps table")
    rows = table[0].xpath(".//tr")[1:]

    records = []
    for row in rows:
        cells = row.xpath("./td")
        if len(cells) < 5:
            continue
        gender = cells[0].text_content().strip()
        state = cells[1].text_content().strip()
        camp_name = cells[2].text_content().strip()
        date = cells[3].text_content().strip()
        link = cells[4].xpath(".//a/@href")
        camp_url = link[0] if link else ""

        start_date = end_date = date
        city = ""
        lat = ""
        lng = ""
        ages = ""
        cost = ""
        address = ""

        if camp_url:
            try:
                camp_resp = requests.get(camp_url, headers=HEADERS, timeout=10)
                camp_resp.raise_for_status()
                camp_tree = html.fromstring(camp_resp.content)

                soup = BeautifulSoup(camp_resp.text, "html.parser")

                text = soup.get_text("\n")

                def find_pattern(patterns):
                    for pat in patterns:
                        m = re.search(pat, text, re.I)
                        if m:
                            return m.group(1).strip()
                    return ""

                ages = find_pattern([r"Ages?[:\-\s]*([\w\- ]+)", r"Age Range[:\-\s]*([\w\- ]+)"])
                cost = find_pattern([r"(?:Cost|Price|Fee)[:\-\s]*([$\d.,]+)"])
                address = find_pattern([r"Address[:\-\s]*([^\n]+)", r"Location[:\-\s]*([^\n]+)"])

                loc_el = camp_tree.xpath("//div[contains(@class,'location')]//text()")
                if loc_el and not address:
                    address = loc_el[0].strip()
            except Exception:
                pass

        geocode_query = address if address else f"{camp_name}, {state}"
        lat, lng, geo_city = get_lat_long(geocode_query)
        if not city:
            city = geo_city

        records.append({
            "Camp Name": camp_name,
            "Camp Organizer": "ExactSports",
            "Camp Type": "collaborative",
            "Image": "",
            "URL": camp_url,
            "Lat": lat,
            "Long": lng,
            "Start_date": start_date,
            "End_date": end_date,
            "City": city,
            "State": state,
            "Grade Level": "",
            "Ages": ages,
            "Address": address,
            "Division": "",
            "Cost": cost,
            "Gender": gender,
        })

    if records:
        df = pd.DataFrame(records)
        df = df.replace({pd.NA: ""}).fillna("")
        sheet.append_rows(df.values.tolist())


if __name__ == "__main__":
    scrape_exact_sports()
