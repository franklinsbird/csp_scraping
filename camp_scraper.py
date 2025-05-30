# simple_nsr_scraper.py

import requests
from lxml import html
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse
import time
from datetime import datetime
from googlemaps import Client as GoogleMaps
import re
from bs4 import BeautifulSoup
import json
import copy

# CONFIG
URLS = [
    "https://www.nsr-inc.com/sport/soccer/womens-college-soccer-camps.php",
    "https://www.nsr-inc.com/sport/soccer/mens-college-soccer-camps.php"
]
XPATH = "/html/body/section/div/div/div/div[1]/table"
SHEET_ID = "1sZPoX0x7zJ0QCgr9G-qpXmeIqugOr9xj5WaPiSl_avU"
TAB_NAME = "Camps"
CREDS_FILE = "gcreds.json"

# Load sheet
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)

# Load GMaps Config
GOOGLE_API_KEY = "AIzaSyBOUjqc42Rd38abVDRzYdbUrlxhJo_9SyI"  # Replace with your real API key
gmaps = GoogleMaps(key=GOOGLE_API_KEY)

def fill_columns(camp):

    # 1. Ensure the camp link loads
    try:
        if camp["Camp Info URL"]:
            
            # Parse the URL with headers
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            res = requests.get(camp["Camp Info URL"], headers=headers, timeout=10)
            
            # Ensure the page loads
            if res.status_code == 200:
                camp["Page Load"] = "OK"
            else:
                camp["Page Load"] = f"Error {res.status_code}"
                print(f"❌ Error {res.status_code} on URL: {camp['Camp Info URL']}")
            content = res.text.lower()
            print("Retrieving data for URL", camp["Camp Info URL"])

            # 2. Use Geocoding API to get lat/lng based on organiser name
            get_lat_long(camp)

            # 3. Look for dates, ages, prices by parsing info to LLaMA
            get_llm_data(res, camp)

        else:
            camp["Page Load"] = "No Link"
            camp["Lat"] = camp["Long"] = camp["Start Date"] = camp["End Date"] = ""
    except Exception as e:
        camp["Page Load"] = f"Error: {e}"
        camp["Lat"] = camp["Long"] = camp["Start Date"] = camp["End Date"] = ""
    time.sleep(1)  # Avoid hammering servers

    return camp

def get_lat_long(camp):
    try:
        geo = gmaps.geocode(camp["Organiser"].strip())
        if geo:
            loc = geo[0]["geometry"]["location"]
            city = ""
            for component in geo[0]["address_components"]:
                if "locality" in component["types"]:
                    city = component["long_name"]
                    break
            camp["Lat"] = loc["lat"]
            camp["Long"] = loc["lng"]
            camp["City"] = city
        else:
            camp["Lat"] = ""
            camp["Long"] = ""
    except:
        camp["Lat"] = camp["Long"] = "Geocode Error"

def get_llm_data(res, camp):
    soup = BeautifulSoup(res.text, "html.parser")
    text_blocks = soup.find_all(["p", "li", "div"])
    relevant_lines = []

    # Loop through HTML tags
    for tag in text_blocks:
        if tag.text:
            text = tag.text.strip()
            text_lower = text.lower()
            if any(keyword in text_lower for keyword in ["camp", "date", "session", "ages", "$", "–", "to", "through"]) or any(char.isdigit() for char in text_lower):
                relevant_lines.append(text)

    snippet = "\n".join(relevant_lines)[:5000]  # Truncate to safe token length
    # row["LLM_INPUT"] = snippet

    # Call OpenRouter API to extract structured info
    OPENROUTER_API_KEY = "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"
    prompt = f"""
    You are a structured data extractor. From the following text, extract ONLY the values below and return them in strict JSON format. 
    You may encounter data on multiple camps. If this is the case, return several JSON objects in an array. THey may often be contained in an HTML table format.

    Fields:
    - event_name
    - start_date
    - end_date
    - ages
    - cost

    Text:
    \"\"\"
    {snippet}
    \"\"\"

    Return only this format:
    {{"event_name":"", "start_date": "", "end_date": "", "ages": "", "cost": ""}}
    """

    headers_llm = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
       "model": "meta-llama/llama-3.3-8b-instruct:free",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2
    }

    addl_camps = []

    try:
        llm_resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers_llm, data=json.dumps(payload))
        llm_json = llm_resp.json()
        # print("🔍 LLM JSON:", llm_json)
        if "choices" in llm_json and llm_json["choices"]:
            llm_output = llm_json["choices"][0]["message"]["content"]
        else:
            raise ValueError("No 'choices' in LLM response")
        print("🔍 LLM Output:", llm_output)

        if llm_output.startswith("```"):
            llm_output = llm_output.strip("`").strip()

        print("🚨 Raw llm_output:", repr(llm_output))
        json_start = llm_output.find('[')
        if json_start == -1:
            raise ValueError("No '[' found in LLM output")

        json_snippet = llm_output[json_start:]

        try:
            parsed = json.loads(json_snippet)
            print("✅ Parsed LLM JSON")
        except json.JSONDecodeError as e:
            print("❌ Still malformed JSON:", e)
            print("🚨 Partial content:", json_snippet[:500])
        try:
            parsed = json.loads(llm_output)

            if isinstance(parsed, list):
                for j, camp_obj in enumerate(parsed):
                    print("Camp", j, camp_obj.get("event_name"))
                    if j == 0:
                        # Overwrite current camp row with first entry
                        camp.update({
                            "Event Details": camp_obj.get("event_name", camp.get("Event Details")),
                            "start_date": camp_obj.get("start_date", ""),
                            "end_date": camp_obj.get("end_date", ""),
                            "Ages / Grade Level": camp_obj.get("ages", ""),
                            "Cost": camp_obj.get("cost", "")
                        })
                    else:
                        # Clone current camp and update with next event details
                        new_camp = copy.deepcopy(camp)
                        new_camp.update({
                            "Event Details": camp_obj.get("event_name", camp.get("Event Details")),
                            "start_date": camp_obj.get("start_date", ""),
                            "end_date": camp_obj.get("end_date", ""),
                            "Ages / Grade Level": camp_obj.get("ages", ""),
                            "Cost": camp_obj.get("cost", "")
                        })
                        addl_camps.append(new_camp)
            else:
                # If it's a single object (not a list), update current camp
                camp.update({
                    "Event Details": parsed.get("event_name", camp.get("Event Details")),
                    "start_date": parsed.get("start_date", ""),
                    "end_date": parsed.get("end_date", ""),
                    "Ages / Grade Level": parsed.get("ages", ""),
                    "Cost": parsed.get("cost", "")
                })


            json_start = llm_output.find('{')
            json_end = llm_output.find('}', json_start) + 1

            # If there are no brackets, there's no JSON object so make columns empty
            # if json_start == -1:

            parsed = json.loads(llm_output[json_start:json_end])
            note = llm_output[json_end:].strip()
            if note:
              print("📌 LLM Note:", note)
        except json.JSONDecodeError as e:
            print("❌ JSON decode error:", e)
        camp["Event Details"] = parsed.get("event_name", "")
        camp["start_date"] = parsed.get("start_date", "")
        camp["end_date"] = parsed.get("end_date", "")
        camp["Ages / Grade Level"] = parsed.get("ages", "")
        camp["Cost"] = parsed.get("cost", "")
    except Exception as e:
        print("⚠️ LLM Parsing Error:", e)
        camp["start_date"] = camp["end_date"] = camp["Ages / Grade Level"] = camp["Cost"] = "LLM Error"

# Main execution
data = []
for url in URLS:
    gender = "Women" if "womens" in url else "Men"
    res = requests.get(url)
    tree = html.fromstring(res.content)
    table = tree.xpath(XPATH)[0]
    rows = table.xpath(".//tr")
    num_camps_filled = 0
    for row in rows:
        if num_camps_filled <= 5:
            cells = row.xpath(".//td")
            if len(cells) == 2:
                state = cells[0].text_content().strip()
                camp_el = cells[1].xpath(".//a")
                camp_host = cells[1].text_content().strip()
                camp_link = camp_el[0].get("href") if camp_el else ""
                camp = {
                    "ID": "",
                    "Event Details": "",
                    "Organiser": camp_host,
                    "Camp Type": "",
                    "Image": "",
                    "Camp Info URL": camp_link,
                    "Page Load": "",
                    "Lat": "",
                    "Long": "",
                    "start_date": "",
                    "end_date": "",
                    "City": "",
                    "State": state,
                    "Ages / Grade Level": "",
                    "Division":"",
                    "Cost":"",
                    "Gender": gender
                }
                camp_filled = fill_columns(camp)
                num_camps_filled += 1
                data.append(camp_filled)
        else:
            break

# Make dataframe from data list
df = pd.DataFrame(data)
df = df.replace({pd.NA: "", float("inf"): "", float("-inf"): "", float("nan"): ""})
df = df.fillna("")
sheet.update([df.columns.tolist()] + df.values.tolist())

