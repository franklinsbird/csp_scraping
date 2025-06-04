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
import os

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
# GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
gmaps = GoogleMaps(key=GOOGLE_API_KEY)

def fill_columns(camp):

    # 1. Ensure the camp link loads
    try:
        addl_camps = []
        
        if camp["Camp Info URL"]:
            
            # Parse the URL with headers
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            res = requests.get(camp["Camp Info URL"], headers=headers, timeout=10)
            
            # Ensure the page loads
            if res.status_code == 200:
                camp["Page Load?"] = "OK"
            else:
                camp["Page Load?"] = f"Error {res.status_code}"
                print(f"❌ Error {res.status_code} on URL: {camp['Camp Info URL']}")
                return addl_camps
            content = res.text.lower()
            print("Retrieving data for URL", camp["Camp Info URL"])

            # 2. Use Geocoding API to get lat/lng based on organiser name
            # get_lat_long(camp)

            # 3. Look for dates, ages, prices by parsing info to LLaMA
            addl_camps = get_llm_data(res, camp)

        else:
            camp["Page Load?"] = "No Link"
            camp["Lat"] = camp["Long"] = camp["start_date"] = camp["end_date"] = ""
    except Exception as e:
        camp["Page Load?"] = f"Error: {e}"
        camp["Lat"] = camp["Long"] = camp["start_date"] = camp["end_date"] = ""
    time.sleep(1)  # Avoid hammering servers

    return addl_camps

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
    # OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    prompt = f"""
    You are a structured data extractor. From the following text, extract ONLY the values below and return them in strict JSON format. You are looking for
    information about soccer camps, including the event name, start and end dates, ages, and cost. Only extract data if you
    are confident there is a soccer camp occurring in the near future. Do not return data just because you see the word soccer.
    There should be at least the word camp and probably a start date of some kind to represent a valid camp. 
    If you are not confident, return an empty string for all fields. The text may contain various formats of dates. 
    You may encounter data on multiple camps. If this is the case, return several JSON objects in an array. They may often be contained in an HTML table format.
    If you find a start_date but no end_date, assume the end_date is the same as the start_date.

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
    
    Even if the text does not contain all fields, return an empty string for those fields. Do not return any other text or explanation, just the JSON.
    """

    headers_llm = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
       "model": "google/gemma-3n-e4b-it:free",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2
    }

    addl_camps = []

    try:
        llm_resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers_llm, data=json.dumps(payload))
        llm_json = llm_resp.json()
        #print("🔍 LLM JSON:", llm_json)
        if "choices" in llm_json and llm_json["choices"]:
            llm_output = llm_json["choices"][0]["message"]["content"]
        else:
            raise ValueError("No 'choices' in LLM response")
        print("🔍 LLM Output:", llm_output)

        if llm_output.startswith("```"):
            llm_output = llm_output.strip("`").strip()

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
            # If we got a list with at least one item, we can update the camp
            if isinstance(parsed, list) and len(parsed) > 0:
                camp["Camp Found?"] = "Yes"
                camp.update({
                    "Event Details": parsed[0].get("event_name", camp.get("Event Details")),
                    "start_date": parsed[0].get("start_date", ""),
                    "end_date": parsed[0].get("end_date", ""),
                    "Ages / Grade Level": parsed[0].get("ages", ""),
                    "Cost": parsed[0].get("cost", "")
                })
                # Update additional camps with valid data
                for camp_obj in parsed[1:]:
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
                camp["Camp Found?"] = "No"
                print("⚠️ No camps found in LLM output")
                return
        except json.JSONDecodeError as e:
            print("❌ JSON decode error:", e)
    except Exception as e:
        print("⚠️ LLM Parsing Error:", e)
        camp["start_date"] = camp["end_date"] = camp["Ages / Grade Level"] = camp["Cost"] = "LLM Error"
    
    return addl_camps

# Main execution
def setup():
    data = []
    for url in URLS:
        camp_limit = None
        gender = "Women" if "womens" in url else "Men"
        res = requests.get(url)
        tree = html.fromstring(res.content)
        table = tree.xpath(XPATH)[0]
        rows = table.xpath(".//tr")
        num_camps_filled = 0
        for i, row in enumerate(rows):
            if camp_limit and num_camps_filled >= camp_limit:
                break
            cells = row.xpath(".//td")
            if len(cells) == 2:
                state = cells[0].text_content().strip()
                camp_el = cells[1].xpath(".//a")
                camp_host = cells[1].text_content().strip()
                if camp_host.endswith("Camp"):
                    camp_host = camp_host[:-len("Camp")].strip()
                camp_link = camp_el[0].get("href") if camp_el else ""
                camp = {
                    "ID": "",
                    "Camp Found?": "",
                    "Event Details": "",
                    "Organiser": camp_host,
                    "Camp Type": "",
                    "Image": "",
                    "Camp Info URL": camp_link,
                    "Page Load?": "",
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
                addl_camps = []
                addl_camps = fill_columns(camp)
                if camp["Page Load?"] != "OK":
                    print(f"❌ Skipping camp due to page load error")
                    continue
                data.append(camp)
                num_camps_filled += 1
                if addl_camps:
                    num_camps_filled += len(addl_camps)
                    data.extend(addl_camps)

# Make dataframe from data list
# df = pd.DataFrame(data)
# df = df.replace({pd.NA: "", float("inf"): "", float("-inf"): "", float("nan"): ""})
# df = df.fillna("")
# sheet.update([df.columns.tolist()] + df.values.tolist())

# Load existing sheet data into a DataFrame
existing_data = pd.DataFrame(sheet.get_all_records()).head(6)

# Array to store rows with page load errors
error_rows = []

# Iterate through rows and update relevant columns
for index, row in existing_data.iterrows():
    camp = {
        "Organiser": row["Organiser"],
        "Camp Info URL": row["Camp Info URL"],
        "Page Load?": row["Page Load?"],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
        "Ages / Grade Level": row["Ages / Grade Level"],
        "Cost": row["Cost"]
    }
    addl_camps = fill_columns(camp)
    if camp["Page Load?"] == "OK":
        # Update the DataFrame with new values
        existing_data.at[index, "start_date"] = camp["start_date"]
        existing_data.at[index, "end_date"] = camp["end_date"]
        existing_data.at[index, "Ages / Grade Level"] = camp["Ages / Grade Level"]
        existing_data.at[index, "Cost"] = camp["Cost"]

        # Check for duplicates before adding new camps
        existing_camp_keys = set(
            (row["Organiser"], row["Camp Info URL"], row["Event Details"], row["start_date"]) for _, row in existing_data.iterrows()
        )

        # Add additional camps as new rows directly below the current row
        if addl_camps:
            for new_camp in addl_camps:
                new_camp_key = (new_camp["Organiser"], new_camp["Camp Info URL"], new_camp["Event Details"], new_camp["start_date"])
                if new_camp_key not in existing_camp_keys:
                    # Copy data from the existing row
                    new_camp.update({
                        "Lat": camp.get("Lat", ""),
                        "Long": camp.get("Long", ""),
                        "City": camp.get("City", ""),
                        "State": camp.get("State", ""),
                        "Gender": camp.get("Gender", "")
                    })
                    # Insert new rows directly below the current row
                    existing_data = pd.concat(
                        [existing_data.iloc[:index + 1], pd.DataFrame([new_camp]), existing_data.iloc[index + 1:]],
                        ignore_index=True
                    )
                    existing_camp_keys.add(new_camp_key)
    else:
        # Add row to error_rows and mark for removal
        error_rows.append(row)
        existing_data.drop(index, inplace=True)

# Write back updated rows to the original sheet
existing_data = existing_data.replace({pd.NA: "", float("inf"): "", float("nan"): ""})
existing_data = existing_data.fillna("")
sheet.update([existing_data.columns.tolist()] + existing_data.values.tolist())

# Write error rows to a new sheet tab
if error_rows:
    try:
        error_sheet = client.open_by_key(SHEET_ID).worksheet("Page Load Errors")
    except gspread.exceptions.WorksheetNotFound:
        print("⚠️ Worksheet 'Page Load Errors' not found. Creating it...")
        error_sheet = client.open_by_key(SHEET_ID).add_worksheet(title="Page Load Errors", rows=100, cols=20)

    error_df = pd.DataFrame(error_rows)
    error_sheet.update([error_df.columns.tolist()] + error_df.values.tolist())