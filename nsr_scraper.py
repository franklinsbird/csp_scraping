# camp_scraper.py

import requests
from lxml import html
import pandas as pd
import gspread
import os
from urllib.parse import urlparse, urljoin
import time
from datetime import datetime
from geocode_utils import get_lat_long
import re
from bs4 import BeautifulSoup
import json
import copy
from difflib import SequenceMatcher

# CONFIG
URLS = [
    "https://www.nsr-inc.com/sport/soccer/womens-college-soccer-camps.php",
    "https://www.nsr-inc.com/sport/soccer/mens-college-soccer-camps.php"
]
XPATH = "/html/body/section/div/div/div/div[1]/table"
SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    raise EnvironmentError("SHEET_ID environment variable not set")
TAB_NAME = "Camps"
CREDS_FILE = "cspscraping.json"

# Load sheet
# Old oauth2client usage replaced with gspread.service_account which uses google-auth under the hood.
# This also checks for the presence of the credentials file and raises a clear error if it's missing.
if not os.path.exists(CREDS_FILE):
    raise FileNotFoundError(f"Google credentials file '{CREDS_FILE}' not found. Place your service account JSON there or set CREDS_FILE variable to its path.")

client = gspread.service_account(filename=CREDS_FILE)
sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)

# Load GMaps Config (used via geocode_utils)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

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
            lat, lng, city = get_lat_long(camp["Organiser"])
            camp["Lat"], camp["Long"], camp["City"] = lat, lng, city

            # 3. Look for dates, ages, and prices using the LLM (OpenRouter)
            addl_camps = get_llm_data(res, camp)

        else:
            camp["Page Load?"] = "No Link"
            camp["Lat"] = camp["Long"] = camp["start_date"] = camp["end_date"] = ""
    except Exception as e:
        camp["Page Load?"] = f"Error: {e}"
        camp["Lat"] = camp["Long"] = camp["start_date"] = camp["end_date"] = ""
    time.sleep(1)  # Avoid hammering servers

    return addl_camps


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
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
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



def token_jaccard(a: str, b: str) -> float:
    a_tokens = set(re.findall(r"\w+", a.lower()))
    b_tokens = set(re.findall(r"\w+", b.lower()))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / len(a_tokens | b_tokens)

def scrape_table_to_json(urls=URLS, xpath=XPATH, output_json='camp_url_school_map.json'):
    """Scrape NSR camp tables and write a JSON array where each element has:
    - camp_school_name (anchor text minus trailing 'Camp')
    - camp_url (resolved href)
    - database_school_name (initially same as camp_school_name)

    Prints the number of camps scraped and the output filename.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; nsr-camp-scraper/1.0)"})
    results = []

    for page_url in urls:
        try:
            resp = session.get(page_url, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"Failed to fetch {page_url}: {e}")
            continue

        try:
            tree = html.fromstring(resp.content)
            table_el = tree.xpath(xpath)
            if not table_el:
                # fallback to any table
                table_el = tree.xpath('//table')
            if not table_el:
                print(f"No table found on {page_url} using xpath {xpath}")
                continue
            table = table_el[0]
            rows = table.xpath('.//tr')
        except Exception as e:
            print(f"Failed to parse table on {page_url}: {e}")
            continue

        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) < 2:
                continue
            a_el = cells[1].xpath('.//a')
            if not a_el:
                # no anchor in the second cell; skip
                continue
            href = a_el[0].get('href')
            if not href:
                continue
            full_link = urljoin(page_url, href)
            anchor_text = a_el[0].text_content().strip()
            camp_school_name = re.sub(r"\s+[Cc]amp$", "", anchor_text).strip()
            if not camp_school_name:
                # fallback to cell text
                cell_text = cells[1].text_content().strip()
                camp_school_name = re.sub(r"\s+[Cc]amp$", "", cell_text).strip()
            entry = {
                "camp_school_name": camp_school_name,
                "camp_url": full_link,
                "database_school_name": camp_school_name
            }
            results.append(entry)

    # write JSON array
    try:
        with open(output_json, 'w', encoding='utf-8') as fh:
            json.dump(results, fh, ensure_ascii=False, indent=2)
        print(f"Scraped {len(results)} camps and wrote to {output_json}")
    except Exception as e:
        print(f"Failed to write JSON to {output_json}: {e}")

    return results


def match_schools_from_sheet(json_path='camp_url_school_map.json', uni_tab='Universities'):
    """Load JSON of camps, match each camp_school_name to the best university_name from sheet, and update
    each JSON object with 'database_school_name' (best match) and 'similarity_score' (0..1 combined score).
    The updated JSON is written back to json_path.
    Returns the list of updated entries.
    """
    # Load camp entries
    try:
        with open(json_path, 'r', encoding='utf-8') as fh:
            camps = json.load(fh)
    except Exception as e:
        print(f"Failed to load {json_path}: {e}")
        return []

    # Load university names from sheet
    uni_names = []
    try:
        uni_ws = client.open_by_key(SHEET_ID).worksheet(uni_tab)
        # Prefer header search for a column containing 'university' or 'school'
        vals = uni_ws.get_all_values()
        if vals and len(vals) > 0:
            header = vals[0]
            # find header index
            uni_idx = None
            for i, h in enumerate(header):
                if h and ('university' in h.lower() or 'school' in h.lower() or 'university_name' in h.lower()):
                    uni_idx = i
                    break
            if uni_idx is not None:
                uni_names = [row[uni_idx].strip() for row in vals[1:] if len(row) > uni_idx and row[uni_idx].strip()]
            else:
                # fallback to second column if available
                if len(header) >= 2:
                    uni_names = [row[1].strip() for row in vals[1:] if len(row) > 1 and row[1].strip()]
        # final fallback: try get_all_records and extract common field
        if not uni_names:
            records = uni_ws.get_all_records()
            if records:
                # try to find a field name that looks like university name
                candidate_key = None
                sample = records[0]
                for k in sample.keys():
                    kn = k.strip().lower().replace(' ', '_')
                    if 'university' in kn or 'school' in kn or 'institution' in kn or 'name' in kn:
                        candidate_key = k
                        break
                if candidate_key:
                    uni_names = [r.get(candidate_key, '').strip() for r in records if r.get(candidate_key, '').strip()]
    except Exception as e:
        print(f"Failed to read Universities tab '{uni_tab}': {e}")
        uni_names = []

    if not uni_names:
        print("No university names found in sheet - aborting matching step.")
        return camps

    # perform matching
    updated = []
    for entry in camps:
        camp_name = (entry.get('camp_school_name') or '').strip()
        best_name = ''
        best_score = 0.0
        if camp_name:
            for uni in uni_names:
                seq = SequenceMatcher(None, camp_name.lower(), uni.lower()).ratio()
                jacc = token_jaccard(camp_name, uni)
                score = 0.4 * seq + 0.6 * jacc
                if score > best_score:
                    best_score = score
                    best_name = uni
        # update entry
        entry['database_school_name'] = best_name if best_name else entry.get('database_school_name', '')
        entry['similarity_score'] = round(best_score, 3)
        updated.append(entry)

    # write back
    try:
        with open(json_path, 'w', encoding='utf-8') as fh:
            json.dump(updated, fh, ensure_ascii=False, indent=2)
        print(f"Updated {len(updated)} camp entries with best university matches and wrote to {json_path}")
    except Exception as e:
        print(f"Failed to write updated JSON to {json_path}: {e}")

    return updated


def print_lowest_similarity(json_path='camp_url_school_map.json', n=30):
    """Print the n camps with the lowest similarity_score for manual resolution.

    Skip any entries where 'manually_confirmed' is set to True (or truthy string values).
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as fh:
            camps = json.load(fh)
    except Exception as e:
        print(f"Failed to load {json_path}: {e}")
        return

    # Filter out manually confirmed entries
    def is_manually_confirmed(entry):
        val = entry.get('manually_confirmed')
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in ('true', 'yes', '1')
        # numeric truthy
        if isinstance(val, (int, float)):
            return bool(val)
        return False

    total = len(camps)
    filtered = [c for c in camps if not is_manually_confirmed(c)]
    skipped = total - len(filtered)

    # ensure similarity_score exists and is numeric
    for c in filtered:
        sc = c.get('similarity_score')
        try:
            c['similarity_score'] = float(sc) if sc is not None and sc != '' else 0.0
        except Exception:
            c['similarity_score'] = 0.0

    camps_sorted = sorted(filtered, key=lambda x: (x.get('similarity_score') is None, x.get('similarity_score', 0.0)))
    to_print = camps_sorted[:n]

    print(f"\nLowest {len(to_print)} similarity scores (skipped {skipped} manually confirmed entries):\n")
    for c in to_print:
        print(f"camp_school_name: {c.get('camp_school_name')!s}")
        print(f"  camp_url: {c.get('camp_url')!s}")
        print(f"  database_school_name: {c.get('database_school_name')!s}")
        print(f"  similarity_score: {c.get('similarity_score')}")
        # indicate if this entry had manually_confirmed set but not truthy (just in case)
        if c.get('manually_confirmed'):
            print(f"  manually_confirmed: {c.get('manually_confirmed')}")
        print("")

# Update __main__ wiring to build enriched mapping after match
def main():
    # NOTE: The original sheet-update workflow that processed the existing sheet rows
    # is intentionally preserved here as a commented block. If you want to restore that
    # behavior, uncomment the block below and remove the build_camp_url_school_map() call.
    '''
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
                            [existing_data.iloc[:index + 1], pd.DataFrame([new_camp]), existing_data.iloc[index + 1:]], # type: ignore
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
    '''
    
    # Scrape NSR school names and camp URL's
    # scrape_table_to_json()

    # Match the scraped camp school names to university names from the sheet
    # match_schools_from_sheet()

    # Print the matches with lowest similarity scores to fix matches
    print_lowest_similarity(n=10)


# Wire into module execution: if run as script, build mapping then run fuzzy-match
if __name__ == "__main__":
    main()
    # mapping may be a dict returned from build function, or main may already have written JSON

