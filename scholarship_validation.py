import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os

SHEET_ID = os.getenv("SHEET_ID")
TAB_NAME = "Scholarships"

def validate_scholarship_links(sheet_id, tab_name):
    """
    Connects to the specified Google Sheet tab and checks if the links in the "URL" column load successfully.

    Args:
        sheet_id (str): The ID of the Google Sheet.
        tab_name (str): The name of the tab to connect to.
    """
    # Set up Google Sheets API credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("cspscraping.json", scope)
    client = gspread.authorize(creds)

    # Open the specified tab
    sheet = client.open_by_key(sheet_id).worksheet(tab_name)

    # Get all records from the sheet
    records = sheet.get_all_records()

    # Check each URL in the "URL" column
    for i, record in enumerate(records, start=2):  # Start at row 2 to account for the header
        url = record.get("URL")
        if url:
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    print(f"Row {i}: URL {url} loaded successfully.")
                else:
                    print(f"Row {i}: URL {url} returned status code {response.status_code}.")
            except Exception as e:
                print(f"Row {i}: URL {url} failed to load. Error: {e}")
        else:
            print(f"Row {i}: No URL provided.")

validate_scholarship_links(SHEET_ID, TAB_NAME)