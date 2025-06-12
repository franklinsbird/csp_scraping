import gspread
from google.oauth2.service_account import Credentials
import requests
import os

SHEET_ID = os.getenv("SHEET_ID")
TAB_NAME = "Scholarships"

def validate_scholarship_links(sheet_id, tab_name):
    """
    Connects to the specified Google Sheet tab, checks if the links in the "URL" column load successfully,
    and writes the response to a new column called "URL Status" in the far-right column.

    Args:
        sheet_id (str): The ID of the Google Sheet.
        tab_name (str): The name of the tab to connect to.
    """
    # Set up Google Sheets API credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("cspscraping.json", scopes=scope)
    client = gspread.authorize(creds)

    # Open the specified tab
    sheet = client.open_by_key(sheet_id).worksheet(tab_name)

    # Get all records from the sheet
    records = sheet.get_all_records()

    # Prepare a list to update the "URL Status" column
    url_status = []

    # Check each URL in the "URL" column
    for i, record in enumerate(records, start=2):  # Start at row 2 to account for the header
        url = record.get("URL")
        if url:
            try:
                url = str(url)  # Ensure URL is a string
                print("Checking URL:", url)
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    status = "Loaded Successfully"
                    print(f"URL {i} loaded successfully.")
                else:
                    status = f"Returned status code {response.status_code}"
                    print(f"URL {i} returned status code {response.status_code}.")
            except Exception as e:
                status = f"Failed to load. Error: {e}"
        else:
            status = "No URL Provided"

        # Append the status to the list
        url_status.append(status)

    # Find the next empty column
    num_columns = len(sheet.row_values(1))  # Get the number of columns in the header row
    next_column_letter = gspread.utils.rowcol_to_a1(1, num_columns + 1)[0]  # Convert to column letter

    # Update the "URL Status" column in the far-right column
    sheet.update( [[status] for status in url_status], f"{next_column_letter}2:{next_column_letter}{len(url_status) + 1}")
    print("URL Status column updated successfully.")

validate_scholarship_links(SHEET_ID, TAB_NAME)