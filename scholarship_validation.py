import csv
import gspread
from google.oauth2.service_account import Credentials
import requests
import os

SHEET_ID = os.getenv("SHEET_ID")
TAB_NAME = "Scholarships"

def validate_scholarship_links(sheet_id, tab_name):
    """
    Connects to the specified Google Sheet tab, checks if the links in the "URL" column load successfully,
    and writes the response to the "URL Status" column.
    Progress is saved to a CSV file every 100 rows.

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

    # Find the "URL Status" column
    header_row = sheet.row_values(1)
    if "URL Status" in header_row:
        url_status_col_index = header_row.index("URL Status") + 1  # Convert to 1-based index
    else:
        raise ValueError("The 'URL Status' column does not exist in the sheet. Please add it manually.")

    # Prepare a list to update the "URL Status" column
    url_status = []
    csv_file = "url_status_progress.csv"

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
                    status = f"Error {response.status_code}"
                    print(f"URL {i} returned status code {response.status_code}.")
            except Exception as e:
                status = f"Failed to load. Error: {e}"
        else:
            status = "No URL Provided"

        # Append the status to the list
        url_status.append((i, url, status))

        # Write progress to CSV every 100 rows
        if len(url_status) % 100 == 0:
            with open(csv_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Row", "URL", "Status"])
                writer.writerows(url_status)
            print(f"Progress saved to {csv_file} at row {i}.")

    # Write final progress to CSV
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Row", "URL", "Status"])
        writer.writerows(url_status)
    print(f"Final progress saved to {csv_file}.")

    # Update the "URL Status" column in the sheet
    for row, _, status in url_status:
        sheet.update_cell(row, url_status_col_index, status)  # Write each status
    print("URL Status column updated successfully.")

validate_scholarship_links(SHEET_ID, TAB_NAME)