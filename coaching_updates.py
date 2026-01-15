import os
import pandas as pd
import gspread
import re
from google.oauth2.service_account import Credentials
from thefuzz import process
import requests
import csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def process_coaching_updates(updates_csv, sheet_id, tab_name, gender):
    # Load and filter updates
    updates = pd.read_csv(updates_csv)
    allowed_positions = [
        "Head Coach",
        "Interim Head Coach",
        "Head Coach (second email)",
        "Head Coach, Director of Soccer",
        "Head Coach, Director of Operations",
        "Head Coach, JV Head Coach"
    ]
    updates = updates[updates['Position'].isin(allowed_positions)]
    updates = updates[updates['Change'].notna() & (updates['Change'] != '#') & (updates['Change'] != 'c')]

    # Set column names based on gender
    if gender.lower() == 'men':
        name_col = 'men_coach_name'
        email_col = 'men_coach_email'
    elif gender.lower() == 'women':
        name_col = 'women_coach_name'
        email_col = 'women_coach_email'
    else:
        raise ValueError("Gender must be 'men' or 'women'")

    # Google Sheets setup
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("/Users/fbird/Desktop/Testing/CSP/cspscraping-4e20669fcaf7.json", scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).worksheet(tab_name)
    # Get headers from the second row
    header_row = sheet.row_values(2)
    sheet_data = pd.DataFrame(sheet.get_all_records(head=2))
    sheet_school_list = sheet_data['university_name'].tolist()

    # Hard-coded name matches for universities where fuzzy matching won't work
    hard_coded_matches = {
        "Union Commonwealth University": "Union College - KY",
        "Bryan College - Tennessee": "Bryan (TN)",
        "Warner University": "Warner University (FL)",
        "Wiley College": "Wiley (TX)",
        "Converse College": "Converse University",
        "St. Joseph's College - Brooklyn": "St. Joseph’s University, New York - Brooklyn"
        # Add more pairs as needed: "name_in_changes": "name_in_universities_tab"
    }

    for _, row in updates.iterrows():
        school = row['School']
        # Check hard-coded matches first
        if school in hard_coded_matches:
            matched_school = hard_coded_matches[school]
            print(f"Using hard-coded match for {school}: {matched_school}")
        else:
            # Fuzzy match school name
            best_match = process.extractOne(school, sheet_school_list)
            if best_match and best_match[1] >= 90:
                matched_school = best_match[0]
            else:
                print(f"No good match found for {school}")
                continue
        change = str(row['Change']).lower().replace(' ', '')
        first_name = row.get('First name', '')
        last_name = row.get('Last name', '')
        new_name = f"{first_name} {last_name}".strip()
        new_email = row['Email address']
        # If no email, skip email update
        if not new_email or pd.isna(new_email):
            new_email = None
            print(f"No email found for {school} in changes. Skipping email update.")

        mask = sheet_data['university_name'] == matched_school
        if not mask.any():
            continue
        for idx in sheet_data.index[mask]:
            output_message = f"""Updated row {idx} in Universities tab for school: {matched_school} with coach: {new_name}"""
            row_num = idx + 3  # Account for header in row 2 and data starting in row 3
            # Get column indices (1-based for Google Sheets)
            try:
                email_col_idx = header_row.index(email_col) + 1
            except ValueError:
                print(f"Column '{email_col}' not found in Google Sheet.")
                continue
            try:
                name_col_idx = header_row.index(name_col) + 1
            except ValueError:
                print(f"Column '{name_col}' not found in Google Sheet.")
                name_col_idx = None
            if change == 'e':
                if new_email:
                    output_message = f"Updated row {idx} in Universities tab for coach {new_name} with email: {new_email}"
                    sheet.update_cell(row_num, email_col_idx, new_email)
                    continue
            elif any(x in change for x in ['j', 'x']):
                if name_col_idx:
                    sheet.update_cell(row_num, name_col_idx, new_name)
                if new_email:
                    output_message += f" and email: {new_email}"
                    sheet.update_cell(row_num, email_col_idx, new_email)

            # Get previous coach URL from the sheet
            prev_url_col = 'men_coach_url' if gender == 'men' else 'women_coach_url'
            try:
                prev_url_col_idx = header_row.index(prev_url_col)
                prev_url = sheet_data.iloc[idx][prev_url_col]
            except Exception:
                prev_url = ''
            # Generate new coach URL by searching the roster, staff-directory, or general coaches page
            new_coach_url = ''
            if prev_url and new_name:
                # Try /roster pattern
                if '/roster/coaches/' in prev_url:
                    roster_base = prev_url.split('/roster/coaches/')[0] + '/roster'
                    found_url = find_coach_profile_url_roster(roster_base, new_name)
                    if found_url:
                        output_message += f" and URL: {found_url}"
                        new_coach_url = found_url
                    else:
                        new_coach_url = roster_base + '/coaches/'
                        output_message += f" and URL: {new_coach_url} (coach URL not updated yet)"
                # Try /staff-directory pattern
                elif '/staff-directory/' in prev_url:
                    staff_base = prev_url.split('/staff-directory/')[0] + '/staff-directory'
                    found_url = find_coach_profile_url_staff_directory(staff_base, new_name)
                    if found_url:
                        output_message += f" and URL: {found_url}"
                        new_coach_url = found_url
                # Try general coaches page for patterns like .../sports/wsoc/coaches/First_Last or /sports/womens-soccer/coaches
                else:
                    if '/coaches' in prev_url:
                        staff_base = prev_url.split('/coaches')[0] + '/coaches'
                    else:
                        staff_base = prev_url
                    found_url = find_coach_profile_url_general_coaches(staff_base, new_name)
                    if found_url:
                        output_message += f" and URL: {found_url}"
                        new_coach_url = found_url
            # Update coach URL in Google Sheet
            coach_url_col_idx = header_row.index(prev_url_col) + 1 if prev_url_col in header_row else None
            if new_coach_url and coach_url_col_idx:
                sheet.update_cell(row_num, coach_url_col_idx, new_coach_url)
            print(output_message)

def extract_division_from_filename(filename):
    # Get just the filename, not the full path
    base = os.path.basename(filename)
    # Search for DI, DII, DIII, or JuCO (case-insensitive)
    match = re.search(r'(DI{1,3}|NAIA)', base, re.IGNORECASE)
    if match:
        return match.group(0)
    return None

def preprocess_csv_file(file_path):
    """
    Ensure the CSV has 'Conference' as the first value; if not, delete the first 5 rows.
    Also normalize the 6th column header to 'Change'. Modifies the file in-place.
    """
    try:
        with open(file_path, newline='', encoding='utf-8') as f:
            rows = list(csv.reader(f))
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return

    if not rows:
        return

    # If first value isn't 'Conference', drop the first 5 rows
    if not rows[0] or rows[0][0] != "Conference":
        print("First value is not 'Conference'. Dropping first 5 rows.")
        rows = rows[5:] if len(rows) >= 5 else []

    # Ensure header exists and set 6th column to 'Change'
    if rows:
        header = rows[0]
        if len(header) < 6:
            header += [""] * (6 - len(header))
        header[5] = "Change"
        rows[0] = header

    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
    except Exception as e:
        print(f"Error writing {file_path}: {e}")

def find_coach_profile_url_roster(base_url, coach_name):
    """
    Search the /roster page for the coach's profile URL using the /roster/coaches/ pattern.
    Matches when the coach name appears in the link text, the aria-label, or the href itself
    (normalized forms like first-last, first_last, or firstlast).
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(base_url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")

        # Normalized versions of the coach name for matching in URLs
        coach_name_norm = ' '.join(coach_name.lower().split())  # collapse extra spaces
        coach_name_url = coach_name_norm.replace(' ', '-')
        coach_name_url_alt = coach_name_norm.replace(' ', '_')
        coach_name_nospace = coach_name_norm.replace(' ', '')

        for a in soup.find_all('a', href=True):
            href = a.get('href', '') or ''
            text = (a.get_text() or '').lower()
            aria = (a.get('aria-label') or '').lower()
            href_lower = href.lower()

            # Only consider roster coach links
            if '/roster/coaches/' in href_lower:
                # Match if name appears in the link text or aria-label
                if coach_name_norm in text or coach_name_norm in aria:
                    return urljoin(base_url, href)

                # Match if a normalized form of the coach name appears in the href
                if any(token in href_lower for token in (coach_name_url, coach_name_url_alt, coach_name_nospace)):
                    return urljoin(base_url, href)

        return None
    except Exception as e:
        print(f"Error searching for coach profile on roster page: {e}")
        return None

def find_coach_profile_url_staff_directory(base_url, coach_name):
    """
    Search the /staff-directory page for the coach's profile URL using the /staff-directory/ pattern.
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(base_url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all('a', href=True):
            if coach_name.lower() in a.text.lower() and '/staff-directory/' in a['href']:
                href = a['href']
                return urljoin(base_url, href)
        return None
    except Exception as e:
        print(f"Error searching for coach profile on staff-directory page: {e}")
        return None

def find_coach_profile_url_general_coaches(base_url, coach_name):
    """
    Search a general coaches page for the coach's profile URL.
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(base_url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        coach_name_url = coach_name.lower().replace(' ', '-')
        for a in soup.find_all('a', href=True):
            if coach_name.lower() in a.text.lower() or (coach_name_url in a['href'].lower()):
                href = a['href']
                return urljoin(base_url, href)
        return None
    except Exception as e:
        print(f"Error searching for coach profile on general coaches page: {e}")
        return None

def main():
    import glob
    folder = "/Users/fbird/Desktop/Testing/CSP/coaching_changes/July"  # Path to the folder containing the 8 CSVs
    sheet_id = "1sZPoX0x7zJ0QCgr9G-qpXmeIqugOr9xj5WaPiSl_avU"    # Google Sheet ID
    tab_name = "Universities" 
    
    # Find all CSV files in the folder
    csv_files = glob.glob(os.path.join(folder, '*.csv'))
    print(f"Found {len(csv_files)} update files.")
    for updates_csv in csv_files:

        # Preprocess the CSV to ensure correct header/rows
        preprocess_csv_file(updates_csv)

        filename = os.path.basename(updates_csv).lower()
        if 'women' in filename:
            gender = 'women'
        elif 'men' in filename:
            gender = 'men'
        else:
            print(f"Skipping {updates_csv}: could not determine gender from filename.")
            continue
        print(extract_division_from_filename(updates_csv), ":")
        
        process_coaching_updates(updates_csv, sheet_id, tab_name, gender)

if __name__ == "__main__":
    main()
