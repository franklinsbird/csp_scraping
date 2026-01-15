import os, json, base64, re, time
from typing import List, Dict, Any, Tuple, Set
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ===================== CONFIG =====================

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
]

# Gmail search (adjust label/time window if needed)
GMAIL_QUERY = "newer_than:1y"

# Target Google Sheet
SPREADSHEET_ID = "1sZPoX0x7zJ0QCgr9G-qpXmeIqugOr9xj5WaPiSl_avU"
SHEET_NAME = "Scholarships to Validate"
PROCESSED_SHEET = "_processed_ids"
PROCESSED_EMAILS_SHEET = "_processed_emails"

HEADERS = [
    "Title","Sponsor","Amount","Closing Date","Description","Link",
    "How to Apply","Eligibility","Type","Location","Application Window"
]

# LLM (OpenAI)
OPENAI_MODEL = "gpt-4o-mini"  # robust + inexpensive
OPENAI_TIMEOUT = 90

# Mark an email as "completed" only if we extracted >=1 scholarship.
# Set True to also mark emails with 0 items (faster, but risk skipping unparsed emails).
MARK_EMAIL_COMPLETE_ON_ZERO = False

# ===================== AUTH =======================

def get_credentials() -> Credentials:
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return creds

# ===================== SHEETS =====================

def ensure_sheet_exists(sheets, spreadsheet_id: str, title: str, header: List[str] = []):
    """
    Ensure the specified sheet exists in the spreadsheet, creating it with the given header if necessary.
    """
    ss = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = [s["properties"]["title"] for s in ss.get("sheets", [])]
    if title not in titles:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title": title}}}]}
        ).execute()
        if header:
            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{title}'!1:1",
                valueInputOption="RAW",
                body={"values":[header]}
            ).execute()

def ensure_headers(sheets, spreadsheet_id: str, sheet_name: str, headers: List[str]):
    ensure_sheet_exists(sheets, spreadsheet_id, sheet_name)
    rng = f"'{sheet_name}'!1:1"
    res = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    row = res.get("values", [[]])[0] if res.get("values") else []
    if [c.strip() for c in row] != headers:
        sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()

def ensure_processed_sheets(sheets, spreadsheet_id: str):
    ensure_sheet_exists(sheets, spreadsheet_id, PROCESSED_SHEET, ["emailId|title"])
    ensure_sheet_exists(sheets, spreadsheet_id, PROCESSED_EMAILS_SHEET, ["emailId","status","timestamp","notes"])

def load_processed_keys(sheets, spreadsheet_id: str) -> Set[str]:
    res = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{PROCESSED_SHEET}'!A2:A"
    ).execute()
    vals = res.get("values", [])
    return set(v[0] for v in vals if v)

def load_processed_emails(sheets, spreadsheet_id: str) -> Set[str]:
    res = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{PROCESSED_EMAILS_SHEET}'!A2:A"
    ).execute()
    vals = res.get("values", [])
    return set(v[0] for v in vals if v)

def append_processed_key(sheets, spreadsheet_id: str, key: str):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{PROCESSED_SHEET}'!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values":[[key]]}
    ).execute()

def append_processed_email(sheets, spreadsheet_id: str, email_id: str, status: str, notes: str = ""):
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{PROCESSED_EMAILS_SHEET}'!A:D",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values":[[email_id, status, ts, notes]]}
    ).execute()

def append_row(sheets, spreadsheet_id: str, sheet_name: str, row: List[str]):
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values":[row]}
    ).execute()

def load_existing_title_link_index(sheets, spreadsheet_id: str, sheet_name: str) -> Set[Tuple[str,str]]:
    """
    Build a fast lookup of already-present rows by (normalized_title, normalized_link).
    This lets us treat existing rows as processed even if _processed_ids wasn't updated.
    """
    res = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'!A2:K"  # A: Title, F: Link
    ).execute()
    rows = res.get("values", [])
    index: Set[Tuple[str,str]] = set()
    for r in rows:
        title = (r[0].strip().lower() if len(r) > 0 else "")
        link  = clean_url(r[5]) if len(r) > 5 else ""
        key = (normalize_title(title), link.lower())
        if key[0]:  # need a title
            index.add(key)
    return index

# ===================== LLM ========================

def clean_url(u: str) -> str:
    x = (u or "").strip()
    if not x: return ""
    x = re.sub(r"\s*[-–—]?\s*open in new window.*$", "", x, flags=re.I)
    x = re.sub(r"^<|>$", "", x)
    if not re.match(r"^https?://", x, flags=re.I) and re.search(r"\b[a-z0-9-]+\.[a-z]{2,}\b", x, flags=re.I):
        x = "https://" + x
    while re.search(r"[)\].,;:!?'\"»›…\-–—]$", x):
        x = x[:-1]
    return x

def normalize_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^\d+\.\s*", "", s)  # drop leading "1. " numbering
    return s

def call_openrouter_api(prompt):
    """
    Call the OpenRouter API with the given prompt.
    """
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    if not OPENROUTER_API_KEY:
        raise EnvironmentError("Set OPENROUTER_API_KEY environment variable.")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "google/gemma-3n-e4b-it:free",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2
    }

    max_retries = 5
    backoff_factor = 2

    for attempt in range(max_retries):
        try:
            print("Sending request to OpenRouter API...")
            response = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload)
            response.raise_for_status()
            with open("response_debug.txt", "w") as response_file:
                response_file.write(json.dumps(response.json(), indent=2))  # Write the response as a single string
            print("LLM Response written to response_debug.txt")
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Too Many Requests
                wait_time = backoff_factor ** attempt
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise e

    raise Exception("Max retries exceeded for OpenRouter API.")

def chunk_html_by_lines(html_code: str, max_lines: int) -> List[str]:
    """
    Split the HTML into smaller chunks based on lines.

    Args:
        html_code (str): The full HTML code.
        max_lines (int): The maximum number of lines per chunk.

    Returns:
        List[str]: A list of HTML chunks.
    """
    lines = html_code.splitlines()
    chunks = []

    for i in range(0, len(lines), max_lines):
        chunk = "\n".join(lines[i:i + max_lines])
        chunks.append(chunk)

    return chunks

def chunk_html_by_array(html_array: List[str], max_items: int) -> List[str]:
    """
    Split the HTML array into smaller chunks.

    Args:
        html_array (List[str]): The array of HTML strings.
        max_items (int): The maximum number of items per chunk.

    Returns:
        List[str]: A list of HTML chunks.
    """
    chunks = []

    for i in range(0, len(html_array), max_items):
        chunk = "\n".join(html_array[i:i + max_items])
        chunks.append(chunk)

    return chunks

def extract_items_with_llm(html_code):
    """
    Extract scholarship items from the given HTML code using the LLM.

    Args:
        html_code (str): The HTML code containing <a> tags for scholarships.

    Returns:
        dict: The extracted scholarship items as JSON.
    """
    
    scholarship_array = []

    llm_prompt = f"""
    You are a data extraction assistant. The following HTML contains links to scholarships, with each <a> tag representing one scholarship. Extract the following fields for each scholarship and return them as a JSON array:
    - title: The title of the scholarship
    - sponsor: The sponsor of the scholarship
    - amount: The amount of the scholarship
    - closing_date: The closing date or deadline for the scholarship
    - description: A brief description of the scholarship
    - link: The URL of the scholarship

    HTML:
    {html_code}
    """

    try:
        llm_response = call_openrouter_api(llm_prompt)
        choices = llm_response.get('choices', [])
        if choices and isinstance(choices[0].get('message', {}).get('content', '[]'), str):
            content = choices[0].get('message', {}).get('content', '[]')
            if isinstance(content, str):
                content = content.strip('`').replace('json', '', 1).replace('\n', '').strip()
                scholarship_array.extend(json.loads(content))
    except Exception as e:
        print(f"Line 247: Error calling LLM: {e}")
        return []
    if scholarship_array:
        return scholarship_array
    
def extract_items_with_llm_chunked(html_array):
    """
    Extract scholarship items from the given HTML array using the LLM with chunking.

    Args:
        html_array (List[str]): The array of HTML strings containing scholarships.

    Returns:
        List[Dict[str, Any]]: The extracted scholarship items as JSON.
    """
    chunks = chunk_html_by_array(html_array, max_items=3)  # Process 3 items per chunk
    extracted_items = []

    for chunk in chunks:
        # Append each chunk to the relevant_body_debug file
        with open("relevant_body_debug.txt", "a") as debug_file:
            print("Writing chunk to debug file:")
            debug_file.write(f"\n--- Chunk ---\n{chunk}\n")

        llm_prompt = f"""
        You are a data extraction assistant. The following HTML contains links to scholarships, with each <a> tag representing one scholarship. Extract the following fields for each scholarship and return them as a JSON array:
        - title: The title of the scholarship
        - sponsor: The sponsor of the scholarship
        - amount: The amount of the scholarship
        - closing_date: The closing date or deadline for the scholarship
        - description: A brief description of the scholarship
        - link: The URL of the scholarship

        Only return scholarships that have both a closing date and an amount.

        HTML:
        {chunk}
        """

        try:
            llm_response = call_openrouter_api(llm_prompt)
            content = llm_response.get("choices", [])[0].get("message", {}).get("content", "[]")
            if isinstance(content, str):
                content = content.strip('`').replace('json', '', 1).replace('\n', '').strip()
                try:
                    json_data = json.loads(content)
                    if isinstance(json_data, list):
                        # Filter out invalid scholarships (must have a closing date and an amount)
                        valid_scholarships = [
                            scholarship for scholarship in json_data
                            if scholarship.get("closing_date") and scholarship.get("amount")
                        ]
                        extracted_items.extend(valid_scholarships)
                except json.JSONDecodeError:
                    print("Skipping chunk due to invalid JSON.")
        except Exception as e:
            print(f"Error processing chunk: {e}")

    return extracted_items

def chunk_html(html_code: str, chunk_size: int) -> str:
    """
    Split the HTML into smaller chunks and return a single string for processing.

    Args:
        html_code (str): The full HTML code.
        chunk_size (int): The maximum number of scholarships per chunk.

    Returns:
        str: A single string containing the HTML chunks.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_code, "html.parser")
    scholarships = soup.find_all("p")  # Assuming each <p> tag represents a scholarship

    chunks = []
    for i in range(0, len(scholarships), chunk_size):
        chunk = "\n".join(str(scholarship) for scholarship in scholarships[i:i + chunk_size])
        chunks.append(chunk)

    print("Chunks:", chunks)
    return "\n".join(chunks)  # Combine all chunks into a single string

# ===================== MAIN =======================

def process_messages_with_llm(service, label_name, query=None):
    """
    Fetch Gmail messages, extract HTML, extract links, and pass to the LLM.
    Returns a JSON array of extracted scholarship items.
    """
    extracted_items = []
    try:
        # Fetch messages
        if query:
            print(f"Fetching emails with query: {query}")
            results = service.users().messages().list(userId='me', q=query, maxResults=100).execute()
        else:
            print(f"Fetching emails under label: {label_name}")
            results = service.users().messages().list(userId='me', labelIds=[label_name], maxResults=100).execute()

        messages = results.get('messages', [])
        print(f"Found {len(messages)} emails.")

        # Limit processing to the first 3 emails
        messages = messages[1:10]

        for message in messages:
            msg_id = message['id']
            msg = service.users().messages().get(userId='me', id=msg_id).execute()
            headers = msg.get('payload', {}).get('headers', [])
            subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'No Subject')
            print(f"Processing Message ID: {msg_id}, Subject: {subject}")

            # Extract HTML body
            payload = msg.get('payload', {})
            parts = payload.get('parts', [])
            decoded_body = ""
            for part in parts:
                if part.get('mimeType') == 'text/html':
                    encoded_body = part.get('body', {}).get('data', '')
                    decoded_body = base64.urlsafe_b64decode(encoded_body)
                    break

            if not decoded_body:
                print("No HTML body found in the email.")
                continue

            # Extract links from the decoded HTML
            relevant_body = extract_links_from_html(decoded_body)

            # Write relevant_body to a txt file for debugging
            # with open("relevant_body_debug.txt", "w") as debug_file:
            #     debug_file.write("\n".join(relevant_body))

            # Pass the relevant part of the HTML to the LLM
            try:
                print("Sending full HTML to LLM")
                llm_response = extract_items_with_llm_chunked(relevant_body)
                # llm_response = extract_items_with_llm(relevant_body)

                if isinstance(llm_response, list):
                    extracted_items.extend(llm_response)  # Append extracted items to the list
                elif llm_response is not None:
                    extracted_items.append(llm_response)
            except Exception as e:
                print(f"Error calling LLM: {e}")

    except Exception as e:
        print(f"Error processing messages: {e}")

    return extracted_items

def decode_base64(encoded_text):
    """
    Decode Base64-encoded text.
    """
    import base64
    try:
        return base64.urlsafe_b64decode(encoded_text).decode("utf-8")
    except Exception as e:
        print(f"Error decoding Base64 content: {e}")
        return ""

def extract_links_from_html(html):
    """
    Extract all <p> tags from the given HTML.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Find all <p> tags
    p_tags = soup.find_all("p")
    print("Extracted relevant HTML")
    return [str(p) for p in p_tags]

def append_scholarships_to_sheet(sheets, spreadsheet_id: str, sheet_name: str, scholarships: List[Dict[str, Any]]):
    """
    Append scholarship data to the specified Google Sheet.
    """
    rows = []
    for scholarship in scholarships:
        row = [
            scholarship.get("title", ""),
            scholarship.get("sponsor", ""),
            scholarship.get("amount", ""),
            scholarship.get("closing_date", ""),
            scholarship.get("description", ""),
            scholarship.get("link", ""),
            scholarship.get("how_to_apply", ""),
            scholarship.get("eligibility", ""),
            scholarship.get("type", ""),
            scholarship.get("location", ""),
            scholarship.get("application_window", "")
        ]
        rows.append(row)

    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

def cache_email_ids(sheets, spreadsheet_id: str, email_ids: List[str]):
    """
    Append processed email IDs to the cache sheet.
    """
    rows = [[email_id] for email_id in email_ids]
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=PROCESSED_EMAILS_SHEET,
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

def deduplicate_scholarships(sheets, spreadsheet_id: str, sheet_name: str, scholarships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove scholarships that already exist in the Google Sheet.
    """
    existing_data = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A2:B"
    ).execute().get("values", [])

    existing_titles_links = {(row[0], row[5]) for row in existing_data if len(row) >= 6}

    deduplicated = [
        scholarship for scholarship in scholarships
        if (scholarship.get("title"), scholarship.get("link")) not in existing_titles_links
    ]

    return deduplicated

def main():
    creds = get_credentials()
    gmail = build("gmail", "v1", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

    ensure_headers(sheets, SPREADSHEET_ID, SHEET_NAME, HEADERS)
    ensure_processed_sheets(sheets, SPREADSHEET_ID)

    processed_keys = load_processed_keys(sheets, SPREADSHEET_ID)         # emailId|title
    processed_emails = load_processed_emails(sheets, SPREADSHEET_ID)     # emailId
    existing_index = load_existing_title_link_index(sheets, SPREADSHEET_ID, SHEET_NAME)

    # Ensure the Gmail service object is passed correctly to the function
    label_name = "CSPScholarships"
    extracted_scholarships = process_messages_with_llm(gmail, label_name, query="label:CSPScholarships newer_than:1y")

    # Print the length of the extracted JSON array
    print(f"Extracted {len(extracted_scholarships)} scholarships.")

    # Deduplicate scholarships
    extracted_scholarships = deduplicate_scholarships(sheets, SPREADSHEET_ID, SHEET_NAME, extracted_scholarships)
    print(f"After deduplication: {len(extracted_scholarships)} scholarships remain.")

    # Print the names of scholarships that remain after deduplication
    remaining_titles = [scholarship.get("title", "Unknown Title") for scholarship in extracted_scholarships]
    print("Scholarships remaining after deduplication:", remaining_titles)

    # Write scholarships to the Google Sheet
    append_scholarships_to_sheet(sheets, SPREADSHEET_ID, SHEET_NAME, extracted_scholarships)

if __name__ == "__main__":
    main()
