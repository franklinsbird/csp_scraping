import csv
import re
import time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import datetime
from typing import Dict, Tuple, List, Union
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse

SPREADSHEET_ID = "1sZPoX0x7zJ0QCgr9G-qpXmeIqugOr9xj5WaPiSl_avU"
SHEET_NAME = "Universities"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; roster-scraper/1.0; +https://example.com/bot)"
}

YEAR_MAP = {
    "sr.": "senior", "sr": "senior", "senior": "senior",
    "jr.": "junior", "jr": "junior", "junior": "junior",
    "so.": "sophomore", "so": "sophomore", "sophomore": "sophomore",
    "fr.": "freshman", "fr": "freshman", "fy.": "freshman", "fy": "freshman", "freshman": "freshman", "first-year": "freshman",
    "graduate": "graduate student"
}


def get_current_season_start_year(now=None) -> int:
    """Return the academic season start year (e.g., 2025 for 2025-2026 season).
    Rule: if current month is June or later (>=6) the season_start is current year,
    otherwise it's current year - 1. This matches the sports academic season convention.
    """
    if now is None:
        now = datetime.datetime.now()
    return now.year if now.month >= 6 else now.year - 1


def class_year_to_academic_level(text: str, now=None) -> str:
    """Convert a text that may contain a graduation year (e.g., 'Class of 2026', '2026')
    into an academic level string matching YEAR_MAP values: 'freshman','sophomore','junior','senior','graduate student'.

    Uses the current season to interpret graduation years. Example:
      - if season is 2025-2026 (season_start=2025, season_end=2026):
        class of 2026 -> senior, 2027 -> junior, 2028 -> sophomore, 2029 -> freshman
    """
    if not text:
        return ""
    if now is None:
        now = datetime.datetime.now()
    season_start = get_current_season_start_year(now)
    season_end = season_start + 1

    s = str(text).lower()
    # try to find a 4-digit year token
    m = re.search(r"\b(20\d{2})\b", s)
    if m:
        grad_year = int(m.group(1))
        # delta: grad_year - season_end
        delta = grad_year - season_end
        if delta == 0:
            return "senior"
        elif delta == 1:
            return "junior"
        elif delta == 2:
            return "sophomore"
        elif delta == 3:
            return "freshman"
        elif grad_year < season_end:
            # already graduated -> treat as graduate
            return "graduate student"
        else:
            # far-future grad year; assume incoming freshman or return normalized token
            return "freshman"

    # fallback to existing YEAR_MAP lookups
    s_clean = s.replace('.', '').strip()
    return YEAR_MAP.get(s_clean, s_clean)

# Map common synonyms/abbreviations → canonical
POSITION_MAP = {
    "gk": "GK", "goal keeper": "GK", "goalkeeper": "GK", "keeper": "GK", 
    "d": "D", "def": "D", "defender": "D", "back": "D", "cb": "D", "rb": "D", "lb": "D", "fb": "D",
    "m": "M", "mid": "M", "midfield": "M", "midfielder": "M", "dm": "M", "cm": "M", "am": "M", "rm": "M", "lm": "M", "wing": "M",
    "f": "F", "fw": "F", "forward": "F", "striker": "F", "st": "F", "cf": "F", "winger": "F", "fwd": "F"
}

POS_TOKEN_RE = re.compile(
    r'\b('
    r'gk|goal keeper|goalkeeper|keeper|'
    r'd|def|defender|back|cb|rb|lb|fb|'
    r'm|mid|midfield|midfielder|dm|cm|am|rm|lm|wing(?!erless)|'
    r'f|fw|forward|striker|st|cf|winger'
    r')\b',
    re.IGNORECASE
)

def canon_pos(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = s.replace(".", "")
    # Keep the shortest/clearest token when compound like "D/M"
    s = s.replace("\\", "/")
    parts = re.split(r"[\/,\-\s]+", s)
    outs = []
    for p in parts:
        if not p:
            continue
        p2 = POSITION_MAP.get(p.lower())
        if p2 and p2 not in outs:
            outs.append(p2)
    return "/".join(outs) if outs else ""

# Create a single session with retry/backoff and default headers
SESSION = requests.Session()
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "HEAD"])
_adapter = HTTPAdapter(max_retries=_retry)
SESSION.mount("http://", _adapter)
SESSION.mount("https://", _adapter)
SESSION.headers.update(HEADERS)


def fetch(url: str, referer: str = None, timeout: int = 30) -> BeautifulSoup:
    """Fetch a URL using a persistent session with retries and optional Referer header.
    Will attempt one extra fetch with a Referer pointing to the site's root when a 403 is encountered.
    """
    headers = dict(SESSION.headers)
    if referer:
        headers["Referer"] = referer
    try:
        r = SESSION.get(url, headers=headers, timeout=timeout)
    except requests.RequestException:
        # Let caller handle retry/backoff; raise up
        raise

    # If we got a 403 and no referer was provided, try once more with a referer set to the site's origin
    if r.status_code == 403 and not referer:
        try:
            parsed = urlparse(url)
            origin = f"{parsed.scheme}://{parsed.netloc}/"
            headers2 = dict(SESSION.headers)
            headers2["Referer"] = origin
            # small backoff
            time.sleep(1)
            r2 = SESSION.get(url, headers=headers2, timeout=timeout)
            r = r2
        except Exception:
            pass

    # Raise for HTTP errors so callers can catch
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def looks_like_sidearm(soup: BeautifulSoup) -> bool:
    meta = soup.find("meta", {"name":"generator"})
    content = ""
    if meta and hasattr(meta, "get"):
        content = meta.get("content")
        if isinstance(content, str) and "sidearm" in content.lower():
            return True
    # Heuristic: many SIDEARM sites have classes or script paths with "sidearm"
    return bool(soup.select_one("[class*='sidearm'], script[src*='sidearm']"))

def clean_text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()

# --- Extractors ---

def extract_sidearm(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    """
    Extract position and year data from SIDEARM roster pages and return as a list of dictionaries.
    """
    players = []
    # Pattern 1: list-based roster
    items = soup.select("li.sidearm-roster-player, li.sidearm-roster-item")
    for li in items:
        player = {
            "position": "",
            "year": ""
        }

        # Extract position
        pos_el = li.select_one(".sidearm-roster-player-position, .position, [data-player-position]")
        if pos_el:
            player["position"] = canon_pos(clean_text(pos_el))
        else:
            # Try a labeled field within the card
            label_els = li.select("div, span, p")
            for e in label_els:
                t = clean_text(e)
                if re.search(r"^\s*(pos|position)\s*[:\-]\s*", t, re.I):
                    player["position"] = canon_pos(re.sub(r"^\s*(pos|position)\s*[:\-]\s*", "", t, flags=re.I))
                    break
            if not player["position"]:
                # Fallback regex on the entire card text
                blob = clean_text(li)
                m = POS_TOKEN_RE.search(blob)
                if m:
                    player["position"] = canon_pos(m.group(1))

        # Extract year
        # year_el = li.select_one(".sidearm-roster-player-academic-year, .year, [data-player-year]")
        year_el = li.select_one(".sidearm-roster-player-academic-year")
        if year_el:
            year_text = clean_text(year_el).lower()
            player["year"] = YEAR_MAP.get(year_text, year_text)
        else:
            # Fallback regex on the entire card text
            blob = clean_text(li)
            for year_key, year_value in YEAR_MAP.items():
                if year_key in blob.lower():
                    player["year"] = year_value
                    break

        players.append(player)

    return players

def extract_non_sidearm(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Extract position and year data from non-sidearm roster pages.
    """
    players = []
    table_div = soup.select_one(".roster-data")
    if table_div:
        rows = table_div.find_all("tr")
        for row in rows:
            player = {
                "position": "",
                "year": ""
            }

            # Extract position
            position_cell = row.find("td", attrs={"data-field": "position"})
            if position_cell:
                pos_text = ''.join(position_cell.find_all(string=True, recursive=False)).strip('" \n ')
                pos_text = pos_text.strip()
                print("Position Text:", pos_text)
                player["position"] = POSITION_MAP.get(pos_text, pos_text)

            # Extract year
            year_cell = row.find("td", attrs={"data-field": "year"})
            if year_cell:
                year_text = ''.join(year_cell.find_all(string=True, recursive=False)).strip('" \n ')
                year_text = year_text.strip().lower()
                print("Year Text:", year_text)
                player["year"] = YEAR_MAP.get(year_text, year_text)
            else:
                # Fallback regex on the entire row text
                blob = clean_text(row)
                for year_key, year_value in YEAR_MAP.items():
                    if year_key in blob.lower():
                        player["year"] = year_value
                        break

            players.append(player)

    return players

def update_roster_url_with_fallback(url: str) -> str:
    """
    Check if the URL without a year loads; if not, update the year.
    """
    try:
        # Attempt to load the URL without a year
        base_url = re.sub(r"/\d{4}(-\d{2})?", "/", url)
        response = requests.head(base_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return base_url
    except requests.RequestException:
        pass

    # Fallback: update the year in the original URL
    current_year = datetime.datetime.now().year
    return re.sub(r"\b(\d{4})(-\d{2})?\b", str(current_year), url)

def update_year_range_in_url(url: str) -> str:
    """
    Update the year range in the URL to the current year range.
    """
    current_year = datetime.datetime.now().year
    next_year = current_year + 1
    return re.sub(r"\b(\d{4})-(\d{2})\b", f"{current_year}-{str(next_year)[-2:]}", url)


def _has_year_token(url: str) -> bool:
    """Return True if the URL path contains a year token like /2025/ or /2025-26/"""
    if not url:
        return False
    try:
        p = urlparse(url)
        path = p.path or ''
    except Exception:
        path = url
    return bool(re.search(r"/\d{4}(-\d{2})?(?:/|$)", path))


def _remove_year_segment(url: str) -> str:
    """Remove any single path segment that is a 4-digit year or year-range and return the new URL.

    Example: https://site.edu/sports/msoc/2025-26/roster -> https://site.edu/sports/msoc/roster
    """
    if not url:
        return url
    try:
        p = urlparse(url)
    except Exception:
        return url
    parts = [seg for seg in p.path.split('/') if seg and not re.fullmatch(r"\d{4}(-\d{2})?", seg)]
    new_path = '/' + '/'.join(parts) if parts else '/'
    new_url = f"{p.scheme or 'https'}://{p.netloc}{new_path}"
    if p.query:
        new_url += '?' + p.query
    if p.fragment:
        new_url += '#' + p.fragment
    return new_url

def count_positions_and_years(soup: BeautifulSoup) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Count positions and class years from the roster page.
    """
    positions = {"defenders": 0, "forwards": 0, "midfielders": 0, "goalkeepers": 0}
    years = {"freshman": 0, "sophomore": 0, "junior": 0, "senior": 0}

    items = soup.select("li.sidearm-roster-player, li.sidearm-roster-item")
    for li in items:
        text = clean_text(li)
        print("Roster Item Text:", text)
        # Count positions
        for pos, canonical in POSITION_MAP.items():
            if re.search(rf"\b{pos}\b", text, re.IGNORECASE):
                positions[canonical.lower() + "s"] += 1

        # Count class years
        for year in years.keys():
            if year in text.lower():
                years[year] += 1

    return positions, years

def process_rosters(sheets, spreadsheet_id: str, sheet_name: str):
    """
    Process roster URLs from the Google Sheet and update counts.
    """
    res = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!B3:B, T3:T, AQ3:AQ"
    ).execute()
    rows = res.get("values", [])

    for row in rows:
        men_url = update_roster_url_with_fallback(row["men_roster_url"])
        women_url = update_roster_url_with_fallback(row["women_roster_url"])

        for url, prefix in [(men_url, "men"), (women_url, "women")]:
            if url:
                try:
                    soup = fetch(url)
                    positions, years = count_positions_and_years(soup)

                    # Update counts in the row
                    row[f"{prefix}_defenders"] = positions["defenders"]
                    row[f"{prefix}_forwards"] = positions["forwards"]
                    row[f"{prefix}_midfielders"] = positions["midfielders"]
                    row[f"{prefix}_goalkeepers"] = positions["goalkeepers"]

                    row[f"{prefix}_freshman"] = years["freshman"]
                    row[f"{prefix}_sophomore"] = years["sophomore"]
                    row[f"{prefix}_junior"] = years["junior"]
                    row[f"{prefix}_senior"] = years["senior"]
                except Exception as e:
                    print(f"Error processing {url}: {e}")

    # Write updated rows back to the sheet
    sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A2:Z",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

# Define SCOPES for Google API access
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Ensure the return type matches the expected Credentials type
from google.oauth2.credentials import Credentials as GoogleCredentials

def get_credentials() -> GoogleCredentials:
    creds = None
    if not os.path.exists("token.json"):
        print("token.json not found. Initiating authentication flow...")
        if not os.path.exists("credentials.json"):
            raise FileNotFoundError("Missing credentials.json file for authentication.")
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    else:
        creds = GoogleCredentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
            with open("token.json", "w") as f:
                f.write(creds.to_json())
    return creds

def update_url_with_year_logic(url: str) -> str:
    """
    Check the URL format and update the year or year range accordingly.
    """
    if re.search(r"\b\d{4}-\d{2}\b", url):
        # URL uses the year range format (e.g., 2024-25)
        return update_year_range_in_url(url)
    else:
        # URL uses a single year format (e.g., 2024)
        return update_roster_url_with_fallback(url)

def map_players_to_positions_and_years(players: List[Union[Dict[str, str], Tuple[str, str]]]) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Map the list of players into dictionaries for positions and years, using POSITION_MAP for formatting.

    Args:
        players (List[Union[Dict[str, str], Tuple[str, str]]]): List of player data, where each player is either a dictionary
                                                                with keys like 'position' and 'year', or a tuple.

    Returns:
        Tuple[Dict[str, int], Dict[str, int]]: Two dictionaries for positions and years.
    """
    positions = {"defenders": 0, "forwards": 0, "midfielders": 0, "goalkeepers": 0}
    years = {"freshman": 0, "sophomore": 0, "junior": 0, "senior": 0, "graduate student": 0}

    for player in players:
        if isinstance(player, dict):
            # Extract values from dictionary
            position = player.get("position", "").lower()
            year = player.get("year", "").lower()
        else:
            continue  # Skip invalid player formats

        # Map positions using POSITION_MAP, including hybrid positions like 'D/M'
        canonical_position = POSITION_MAP.get(position, None)
        if canonical_position:
            # Single canonical code like 'D','M','F','GK'
            if canonical_position == "D":
                positions["defenders"] += 1
            elif canonical_position == "F":
                positions["forwards"] += 1
            elif canonical_position == "M":
                positions["midfielders"] += 1
            elif canonical_position == "GK":
                positions["goalkeepers"] += 1
        else:
            # Handle hybrid positions (e.g., 'D/M', 'Defense/Midfield', 'D & M') by splitting on common separators
            parts = re.split(r"\s*[\/&|,]\s*", position)
            counted_any = False
            for part in parts:
                token = part.strip()
                if not token:
                    continue
                # Try direct mapping of the token
                tok_canon = POSITION_MAP.get(token, None)
                # Fallback heuristics: use first letter when mapping not found
                if not tok_canon:
                    fl = token[0].upper()
                    if fl == "D":
                        tok_canon = "D"
                    elif fl == "M":
                        tok_canon = "M"
                    elif fl == "F":
                        tok_canon = "F"
                    elif fl == "G":
                        tok_canon = "GK"
                if tok_canon == "D":
                    positions["defenders"] += 1
                    counted_any = True
                elif tok_canon == "M":
                    positions["midfielders"] += 1
                    counted_any = True
                elif tok_canon == "F":
                    positions["forwards"] += 1
                    counted_any = True
                elif tok_canon == "GK":
                    positions["goalkeepers"] += 1
                    counted_any = True

            # If no parts produced a count, try a last-resort keyword search
            if not counted_any:
                if re.search(r"\b(defense|defender|back|def)\b", position, re.I):
                    positions["defenders"] += 1
                if re.search(r"\b(midfield|midfielder|mid)\b", position, re.I):
                    positions["midfielders"] += 1
                if re.search(r"\b(forward|forwarder|forw|attacker|fw)\b", position, re.I):
                    positions["forwards"] += 1
                if re.search(r"\b(goalkeeper|keeper|gk|goal)\b", position, re.I):
                    positions["goalkeepers"] += 1

        # Map years
        if year in years:
            years[year] += 1

    return positions, years

def fetch_via_playwright(url: str, referer: str = None, timeout: int = 30000, try_origin_first: bool = True):
    """Attempt to load the page using Playwright (headless Chromium) and return a BeautifulSoup object.
    Improvements to increase chance of bypassing simple CDN/WAF checks:
      - visit the site's origin first (homepage) so the site can set any required cookies/headers
      - try to click a local link to the roster page from the homepage when possible
      - set realistic headers and a small stealth script
    Returns BeautifulSoup parsed HTML. On failure, raises an exception.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise RuntimeError("Playwright is not installed") from e

    ua = HEADERS.get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36")
    extra_headers = {"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        extra_headers["Referer"] = referer

    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=ua, viewport={"width": 1280, "height": 800})
        try:
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        except Exception:
            pass
        page = context.new_page()
        try:
            page.set_extra_http_headers(extra_headers)
        except Exception:
            pass
        page.set_default_navigation_timeout(timeout)

        html = ""
        # Optionally visit origin first to establish cookies / headers
        if try_origin_first:
            try:
                page.goto(origin, wait_until="load", timeout=min(8000, timeout))
                try:
                    page.wait_for_load_state("networkidle", timeout=2000)
                except Exception:
                    pass
                # try to find a local link to the roster path and click it (helps with referer checks)
                try:
                    roster_segment = parsed.path.split('/')[-1]
                    # look for links containing 'roster' or the final path segment
                    link = page.query_selector("a[href*='roster']") or page.query_selector(f"a[href*='{roster_segment}']")
                    if link:
                        try:
                            link.click()
                            try:
                                page.wait_for_load_state("networkidle", timeout=4000)
                            except Exception:
                                pass
                        except Exception:
                            # fall back to direct navigation
                            page.goto(url, wait_until="load", timeout=timeout)
                    else:
                        # no link found, navigate directly
                        page.goto(url, wait_until="load", timeout=timeout)
                except Exception:
                    # fallback to direct navigation
                    page.goto(url, wait_until="load", timeout=timeout)
            except Exception as e:
                # origin visit failed; try direct navigation anyway
                try:
                    page.goto(url, wait_until="load", timeout=timeout)
                except Exception as e2:
                    raise RuntimeError(f"Playwright navigation failed: {e2}") from e2
        else:
            page.goto(url, wait_until="load", timeout=timeout)
            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass

        try:
            html = page.content()
        except Exception as e:
            html = ""

        # Save HTML for debugging when it looks like an error page
        if html and ("The request could not be satisfied" in html or "AccessDenied" in html or "Error 403" in html):
            try:
                safe_host = parsed.netloc.replace(':', '_')
                dump_path = f"playwright_debug_{safe_host}.html"
                with open(dump_path, 'w', encoding='utf-8') as df:
                    df.write(html)
                print(f"Playwright fetched HTML length: {len(html)}\nSaved debug HTML to {dump_path}")
            except Exception:
                pass

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    # If the page clearly indicates a CDN/WAF error, raise to let caller decide fallback behavior
    if html and ("The request could not be satisfied" in html or "AccessDenied" in html or "Error 403" in html):
        raise RuntimeError("Playwright fetched an error page (CDN/WAF). See saved HTML for details.")

    return BeautifulSoup(html, "html.parser")

def try_yearless_and_update_sheet(limit: int = 0):
    """For rows whose men/women roster URL contains a year token, try the year-less URL.

    If the year-less URL loads, update the sheet to use that URL and populate position/year counts
    into the corresponding columns (if present in the header). Prints progress as it goes.
    limit: optional maximum number of rows to process (0 = no limit)
    """
    creds = get_credentials()
    sheets = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # Read header row (row 2) to discover column indices for roster URLs and counts
    header_range = f"'{SHEET_NAME}'!B2:AQ2"
    hdr_res = sheets.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=header_range).execute()
    header_rows = hdr_res.get('values', [])
    if not header_rows:
        print('Could not read header row B2:AQ2')
        return
    header = [c.strip() for c in header_rows[0]]

    def find_col(name_variants):
        for i, h in enumerate(header):
            if h and h.strip().lower() in name_variants:
                return i
        return None

    men_url_idx = find_col({'men_roster_url', 'men roster url', 'men url', 'men_roster'})
    women_url_idx = find_col({'women_roster_url', 'women roster url', 'women url', 'women_roster'})

    # Position/year count columns (optional)
    men_counts = {
        'freshman': find_col({'men_freshman', 'men_freshmen', 'men_freshman_count'}),
        'sophomore': find_col({'men_sophomore', 'men_sophomores'}),
        'junior': find_col({'men_juniors', 'men_junior'}),
        'senior': find_col({'men_seniors', 'men_senior'}),
    }
    men_pos = {
        'defenders': find_col({'men_defenders', 'men_defender', 'men_def'}),
        'forwards': find_col({'men_forwards', 'men_forward', 'men_fw'}),
        'midfielders': find_col({'men_mids', 'men_midfielders', 'men_mid'}),
        'goalkeepers': find_col({'men_gk', 'men_goalkeepers', 'men_goalie'}),
    }

    women_counts = {
        'freshman': find_col({'women_freshman', 'women_freshmen'}),
        'sophomore': find_col({'women_sophomore', 'women_sophomores'}),
        'junior': find_col({'women_juniors', 'women_junior'}),
        'senior': find_col({'women_seniors', 'women_senior'}),
    }
    women_pos = {
        'defenders': find_col({'women_defenders', 'women_defender', 'women_def'}),
        'forwards': find_col({'women_forwards', 'women_forward', 'women_fw'}),
        'midfielders': find_col({'women_mids', 'women_midfielders', 'women_mid'}),
        'goalkeepers': find_col({'women_gk', 'women_goalkeepers', 'women_goalie'}),
    }

    # Read data rows B3:AQ
    data_range = f"'{SHEET_NAME}'!B3:AQ"
    res = sheets.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=data_range).execute()
    rows = res.get('values', [])
    print(f"Read {len(rows)} data rows from sheet")

    # Find indices for men_freshman and women_freshman to filter rows that indicate 'no longer exist'
    men_fresh_idx = find_col({'men_freshman', 'men_freshmen', 'men_freshman_count', 'men_freshmen_count'})
    women_fresh_idx = find_col({'women_freshman', 'women_freshmen', 'women_freshman_count', 'women_freshmen_count'})

    def _is_no_longer(val):
        return bool(val and isinstance(val, str) and 'no longer exist' in val.strip().lower())

    # Build list of rows to process (sheet row number, index into rows list)
    rows_to_process = []
    for sheet_row_num, r in enumerate(rows, start=3):
        if len(r) == 0:
            continue
        men_val = r[men_fresh_idx] if men_fresh_idx is not None and len(r) > men_fresh_idx else ''
        women_val = r[women_fresh_idx] if women_fresh_idx is not None and len(r) > women_fresh_idx else ''
        if _is_no_longer(men_val) or _is_no_longer(women_val):
            rows_to_process.append((sheet_row_num, sheet_row_num - 3))
        if limit and len(rows_to_process) >= limit:
            break

    print(f"Rows matching 'no longer exist' filter: {len(rows_to_process)}")

    updates = []
    for sheet_row_num, local_idx in rows_to_process:
        row = rows[local_idx]
        university = row[0].strip() if len(row) > 0 else ''
        men_url = row[men_url_idx].strip() if men_url_idx is not None and len(row) > men_url_idx and row[men_url_idx] else ''
        women_url = row[women_url_idx].strip() if women_url_idx is not None and len(row) > women_url_idx and row[women_url_idx] else ''

        for side, orig_url, col_idx in (('men', men_url, men_url_idx), ('women', women_url, women_url_idx)):
            if not orig_url or not _has_year_token(orig_url):
                continue

            yearless = _remove_year_segment(orig_url)
            print(f"Row {sheet_row_num}: {university} ({side}) - trying year-less URL: {yearless}")
            try:
                soup = fetch(yearless)
            except Exception as e:
                print(f"  Year-less URL did NOT load: {e}")
                continue

            # Attempt to extract players
            try:
                if looks_like_sidearm(soup):
                    players = extract_sidearm(soup, yearless)
                else:
                    players = extract_non_sidearm(soup)
            except Exception as e:
                print(f"  Error extracting players from {yearless}: {e}")
                players = []

            if not players:
                print(f"  No players extracted from {yearless}")
                continue

            # Map counts
            pos_counts, year_counts = map_players_to_positions_and_years(players)

            # Ensure row has enough columns
            if len(row) < len(header):
                row.extend([''] * (len(header) - len(row)))

            # Update roster URL cell
            if col_idx is not None:
                row[col_idx] = yearless

            # Update men/women count columns if indices found
            if side == 'men':
                for k, v in men_counts.items():
                    if v is not None:
                        row[v] = year_counts.get(k, '')
                for k, v in men_pos.items():
                    if v is not None:
                        row[v] = pos_counts.get(k, '')
            else:
                for k, v in women_counts.items():
                    if v is not None:
                        row[v] = year_counts.get(k, '')
                for k, v in women_pos.items():
                    if v is not None:
                        row[v] = pos_counts.get(k, '')

            # record updated row for batch write
            updates.append((sheet_row_num, local_idx))
            print(f"  Success: updated sheet row {sheet_row_num} with year-less URL and counts")

    if not updates:
        print("No updates to write")
        return

    # Try batch write of all rows; fallback to per-row writes with retries on failure
    try:
        sheets.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=data_range,
            valueInputOption='RAW',
            body={'values': rows}
        ).execute()
        print(f"Wrote {len(updates)} updated rows back to sheet (batch)")
    except Exception as e:
        print(f"Batch write failed: {e}; falling back to per-row updates with retries")
        # Per-row update with retries
        for sheet_row_num, local_idx in updates:
            single_row = rows[local_idx]
            row_range = f"'{SHEET_NAME}'!B{sheet_row_num}:AQ{sheet_row_num}"
            success = False
            for attempt in range(1, 4):
                try:
                    sheets.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=row_range,
                        valueInputOption='RAW',
                        body={'values': [single_row]}
                    ).execute()
                    success = True
                    print(f"  Wrote row {sheet_row_num} (attempt {attempt})")
                    break
                except Exception as ex:
                    wait = 2 ** attempt
                    print(f"  Failed to write row {sheet_row_num} (attempt {attempt}): {ex}; retrying in {wait}s")
                    time.sleep(wait)
            if not success:
                print(f"  Giving up on row {sheet_row_num} after retries")

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--try-yearless', action='store_true', help='Try year-less URLs and update the sheet')
    p.add_argument('--limit', type=int, default=0, help='Limit number of rows to process when trying year-less (0=all)')
    args = p.parse_args()

    if args.try_yearless:
        try_yearless_and_update_sheet(limit=args.limit)
        return

    output_csv = "rosters_out.csv"

    # Initialize Google Sheets API
    creds = get_credentials()
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # Read data from Google Sheet
    res = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!B3:AQ"
    ).execute()
    rows = res.get("values", [])
    # Limit processing to the first 10 rows for testing
    # rows = rows[:10]

    # Write data to CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        # Header: university + original/updated urls + men/women counts
        header = [
            "university_name", "original_men_url", "updated_men_url", "original_women_url", "updated_women_url",
            "men_freshman", "men_sophomore", "men_juniors", "men_seniors", "men_graduates",
            "men_defenders", "men_forwards", "men_mids", "men_gk",
            "women_freshman", "women_sophomore", "women_juniors", "women_seniors", "women_graduates",
            "women_defenders", "women_forwards", "women_mids", "women_gk"
        ]
        writer.writerow(header)

        # For each row, fetch roster pages and route to the appropriate extractor (Sidearm vs non-Sidearm)
        for row in rows:
            university_name = row[0].strip() if row and len(row) > 0 else ""
            print("Processing row:", university_name)
            men_url = row[18].strip() if row and len(row) > 18 else ""
            women_url = row[41].strip() if row and len(row) > 41 else ""

            updated_men_url = update_url_with_year_logic(men_url) if men_url else ""
            updated_women_url = update_url_with_year_logic(women_url) if women_url else ""

            # Helper to fetch players list for a single url using the existing extractors
            def get_players_for_url(url: str):
                if not url:
                    return []

                # Only attempt scraping when the URL path contains a year token (e.g. /2025-26/ or /2025/)
                try:
                    parsed = urlparse(url)
                    path = parsed.path or ""
                except Exception:
                    path = ""

                if not re.search(r"/\d{4}(-\d{2})?/", path):
                    # No year token in path -> assume this is already the base URL format; skip to save time
                    print(f"Skipping scrape for {url} (no year token in path)")
                    return []

                def try_extract(soup_obj, base_url):
                    if looks_like_sidearm(soup_obj):
                        try:
                            return extract_sidearm(soup_obj, base_url)
                        except Exception:
                            return []
                    else:
                        try:
                            return extract_non_sidearm(soup_obj)
                        except Exception:
                            return []

                players = []
                # Try normal session-based fetch first; if it raises a 403, attempt Playwright fallback
                try:
                    page_soup = fetch(url)
                    players = try_extract(page_soup, url)
                except requests.exceptions.HTTPError as he:
                    code = None
                    try:
                        code = he.response.status_code
                    except Exception:
                        code = None
                    if code == 403:
                        print(f"Got 403 fetching {url}; attempting Playwright fallback...")
                        try:
                            pw_soup = fetch_via_playwright(url)
                            print("Playwright fetched soup:", pw_soup.title.string if pw_soup.title else "No title")
                            players = try_extract(pw_soup, url)
                            print(f"Playwright extracted {len(players)} players for {url}")
                        except Exception as e:
                            print(f"Playwright fetch failed for {url}: {e}")
                    else:
                        print(f"Error fetching {url}: {he}")
                except Exception as e:
                    print(f"Error fetching {url}: {e}")

                # If no players found, try fallback by removing the year segment (e.g., /sports/msoc/2025-26/roster -> /sports/msoc/roster)
                if not players:
                    try:
                        parts = parsed.path.split('/')
                        # find a year-like segment and remove it
                        new_parts = list(parts)
                        for i, p in enumerate(parts):
                            if re.match(r"^\d{4}(-\d{2})?$", p):
                                new_parts.pop(i)
                                break
                        # build fallback path and attempt fetch
                        new_path = '/'.join([seg for seg in new_parts if seg])
                        new_path = '/' + new_path if new_path and not new_path.startswith('/') else new_path
                        fallback_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
                        print(f"No players found at {url}. Trying fallback without year: {fallback_url}")
                        try:
                            fallback_soup = fetch(fallback_url)
                            players = try_extract(fallback_soup, fallback_url)
                        except requests.exceptions.HTTPError as he2:
                            # Don't attempt Playwright on the fallback URL; only try Playwright for the original year-token URL
                            print(f"Fallback fetch failed for {fallback_url}: {he2}")
                        except Exception as e2:
                            print(f"Fallback fetch failed for {fallback_url}: {e2}")
                    except Exception:
                        pass

                return players

            men_players = get_players_for_url(updated_men_url)
            women_players = get_players_for_url(updated_women_url)

            # Normalize player dicts and ensure position/year canonicalization
            def normalize_players(players_list):
                out = []
                for item in players_list:
                    if isinstance(item, dict):
                        pos = item.get("position", "")
                        yr = item.get("year", "")
                    elif isinstance(item, (list, tuple)):
                        pos = item[1] if len(item) > 1 else ""
                        yr = item[2] if len(item) > 2 else ""
                    else:
                        continue
                    pos_norm = canon_pos(pos) if isinstance(pos, str) else ""
                    yr_norm = YEAR_MAP.get(str(yr).lower().strip(), str(yr).lower().strip()) if isinstance(yr, str) else ""
                    out.append({"position": pos_norm, "year": yr_norm})
                return out

            men_normalized = normalize_players(men_players)
            women_normalized = normalize_players(women_players)

            men_positions, men_years = map_players_to_positions_and_years(men_normalized)
            women_positions, women_years = map_players_to_positions_and_years(women_normalized)

            out_row = [
                university_name, men_url, updated_men_url, women_url, updated_women_url,
                men_years.get("freshman", 0), men_years.get("sophomore", 0), men_years.get("junior", 0), men_years.get("senior", 0), men_years.get("graduate student", 0),
                men_positions.get("defenders", 0), men_positions.get("forwards", 0), men_positions.get("midfielders", 0), men_positions.get("goalkeepers", 0),
                women_years.get("freshman", 0), women_years.get("sophomore", 0), women_years.get("junior", 0), women_years.get("senior", 0), women_years.get("graduate student", 0),
                women_positions.get("defenders", 0), women_positions.get("forwards", 0), women_positions.get("midfielders", 0), women_positions.get("goalkeepers", 0)
            ]

            writer.writerow(out_row)

if __name__ == "__main__":
    main()