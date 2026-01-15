#!/usr/bin/env python3
"""Scrape college soccer conference standings (TopDrawerSoccer D1 men example) and
update Google Sheet columns for conference_record (Y), overall_record (Z), and
conference_standing (AA).

Usage:
  export SHEET_ID=...
  python3 scrape_conference_standings.py

Interactive prompts will confirm fuzzy-matched school names before updating.

Requirements: requests, beautifulsoup4, gspread, google-auth, lxml (optional)
Place service account JSON as 'cspscraping.json' in the repo.
"""
import os
import sys
import re
import time
import requests
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
import gspread
import json
from datetime import datetime
from typing import List, Optional, Tuple, cast

# Config
CREDS_FILE = 'cspscraping.json'
SHEET_ID = os.getenv('SHEET_ID')
UNIVERSITIES_TAB = 'Universities'
# Output columns: Y=25, Z=26, AA=27 (1-indexed)
COL_CONFERENCE_RECORD = 25
COL_OVERALL_RECORD = 26
COL_CONFERENCE_STANDING = 27
# Women-specific columns: AV=48, AW=49, AX=50
WOMEN_COL_CONFERENCE_RECORD = 48
WOMEN_COL_OVERALL_RECORD = 49
WOMEN_COL_CONFERENCE_STANDING = 50

# Write limit configuration (writes per 60s window)
WRITE_LIMIT_PER_MIN = int(os.getenv('SHEETS_WRITE_LIMIT', '60'))

# Default URL(s) - D1 men TopDrawerSoccer
URLS = {
    'd1_men': 'https://www.topdrawersoccer.com/college-soccer/college-soccer-conference-standings/men',
    'd1_women': 'https://www.topdrawersoccer.com/college-soccer/college-soccer-conference-standings/women'
}

USER_AGENT = 'Mozilla/5.0 (compatible; conf-standings-scraper/1.0)'

def token_jaccard(a: str, b: str) -> float:
    a_tokens = set(re.findall(r"\w+", (a or '').lower()))
    b_tokens = set(re.findall(r"\w+", (b or '').lower()))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / len(a_tokens | b_tokens)

def combined_similarity(a: str, b: str) -> float:
    seq = SequenceMatcher(None, (a or '').lower(), (b or '').lower()).ratio()
    j = token_jaccard(a, b)
    return 0.4 * seq + 0.6 * j

def fetch_page(url: str) -> str:
    headers = {'User-Agent': USER_AGENT}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

def parse_topdrawersoccer_table(html_text: str):
    """Parse conference standings table(s). Try lxml to locate all conference table containers
    under /html/body/main/div[2]/div[1]/div[3]/div[*] and extract each table found. If lxml
    is not available or yields no results, fall back to BeautifulSoup and look for tables
    under <main> and filter ones that look like standings tables.

    Returns list of dicts: {'conference': str, 'standing': str, 'school': str, 'conf_record': str, 'overall_record': str}
    """
    # Try lxml + XPath first
    try:
        from lxml import html as lxml_html
        tree = lxml_html.fromstring(html_text)

        results = []
        # Try to find conference container divs (each conference block is a child div)
        conf_divs = tree.xpath('/html/body/main/div[2]/div[1]/div[3]/div')
        if not conf_divs:
            # fallback: try older XPaths / specific table XPaths
            conf_divs = tree.xpath('/html/body/main/div[2]/div[1]/div[3]') or tree.xpath('//*[@id="tab-0"]/ancestor::div')
        # For each conference block, find any table(s) inside and parse rows
        for block in conf_divs:
            # attempt to extract a conference title from headings inside the block
            conf_name = ''
            # Prefer the first <a> text if present (matches the provided XPath /.../div/.../a[1])
            try:
                a_texts = block.xpath('.//a[1]/text()')
                if a_texts:
                    for t in a_texts:
                        if t and t.strip():
                            conf_name = t.strip()
                            break
            except Exception:
                pass
            for htag in ('h1','h2','h3','h4'):
                nodes = block.xpath('.//%s/text()' % htag)
                if nodes:
                    # take first non-empty
                    for n in nodes:
                        if n and n.strip():
                            conf_name = n.strip()
                            break
                if conf_name:
                    break
            # As a fallback, try to find a div with a title-like class
            if not conf_name:
                nodes = block.xpath('.//div[contains(@class,"title") or contains(@class,"header")]/text()')
                if nodes:
                    for n in nodes:
                        if n and n.strip():
                            conf_name = n.strip()
                            break

            tables = block.xpath('.//table')
            for table in tables:
                rows = table.xpath('.//tr')
                for tr in rows:
                    tds = tr.xpath('./td')
                    if not tds:
                        continue
                    texts = [td.xpath('string()').strip() for td in tds]
                    # require at least school name in second column
                    if len(texts) < 2:
                        continue
                    standing = texts[0] if len(texts) > 0 else ''
                    school = texts[1] if len(texts) > 1 else ''
                    conf_record = texts[2] if len(texts) > 2 else ''
                    overall = texts[3] if len(texts) > 3 else ''
                    if not school:
                        continue
                    results.append({'conference': conf_name, 'standing': standing, 'school': school, 'conf_record': conf_record, 'overall_record': overall})
        if results:
            return results
    except Exception:
        pass

    # Fallback BeautifulSoup: find tables under <main> and pick those that look like standings
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')
    results = []
    main = soup.find('main')
    tables = []
    if main:
        # collect all table-containing blocks under the main conferences area
        # conference blocks are under main div[2]/div[1]/div[3]/div[*] - attempt to find those
        conf_blocks = main.select('div > div > div') or [main]
        for block in conf_blocks:
            # try to get a conference title
            conf_name = ''
            for tag in ('h1','h2','h3','h4'):
                h = block.find(tag)
                if h and h.get_text(strip=True):
                    conf_name = h.get_text(strip=True)
                    break
            # fallback: use first <a> text if available (matches /.../a[1])
            if not conf_name:
                a = block.find('a')
                if a and a.get_text(strip=True):
                    conf_name = a.get_text(strip=True)
            # find any tables inside this block
            for table in block.select('table'):
                parsed_any = False
                for tr in table.select('tr'):
                    tds = [td.get_text(' ', strip=True) for td in tr.select('td')]
                    if not tds or len(tds) < 2:
                        continue
                    standing = tds[0] if len(tds) > 0 else ''
                    school = tds[1] if len(tds) > 1 else ''
                    conf_record = tds[2] if len(tds) > 2 else ''
                    overall = tds[3] if len(tds) > 3 else ''
                    if not school:
                        continue
                    results.append({'conference': conf_name, 'standing': standing, 'school': school, 'conf_record': conf_record, 'overall_record': overall})
                    parsed_any = True
        return results

def load_university_list(client, sheet_id, uni_tab=UNIVERSITIES_TAB, gender='men'):
    try:
        ws = client.open_by_key(sheet_id).worksheet(uni_tab)
    except Exception as e:
        print('Failed to open Universities tab:', e)
        return [], None

    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return [], ws
    # Treat second row as header and ignore first row
    header = vals[1]
    uni_idx = 1  # column B
    # Determine conference column based on gender
    conf_idx = None
    target_header = 'men_conference' if gender == 'men' else 'women_conference'
    for i, h in enumerate(header):
        if h and h.strip().lower() == target_header:
            conf_idx = i
            break
    if conf_idx is None:
        # fallback defaults: V for men (0-based 21), AS for women (0-based 44)
        conf_idx = 21 if gender == 'men' else 44

    universities = []  # list of (row_number, name, conference)
    for r_idx, row in enumerate(vals[2:], start=3):
        name = row[uni_idx].strip() if len(row) > uni_idx else ''
        conf = row[conf_idx].strip() if len(row) > conf_idx else ''
        if name:
            universities.append((r_idx, name, conf))
    return universities, ws

def find_best_candidates(scraped_name: str, universities, top_n=10, preferred_conf=None):
    """Return top_n candidate tuples (row, name, score).

    universities: list of (row, name, conference)
    preferred_conf: optional conference name to boost candidates from that conference
    """
    scores = []
    pref_norm = (preferred_conf or '').strip().lower() if preferred_conf else None
    for (row, name, conf) in universities:
        base = combined_similarity(scraped_name, name)
        bonus = 0.0
        if pref_norm and conf and conf.strip().lower() == pref_norm:
            # boost same-conference candidates to prioritize them
            bonus = 0.15
        score = min(1.0, base + bonus)
        scores.append((row, name, score))
    scores.sort(key=lambda x: x[2], reverse=True)
    return scores[:top_n]

def interactive_confirm(scraped_name: str, candidate_list, universities=None, scraped_conf: str = ''):
    """candidate_list is list of (row, name, score) sorted desc. Returns chosen (row, name) or None.

    New behaviors:
    - 'e' or 'E' => prompt manual full university name; try to find exact match in sheet; if not found offer fuzzy matches.
    - 'p' or 'P' => prompt partial substring; filter the universities list for names containing the substring and present new closest results.
    """
    if not candidate_list:
        print('No candidates to choose from.')
        return None
    idx = 0
    while True:
        row, name, score = candidate_list[idx]
        # Show scraped school along with its conference (if available)
        if scraped_conf:
            print(f"\nScraped school: {scraped_name}  (Conference: {scraped_conf})")
        else:
            print('\nScraped school:', scraped_name)
        print(f"Suggested: {name} (score={score:.3f})")
        ans = input("Confirm? [y]es / [n]ext / s=show top 10 / e=manual / p=partial search / q=skip: ").strip().lower()
        if ans in ('y', 'yes', ''):
            return (row, name)
        if ans in ('n', 'next'):
            idx += 1
            if idx >= len(candidate_list):
                print('No more candidates in list.')
                return None
            continue
        if ans == 's':
            for i, (r, u, s) in enumerate(candidate_list, start=1):
                print(f"{i}) {u} (score={s:.3f})")
            pick = input('Choose number (or Enter to return): ')

            if pick.isdigit():
                p = int(pick)
                if 1 <= p <= len(candidate_list):
                    r, u, s = candidate_list[p-1]
                    return (r, u)
            continue
        if ans == 'e':
            # Manual full name entry
            manual = input('Type manual university name (exact as in sheet): ').strip()
            if not manual:
                continue
            # try to find exact match in universities
            if universities:
                for r, u, c in universities:
                    if u.strip().lower() == manual.strip().lower():
                        return (r, u)
                # not exact - offer fuzzy matches
                fuzzy = [(r, u, combined_similarity(manual, u)) for (r, u, _c) in universities]
                fuzzy.sort(key=lambda x: x[2], reverse=True)
                print('No exact match found; top fuzzy matches:')
                for i, (_r, u, s) in enumerate(fuzzy[:10], start=1):
                    print(f"{i}) {u} (score={s:.3f})")
                pick = input('Choose number to accept (or Enter to cancel): ').strip()
                if pick.isdigit():
                    p = int(pick)
                    if 1 <= p <= min(10, len(fuzzy)):
                        r, u, s = fuzzy[p-1]
                        return (r, u)
            print('Manual entry not found. Returning to menu.')
            continue
        if ans == 'p':
            # Partial search: prompt substring and filter universities
            substr = input('Enter partial name to search sheet for: ').strip().lower()
            if not substr:
                continue
            filtered = [(r, u, c) for (r, u, c) in (universities or []) if substr in u.lower()]
            if not filtered:
                print('No universities in sheet match that partial string.')
                continue
            # build similarity scores against scraped_name and present top results
            scored = [(r, u, combined_similarity(scraped_name, u)) for (r, u, _c) in filtered]
            scored.sort(key=lambda x: x[2], reverse=True)
            print('Top matches for partial search:')
            for i, (_r, u, s) in enumerate(scored[:10], start=1):
                print(f"{i}) {u} (score={s:.3f})")
            pick = input('Choose number to accept (or Enter to cancel): ').strip()
            if pick.isdigit():
                p = int(pick)
                if 1 <= p <= min(10, len(scored)):
                    r, u, s = scored[p-1]
                    return (r, u)
            continue
        if ans in ('q', 'skip'):
            print('Skipped.')
            return None
        print('Invalid input.')

def safe_update_cell(ws, row: int, col: int, value, progress: dict, progress_file: str) -> object:
    """Perform a single-cell write while enforcing a per-minute write limit.

    Returns True if the write was performed, False if the write failed, or the
    sentinel string 'limit' when the rate limit would be exceeded. Updates
    progress['write_timestamps'] with the write time on success and persists the progress file.
    """
    now = time.time()
    window = 60.0
    timestamps = progress.get('write_timestamps', [])
    # prune old timestamps
    timestamps = [t for t in timestamps if now - t <= window]
    if len(timestamps) >= WRITE_LIMIT_PER_MIN:
        # calculate wait until the oldest timestamp falls outside the window
        oldest = min(timestamps)
        wait = int(window - (now - oldest)) + 1
        human = time.strftime('%H:%M:%S', time.localtime(now + wait))
        print(f"Write limit would be exceeded ({WRITE_LIMIT_PER_MIN} writes per {int(window)}s).")
        print(f"Please wait ~{wait} seconds (until {human}) before retrying.")
        # return a sentinel value to indicate rate-limited outcome
        return 'limit'

    # perform the write
    try:
        ws.update_cell(row, col, value)
    except Exception as e:
        print(f"Failed to write to sheet at row {row} col {col}: {e}")
        return False

    # record timestamp and persist
    timestamps.append(now)
    progress['write_timestamps'] = timestamps
    try:
        with open(progress_file, 'w') as f:
            json.dump(progress, f)
    except Exception:
        # non-fatal
        pass
    return True

def flush_pending_writes(ws, progress: dict, progress_file: str):
    """Attempt to flush any pending writes saved in progress['pending_writes'].

    Each pending item is a dict: {'row': int, 'writes': [{'col': int, 'value': ...}, ...]}.
    Stop flushing if a rate-limit is hit and persist remaining pending items.
    """
    pending = progress.get('pending_writes', []) or []
    if not pending:
        return
    print(f"Found {len(pending)} pending write group(s); attempting to flush...")
    remaining = []
    for item in pending:
        row = item.get('row')
        writes = item.get('writes', [])
        incomplete = []
        for w in writes:
            col = w.get('col')
            val = w.get('value')
            res = safe_update_cell(ws, row, col, val, progress, progress_file)
            if res == 'limit':
                # rate-limited: save this write and the rest of this group's writes
                print('Write limit reached while flushing pending writes; will resume on next run.')
                incomplete.append({'col': col, 'value': val})
                # append remaining after current
                idx = writes.index(w)
                for rem in writes[idx+1:]:
                    incomplete.append({'col': rem.get('col'), 'value': rem.get('value')})
                break
            elif res is True:
                # success; continue
                continue
            else:
                # non-rate write failure: keep this and remaining writes to try later
                print(f"Failed to flush write for row {row} col {col}; keeping pending for manual review.")
                incomplete.append({'col': col, 'value': val})
                idx = writes.index(w)
                for rem in writes[idx+1:]:
                    incomplete.append({'col': rem.get('col'), 'value': rem.get('value')})
                break

        if incomplete:
            remaining.append({'row': row, 'writes': incomplete})
        else:
            # Build data dict in the same format as update_university_record prints
            data = {}
            for w in writes:
                c = w.get('col')
                v = w.get('value')
                if c == COL_CONFERENCE_RECORD:
                    data['conf_record'] = v
                elif c == COL_OVERALL_RECORD:
                    data['overall_record'] = v
                elif c == COL_CONFERENCE_STANDING:
                    data['standing'] = v
                else:
                    # preserve any other columns for debugging
                    data[f'col_{c}'] = v

    # persist remaining pending writes
    progress['pending_writes'] = remaining
    try:
        with open(progress_file, 'w') as f:
            json.dump(progress, f)
    except Exception:
        pass
    if remaining:
        print(f"Saved {len(remaining)} pending write group(s) to {progress_file}; they will be retried next run.")
    else:
        print('All pending writes flushed.')

def update_university_record(ws, row: int, data: dict, progress: dict, progress_file: str):
    """Update university record in sheet using safe writes that respect rate limits.

    Returns True if all writes succeeded, False if aborted due to rate limiting or write failures.
    """
    try:
        writes = [
            {'col': COL_CONFERENCE_RECORD, 'value': data.get('conf_record', '')},
            {'col': COL_OVERALL_RECORD, 'value': data.get('overall_record', '')},
            {'col': COL_CONFERENCE_STANDING, 'value': data.get('standing', '')},
        ]
        for i, w in enumerate(writes):
            res = safe_update_cell(ws, row, w['col'], w['value'], progress, progress_file)
            if res == 'limit':
                # Save remaining writes including this one into pending_writes
                remaining = [ {'col': w['col'], 'value': w['value']} ]
                for rem in writes[i+1:]:
                    remaining.append({'col': rem['col'], 'value': rem['value']})
                pending = progress.get('pending_writes', [])
                pending.append({'row': row, 'writes': remaining})
                progress['pending_writes'] = pending
                try:
                    with open(progress_file, 'w') as f:
                        json.dump(progress, f)
                except Exception:
                    pass
                print(f"Rate limit reached. Saved {len(remaining)} pending writes for row {row} and will resume next run.")
                return False
            if res is True:
                continue
            # non-rate-limited write failure -> abort and keep trying later (do not drop writes)
            print(f"Failed to write row {row} col {w['col']}. Aborting update for this row.")
            return False

        print(f"Updated row {row}: {data}")
        return True
    except Exception as e:
        print(f"Error updating row {row}: {e}")
        return False

def validate_sheet_io(creds_file=CREDS_FILE, sheet_id=SHEET_ID):
    """Validate Google Sheets read/write access.

    Steps:
    - Authorize with the provided service account JSON
    - Open the sheet by ID
    - Read cell A1 (backup)
    - Write a test timestamp to A1
    - Restore the original value

    Returns a dict with 'ok': bool and 'message'. Catches API quota errors and reports them.
    """
    result = {'ok': False, 'message': ''}
    if not sheet_id:
        result['message'] = 'SHEET_ID environment variable not set.'
        print(result['message'])
        return result
    if not os.path.exists(creds_file):
        result['message'] = f"Credentials file '{creds_file}' not found."
        print(result['message'])
        return result
    try:
        # prefer gspread.service_account for simplicity
        client = gspread.service_account(filename=creds_file)
        sh = client.open_by_key(sheet_id)
        # pick the first worksheet
        try:
            ws = sh.worksheet(UNIVERSITIES_TAB)
        except Exception:
            ws = sh.get_worksheet(0)

        # read A1 backup
        try:
            old_val = ws.acell('A1').value
        except Exception:
            old_val = None

        test_val = f"TEST-{datetime.utcnow().isoformat()}"
        print(f"Writing test value to A1: {test_val}")
        ws.update_acell('A1', test_val)
        # small pause
        time.sleep(0.5)
        # read back
        readback = ws.acell('A1').value
        if readback != test_val:
            result['message'] = f"Write/read mismatch: wrote '{test_val}' read back '{readback}'"
            # attempt to restore
            try:
                ws.update_acell('A1', old_val if old_val is not None else '')
            except Exception:
                pass
            print(result['message'])
            return result
        # restore original
        try:
            ws.update_acell('A1', old_val if old_val is not None else '')
        except Exception as e:
            result['message'] = f"Failed to restore original A1 value: {e}"
            print(result['message'])
            return result

        result['ok'] = True
        result['message'] = 'Read/write test succeeded and original value restored.'
        print(result['message'])
        return result
    except Exception as e:
        # Detect quota errors in message
        msg = str(e)
        if 'Quota exceeded' in msg or 'quota' in msg.lower() or isinstance(e, Exception):
            # include the exception text
            result['message'] = f"API error during validate: {msg}"
            print(result['message'])
            return result
        result['message'] = f"Unexpected error: {e}"
        print(result['message'])
        return result

def main():
    
    print("Starting conference standings scraper...")
    
    # allow quick test mode
    if len(sys.argv) > 1 and sys.argv[1].strip().lower() in ('test-io', 'test-gs', 'test'):
        validate_sheet_io()
        return

    # parse optional command-line args early so we can load the sheet with correct gender
    gender = 'men'
    division = 'd2'
    for a in sys.argv[1:3]:
        if not a:
            continue
        aa = a.strip().lower()
        if aa in ('men', 'women'):
            gender = aa
        elif aa in ('d1', 'd2', 'd3', 'naia'):
            division = aa

    print("Running scrape for gender='%s' division='%s'" % (gender, division))

    # allow interactive conference mapping only when user passes -conference / --conference / -c
    confirm_conference = any(arg.strip().lower() in ('-conference', '--conference', '-c') for arg in sys.argv[1:])

    target = f"{division}_{gender}"

    # Parse optional start/resume flags: --start-conference=<NAME> or -s <NAME>, and --force to reprocess
    start_conference = None
    force_reprocess = any(arg.strip().lower() in ('--force', '-f') for arg in sys.argv[1:])
    for i, a in enumerate(sys.argv[1:]):
        if a.startswith('--start-conference='):
            start_conference = a.split('=', 1)[1].strip()
        if a in ('--start-conference', '-s'):
            # take next arg if available
            if i + 2 <= len(sys.argv[1:]):
                start_conference = sys.argv[1:][i+1].strip()

    # completed_confs will be loaded from the progress file once it's read below
    completed_confs = set()

    # If women, set output columns to AV/AW/AX
    if gender == 'women':
        global COL_CONFERENCE_RECORD, COL_OVERALL_RECORD, COL_CONFERENCE_STANDING
        COL_CONFERENCE_RECORD = WOMEN_COL_CONFERENCE_RECORD
        COL_OVERALL_RECORD = WOMEN_COL_OVERALL_RECORD
        COL_CONFERENCE_STANDING = WOMEN_COL_CONFERENCE_STANDING

    # Setup Google Sheets client
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
    except Exception as e:
        print('Failed to setup Google Sheets client:', e)
        return

    print("Loading university list from sheet...")
    # Load university list from sheet using gender-aware conference column
    universities, ws_universities = load_university_list(client, SHEET_ID, gender=gender)
    if not universities:
        print('No universities found in sheet. Aborting.')
        return

    print("Checking progress file for pending writes...")
    # Progress file to remember pending writes and last processed index
    progress_file = os.path.join(os.path.dirname(__file__), '.conference_progress.json')
    try:
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    except Exception:
        progress = {}

    # Load completed conferences from progress for resume behavior
    # We'll initialize an empty set here; per-division completed lists are populated
    # after loading .division_conference_xpaths.json so we can auto-populate certain
    # division/gender combinations from that file when desired.
    completed_confs = set()

    # Attempt to flush any pending writes from previous run (rate-limit interrupted)
    flush_pending_writes(ws_universities, progress, progress_file)

    # Fetch and parse standings page
    standings = []
    if division == 'd1':
        try:
            html_text = fetch_page(URLS[target])
            standings = parse_topdrawersoccer_table(html_text) or []
            if not standings:
                print('No standings data found for D1.')
                return
        except Exception as e:
            print('Failed to fetch or parse D1 standings page:', e)
            return
    else:
        # D2/D3: we'll build standings from per-conference JSON entries below
        standings = []

    # If Division 2 or Division 3, use per-conference URLs/XPaths from .division_conference_xpaths.json
    try:
        div_xpath_file = os.path.join(os.path.dirname(__file__), '.division_conference_xpaths.json')
        if os.path.exists(div_xpath_file):
            with open(div_xpath_file, 'r', encoding='utf-8') as f:
                print('Loading conference xpaths from', div_xpath_file)
                div_xpaths = json.load(f)
        else:
            div_xpaths = {}
    except Exception as e:
        print('Failed to load division xpaths file:', e)
        div_xpaths = {}

    division_label = None
    if division == 'd2':
        division_label = 'Division 2'
    elif division == 'd3':
        division_label = 'Division 3'
    elif division == 'naia':
        division_label = 'NAIA'

    print("Checking JSON file for", division_label, "conference xpaths...")
    # For D2/D3, prefer per-conference scrapes defined in JSON; D1 remains TopDrawerSoccer only
    if division_label:
        conf_map_entries = div_xpaths.get(gender, {}).get(division_label, {}) if div_xpaths else {}
        print(f"Found {len(conf_map_entries)} conference entries for {gender} {division_label} in .division_conference_xpaths.json.")
        per_conf_entries = []
        processed_conferences = set()
        # Diagnose when no usable per-conference entries exist
        if not conf_map_entries:
            print(f"No per-conference entries found in .division_conference_xpaths.json for {gender} {division_label}.")
            print("You can add entries using: python3 scrape_conference_standings.py add-confs")
        else:
            usable = [c for c,info in conf_map_entries.items() if (info.get('url') or '').strip() and (info.get('xpath') or '').strip()]
            if not usable:
                print(f"Found {len(conf_map_entries)} conference entries for {gender} {division_label}, but none have both url and xpath set.")
                print('Entries found (missing url/xpath marked):')
                for c, info in conf_map_entries.items():
                    has_url = bool((info.get('url') or '').strip())
                    has_xpath = bool((info.get('xpath') or '').strip())
                    status = []
                    if has_url: status.append('url')
                    if has_xpath: status.append('xpath')
                    if not status:
                        status = ['(no url/xpath)']
                    print(f" - {c}: {', '.join(status)}")
                print("To populate entries automatically from the sheet, run: python3 scrape_conference_standings.py add-confs")
        
        if conf_map_entries:
            print("Beginning per-conference scraping...")
            # cache of column mappings per URL or per conference
            seen_start = False if start_conference else True
            for conf_name, info in conf_map_entries.items():
                # If a start_conference was provided, skip until we encounter it
                if not seen_start:
                    if conf_name != start_conference:
                        print(f"Skipping conference '{conf_name}' until start-conference '{start_conference}' is reached.")
                        continue
                    else:
                        print(f"Starting resume at conference '{conf_name}'")
                        seen_start = True
                # If this conference was previously completed and force_reprocess not set, skip it
                if (conf_name in completed_confs) and not force_reprocess:
                    print(f"Skipping already-completed conference '{conf_name}'. Use --force to reprocess.")
                    continue
                per_div_key = f"{division}_{gender}"
                already_done = False
                try:
                    completed_list = progress.get(per_div_key, []) if isinstance(progress, dict) else []
                    already_done = conf_name in completed_list
                except Exception:
                    completed_list = []
                    already_done = False
                if already_done and not force_reprocess:
                    print(f"Skipping already-completed conference '{conf_name}' (per {per_div_key}). Use --force to reprocess.")
                    continue
                url = (info.get('url') or '').strip()
                xpath = (info.get('xpath') or '').strip()

                print(f"\nProcessing conference '{conf_name}' with URL '{url}'")

                if not url or not xpath:
                    continue
                try:
                    txt = fetch_page(url)
                except Exception as e:
                    print(f"Failed to fetch URL for conference '{conf_name}': {e}")
                    continue

                # removed stray try: directly import and parse with lxml
                from lxml import html as lxml_html
                tree = lxml_html.fromstring(txt)
                print(f"Applying XPath '{xpath}' for conference '{conf_name}'")
                print(tree[0:100])
                nodes = tree.xpath(xpath)
                # If the xpath returned no nodes or only non-table nodes (e.g., head/body),
                # try fallbacks that search for tables under <main> or any <table>.
                def _is_non_table_node(n):
                    try:
                        return getattr(n, 'tag', '') in ('head', 'body')
                    except Exception:
                        return False
                if not nodes or all(_is_non_table_node(n) for n in nodes) or not any(getattr(n, 'tag', '') == 'table' for n in nodes):
                    alt_nodes = tree.xpath('//main//table') or tree.xpath('//table')
                    if alt_nodes:
                        print(f"XPath returned no table nodes for conference '{conf_name}'; falling back to //main//table or //table (found {len(alt_nodes)} tables).")
                        nodes = alt_nodes
                    else:
                        print(f"XPath returned no nodes for conference '{conf_name}' at {url}")
                        continue

                # Determine whether this conference page is Sidearm-powered early so header
                # detection can use the appropriate visible/th exclusion rules.
                sidearm_flag = bool(info.get('sidearm')) if ('sidearm' in info) else (xpath == '/html/body/form/main/article/div[3]/table')
                print(f"Sidearm Site: {sidearm_flag} for {conf_name}")

                # attempt to detect header and table rows
                rows = []
                header_cells = None
                header_row_idx = None
                # positions/text for visible headers (th elements we consider for col_map_indices)
                header_visible_positions = None
                header_visible = None
                for node in nodes:
                    # prefer table nodes under node
                    table_nodes = []
                    if getattr(node, 'tag', '').lower() == 'table':
                        table_nodes = [node]
                    else:
                        table_nodes = node.xpath('.//table') or []

                    if table_nodes:
                        for t in table_nodes:
                            trs = t.xpath('.//tr')
                            if not trs:
                                continue
                            # Allow the user to explicitly pick which <tr> is the header row.
                            # If a saved header_row exists in the JSON entry, use it without prompting.
                            user_header_choice = None
                            try:
                                saved_hr = info.get('header_row')
                                if isinstance(saved_hr, int) and 0 <= saved_hr < len(trs):
                                    user_header_choice = int(saved_hr)
                                    print(f"Using saved header_row={user_header_choice} for conference '{conf_name}'")
                                else:
                                    preview_n = min(3, len(trs))
                                    print(f"\nPreview of first {preview_n} rows for conference '{conf_name}':")
                                    for ri in range(preview_n):
                                        cells_preview = [c.text_content().strip() for c in trs[ri].xpath('./th|./td')]
                                        print(f" {ri}) {cells_preview}")
                                    ans = input('Enter header row index (0-based) to use, or press Enter to auto-detect: ').strip()
                                    if ans.isdigit():
                                        ui = int(ans)
                                        if 0 <= ui < len(trs):
                                            user_header_choice = ui
                                            info['header_row'] = int(ui)
                                            try:
                                                with open(div_xpath_file, 'w', encoding='utf-8') as f:
                                                    json.dump(div_xpaths, f, indent=2)
                                                print(f"Saved header_row={ui} for '{conf_name}' to {div_xpath_file}")
                                            except Exception:
                                                print('Failed to save header_row to JSON file')
                            except Exception:
                                user_header_choice = None
                            # detect header: look for th in first rows
                            # Choose the best header row among the first few rows. Prefer a row
                            # containing <th> with the largest number of non-empty header cells.
                            # If no <th> rows are found, fall back to a <td> row that looks header-like
                            # (majority alphabetic cells). If the user specified a header row, honor it.
                            best_idx = None
                            best_score = -1
                            if user_header_choice is not None:
                                best_idx = user_header_choice
                                best_score = 1
                            else:
                                for j, tr in enumerate(trs[:3]):
                                    th_nodes = tr.xpath('./th')
                                    if th_nodes:
                                        texts = [th.text_content().strip() for th in th_nodes]
                                        score = sum(1 for t in texts if t)
                                    else:
                                        td_nodes = tr.xpath('./td')
                                        texts = [td.text_content().strip() for td in td_nodes]
                                        nonempty = sum(1 for t in texts if t)
                                        alpha = sum(1 for t in texts if re.search(r'[A-Za-z]', t))
                                        score = alpha if nonempty > 0 and (alpha / nonempty) >= 0.5 else 0
                                    if score > best_score:
                                        best_score = score
                                        best_idx = j

                            if best_idx is not None and best_score > 0:
                                tr = trs[best_idx]
                                # all header texts (th or td fallback)
                                header_nodes = tr.xpath('./th|./td')
                                header_cells = [cell.text_content().strip() for cell in header_nodes]
                                # Collect data rows following the chosen header row so we have sample rows
                                rows = []
                                for data_tr in trs[best_idx+1:]:
                                    data_nodes = data_tr.xpath('./th|./td')
                                    # Align data cells with header_nodes positions. If a data row
                                    # lacks a leading <th> (common on PrestoSports), pad with ''
                                    # so indices correspond to header_nodes positions.
                                    row_cells = []
                                    for i in range(len(header_nodes)):
                                        if i < len(data_nodes):
                                            row_cells.append(data_nodes[i].text_content().strip())
                                        else:
                                            row_cells.append('')
                                    # skip empty rows
                                    if any(c for c in row_cells):
                                        rows.append(row_cells)
                                # Determine visible header positions among all header nodes. Prefer
                                # nodes with class 'hide-on-medium-down' when present (Sidearm).
                                visible_positions = []
                                visible_texts = []
                                for pos, node in enumerate(header_nodes):
                                    # Only th nodes carry the classes we care about
                                    try:
                                        tag = getattr(node, 'tag', '')
                                    except Exception:
                                        tag = ''
                                    cls = (node.get('class') or '') if tag else ''
                                    if 'hide-on-medium-down' in cls:
                                        visible_positions.append(pos)
                                        visible_texts.append(node.text_content().strip())

                                # If none explicitly marked, decide behavior based on Sidearm flag.
                                if not visible_positions:
                                    if sidearm_flag:
                                        # Sidearm: exclude nodes with 'hide-on-large' class
                                        for pos, node in enumerate(header_nodes):
                                            cls = (node.get('class') or '')
                                            if 'hide-on-large' in cls:
                                                continue
                                            visible_positions.append(pos)
                                            visible_texts.append(node.text_content().strip())
                                    else:
                                        # Non-Sidearm: treat all header positions as visible
                                        for pos, node in enumerate(header_nodes):
                                            visible_positions.append(pos)
                                            visible_texts.append(node.text_content().strip())

                                print("Visible Headers:", visible_texts)
                                header_visible_positions = visible_positions
                                header_visible = visible_texts
                                header_row_idx = best_idx
                                break
                        # If we found header_cells, break out of the outer nodes loop as well
                        if header_cells is not None:
                            break
                    else:
                        # node is container: find tr elements
                        trs = node.xpath('.//tr')
                        if not trs:
                            continue
                        # detect header similarly
                        for i, tr in enumerate(trs[:3]):
                            ths = tr.xpath('.//th')
                            if ths:
                                header_cells = [th.text_content().strip() for th in tr.xpath('./th|./td')]
                                header_row_idx = i
                                break
                        if header_cells is None:
                            first_texts = [td.text_content().strip() for td in trs[0].xpath('./td')]
                            if first_texts and any(re.search(r'[A-Za-z]', t) for t in first_texts):
                                header_cells = first_texts
                                header_row_idx = 0
                        data_trs = trs[(header_row_idx + 1) if header_row_idx is not None else 1:]
                        for tr in data_trs:
                            data_nodes = tr.xpath('./th|./td')
                            # If header_cells exists, align data to its length
                            if header_cells:
                                row_cells = []
                                for i in range(len(header_cells)):
                                    if i < len(data_nodes):
                                        row_cells.append(data_nodes[i].text_content().strip())
                                    else:
                                        row_cells.append('')
                            else:
                                row_cells = [n.text_content().strip() for n in data_nodes]
                            rows.append(row_cells)

                # If header_cells not detected, try to infer by length from first row
                if header_cells is None and rows:
                    # create generic headers: col1, col2, ...
                    maxlen = max(len(r) for r in rows)
                    header_cells = [f'col{idx+1}' for idx in range(maxlen)]
                    
                # Build or retrieve column mapping for this conference
                hdr = header_cells or []

                # Map visible indices (user-provided) to actual header positions using header_visible_positions
                # Initialize col_map from existing info to avoid UnboundLocalError
                col_map = info.get('col_map') or None
                raw_col_indices = info.get('col_map_indices') or {}
                # Whether the mapping indices came from an explicit source (JSON or manual prompt).
                explicit_indices_provided = bool(raw_col_indices)
                mapped_indices = {}

                # Ensure header_positions_for_visible is defined for mapping raw indices
                header_positions_for_visible = None
                if sidearm_flag:
                    header_positions_for_visible = header_visible_positions or list(range(len(hdr)))
                else:
                    header_positions_for_visible = list(range(len(hdr)))

                # If this is a non-Sidearm site, prefer a simple manual mapping of true
                # 0-based header indices: present the headers and ask the user to enter
                # the column indices to use. Persist the choice into info so it is
                # reused on subsequent runs.
                if not sidearm_flag and header_cells:
                    print(f"\nNon-Sidearm site detected for '{conf_name}'. Please map columns by true 0-based header index:")
                    for i_h, hname in enumerate(hdr):
                        print(f" {i_h}) {hname}")

                    # Special-case NAIA Prestosports pages: team name is often in a
                    # leading <th> while other columns are <td>. Users commonly think
                    # in terms of data columns where 0=team, 1=first-td, 2=second-td, etc.
                    naia_flag = False
                    try:
                        if url and 'naiastats.prestosports.com' in url:
                            naia_flag = True
                    except Exception:
                        naia_flag = False

                    # Build list of header node tags if available to support NAIA mapping
                    header_node_tags = []
                    try:
                        # attempt to import lxml for any potential re-parsing if needed
                        from lxml import html as lxml_html  # noqa: F401
                    except Exception:
                        pass

                    # Compute td-only positions among hdr when possible
                    td_positions = []
                    try:
                        # if header_nodes variable available (from xpath parsing), use it
                        if 'header_nodes' in locals():
                            for pos, node in enumerate(header_nodes):
                                ttag = getattr(node, 'tag', '')
                                if ttag == 'td':
                                    td_positions.append(pos)
                        else:
                            # fallback: assume first header may be th and the rest are td
                            if hdr:
                                td_positions = list(range(1, len(hdr)))
                    except Exception:
                        td_positions = list(range(1, len(hdr)))

                    if naia_flag and td_positions:
                        print('\nNOTE: This appears to be an NAIA PrestoSports page. For mapping, enter indices where: 0 = team (<th>), 1 = first data column (<td>), 2 = second <td>, etc.')

                    def prompt_idx(prompt_text, allow_blank=False):
                        while True:
                            ans = input(prompt_text).strip()
                            if allow_blank and ans == '':
                                return None
                            if ans.isdigit():
                                v = int(ans)
                                # If NAIA Prestosports: interpret 0 as first header (<th>),
                                # and values >=1 as indices into the td_positions list (1 -> first td).
                                if naia_flag and td_positions:
                                    if v == 0:
                                        return 0
                                    # map v (1-based for td) to the actual header position
                                    td_idx = v - 1
                                    if 0 <= td_idx < len(td_positions):
                                        return td_positions[td_idx]
                                else:
                                    if 0 <= v < len(hdr):
                                        return v
                            print(f"Enter a number between 0 and {len(hdr)-1}{', or blank' if allow_blank else ''}.")

                    s_idx = prompt_idx(' School column index (0-based): ')
                    if s_idx is None:
                        print('School column is required for manual mapping. Skipping this conference.')
                        continue
                    cr_idx = prompt_idx(' Conference-record column index (0-based, or blank to skip): ', allow_blank=True)
                    or_idx = prompt_idx(' Overall-record column index (0-based, or blank to skip): ', allow_blank=True)
                    st_idx = prompt_idx(' Standing column index (0-based, or blank to use row number): ', allow_blank=True)

                    # Build col_map using actual hdr names and persist indices
                    col_map = {}
                    col_map[hdr[s_idx]] = 'school'
                    if cr_idx is not None:
                        col_map[hdr[cr_idx]] = 'conf_record'
                    if or_idx is not None:
                        col_map[hdr[or_idx]] = 'overall_record'
                    if st_idx is not None:
                        col_map[hdr[st_idx]] = 'standing'

                    info['col_map'] = col_map
                    col_map_indices = {}
                    col_map_indices['school'] = int(s_idx)
                    if cr_idx is not None:
                        col_map_indices['conf_record'] = int(cr_idx)
                    if or_idx is not None:
                        col_map_indices['overall_record'] = int(or_idx)
                    if st_idx is not None:
                        col_map_indices['standing'] = int(st_idx)
                    info['col_map_indices'] = col_map_indices
                    try:
                        with open(div_xpath_file, 'w', encoding='utf-8') as f:
                            json.dump(div_xpaths, f, indent=2)
                    except Exception:
                        pass
                    # Also set mapped_indices so extraction uses this mapping now
                    mapped_indices = {}
                    for k, v in col_map_indices.items():
                        mapped_indices[k] = int(v)
                    explicit_indices_provided = True

                # If no pre-existing col_map, and explicit indices exist for at least the required
                # columns (and headers are available), construct a mapping from header name -> field
                # using the provided mapped indices. Persist so future runs won't prompt.
                if not col_map and hdr and mapped_indices:
                    print("Found explicit column indices for conference '%s' (visible->actual): %s" % (conf_name, mapped_indices))
                    valid_indices = {k: v for k, v in mapped_indices.items() if isinstance(v, int) and 0 <= v < len(hdr)}
                    required_keys = ('school', 'conf_record', 'overall_record')
                    if all(k in valid_indices for k in required_keys):
                        inferred = {}
                        for key in ('school', 'conf_record', 'overall_record', 'pts'):
                            idx = valid_indices.get(key)
                            if isinstance(idx, int) and 0 <= idx < len(hdr):
                                inferred[hdr[idx]] = key
                        if inferred:
                            col_map = inferred
                            info['col_map'] = col_map
                            try:
                                with open(div_xpath_file, 'w', encoding='utf-8') as f:
                                    json.dump(div_xpaths, f, indent=2)
                            except Exception:
                                pass

                # If still no col_map, attempt auto-mapping by keywords over VISIBLE headers first
                if not col_map and header_cells:
                    # build visible lists
                    visible_positions = header_visible_positions or list(range(len(hdr)))
                    visible_display = header_visible or [hdr[i] for i in visible_positions]

                    # attempt auto-mapping by keywords over visible headers first
                    auto_map = {}
                    keywords = {
                        'school': ['school', 'team', 'institution', 'university', 'college', 'teamname'],
                        'conf_record': ['conf', 'conference record', 'conf_record', 'conference_record', 'conference'],
                        'overall_record': ['overall', 'overall record', 'overall_record', 'record'],
                        'standing': ['standing', 'place', 'pos', 'position', 'rank']
                    }
                    for i_v, h in enumerate(visible_display):
                        hn = (h or '').lower()
                        mapped = None
                        for key, keys in keywords.items():
                            for k in keys:
                                if k in hn:
                                    mapped = key
                                    break
                            if mapped:
                                break
                        if mapped:
                            # map using actual hdr name for consistency
                            actual_idx = visible_positions[i_v]
                            auto_map[hdr[actual_idx]] = mapped

                    if 'school' in auto_map and ('conf_record' in auto_map or 'overall_record' in auto_map or 'standing' in auto_map):
                        col_map = auto_map
                        info['col_map'] = col_map
                        try:
                            with open(div_xpath_file, 'w', encoding='utf-8') as f:
                                json.dump(div_xpaths, f, indent=2)
                        except Exception:
                            pass
                    else:
                        # Interactive prompt: show visible headers with visible 0-based indices
                        print(f"\nDetected visible columns for conference '{conf_name}' at {url} (visible 0-based indices):")
                        for i_v, h in enumerate(visible_display):
                            print(f" {i_v}) {h}")

                        def prompt_visible_index(prompt_text, allow_blank=False):
                            while True:
                                ans = input(prompt_text).strip()
                                if allow_blank and ans == '':
                                    return None
                                if ans.isdigit():
                                    vidx = int(ans)
                                    if 0 <= vidx < len(visible_display):
                                        actual_idx = visible_positions[vidx]
                                        return vidx, actual_idx
                                print(f"Enter a number between 0 and {len(visible_display)-1}{', or blank' if allow_blank else ''}.")

                        while True:
                            print('\nEnter the visible column number for the following fields:')
                            s_res = prompt_visible_index(" School column number (visible 0-based): ")
                            cr_res = prompt_visible_index(" Conference-record column number (visible 0-based, or blank to skip): ", allow_blank=True)
                            or_res = prompt_visible_index(" Overall-record column number (visible 0-based, or blank to skip): ", allow_blank=True)
                            st_res = prompt_visible_index(" Standing column number (visible 0-based, or blank to use row number): ", allow_blank=True)

                            def _unpack(res):
                                if res is None:
                                    return (None, None)
                                return res

                            s_vidx, s_actual = _unpack(s_res)
                            cr_vidx, cr_actual = _unpack(cr_res)
                            or_vidx, or_actual = _unpack(or_res)
                            st_vidx, st_actual = _unpack(st_res)

                            chosen_actuals = [i for i in (s_actual, cr_actual, or_actual) if i is not None]
                            if st_actual is not None:
                                chosen_actuals.append(st_actual)
                            if len(chosen_actuals) != len(set(chosen_actuals)):
                                print('Duplicate column selections detected; please choose distinct columns.')
                                continue
                            if s_actual is None:
                                print('School column is required.')
                                continue
                            break

                        # Build col_map using actual hdr indices
                        col_map = {}
                        col_map[hdr[s_actual]] = 'school'
                        if cr_actual is not None:
                            col_map[hdr[cr_actual]] = 'conf_record'
                        if or_actual is not None:
                            col_map[hdr[or_actual]] = 'overall_record'
                        if st_actual is not None:
                            col_map[hdr[st_actual]] = 'standing'

                        info['col_map'] = col_map
                        # Persist visible 0-based indices into col_map_indices for future runs
                        col_map_indices = info.get('col_map_indices') or {}
                        if s_vidx is not None:
                            col_map_indices['school'] = int(s_vidx)
                        if cr_vidx is not None:
                            col_map_indices['conf_record'] = int(cr_vidx)
                        if or_vidx is not None:
                            col_map_indices['overall_record'] = int(or_vidx)
                        if st_vidx is not None:
                            col_map_indices['standing'] = int(st_vidx)
                        info['col_map_indices'] = col_map_indices
                        try:
                            with open(div_xpath_file, 'w', encoding='utf-8') as f:
                                json.dump(div_xpaths, f, indent=2)
                        except Exception:
                            pass

                # Ensure we have explicit col_map_indices when possible so duplicate header names
                # don't cause ambiguous mappings. If col_map exists but col_map_indices is empty,
                # try to resolve header-name -> index mapping. Prompt user only when a header
                # name occurs multiple times so we can disambiguate.
                try:
                    existing_indices = info.get('col_map_indices') or {}
                    if (not existing_indices) and col_map and hdr:
                        computed_indices = {}
                        for hname, field in list(col_map.items()):
                            # find all header positions matching this header name (case-insensitive)
                            matches = [i for i, h in enumerate(hdr) if (h or '').strip().lower() == (hname or '').strip().lower()]
                            if len(matches) == 1:
                                computed_indices[field] = matches[0]
                            elif len(matches) > 1:
                                # Ask user to disambiguate which occurrence to use
                                print(f"Header name '{hname}' appears multiple times for conference '{conf_name}':")
                                for m in matches:
                                    print(f" {m}) {hdr[m]}")
                                ans = None
                                while True:
                                    pick = input(f"Choose 0-based index for field '{field}' (or Enter to skip mapping this field): ").strip()
                                    if pick == '':
                                        break
                                    if pick.isdigit() and int(pick) in matches:
                                        ans = int(pick)
                                        break
                                    print(f"Enter one of: {matches}, or blank to skip.")
                                if ans is not None:
                                    computed_indices[field] = ans
                        # If we found any indices, persist them into info and save JSON
                        if computed_indices:
                            # merge into existing structure
                            col_map_indices = info.get('col_map_indices') or {}
                            col_map_indices.update(computed_indices)
                            info['col_map_indices'] = col_map_indices
                            try:
                                with open(div_xpath_file, 'w', encoding='utf-8') as f:
                                    json.dump(div_xpaths, f, indent=2)
                                print(f"Saved computed col_map_indices for '{conf_name}' to {div_xpath_file}: {computed_indices}")
                            except Exception:
                                print('Failed to persist computed col_map_indices to JSON file')
                except Exception:
                    pass

                # Detect PTS/Points column index if present in headers
                pts_col_idx = None
                # prefer explicit mapped_indices (actual hdr indices)
                if isinstance(mapped_indices, dict) and 'pts' in mapped_indices:
                    pidx = mapped_indices.get('pts')
                    if isinstance(pidx, int) and 0 <= pidx < len(hdr):
                        pts_col_idx = pidx
                if pts_col_idx is None:
                    for i_h, hname in enumerate(hdr):
                        if hname and re.search(r"\b(c?pts?|points?)\b", hname, flags=re.IGNORECASE):
                            pts_col_idx = i_h
                            break

                # Prepare header usage counters to support duplicate header names mapping to multiple fields
                header_usage = {}
                conf_entries = []

                # Normalize name-based col_map keys once
                raw_col_names = info.get('col_map_names') or info.get('col_map') or {}
                col_names_norm = {}
                try:
                    for k, v in raw_col_names.items():
                        nk = re.sub(r"[\[\(].*?[\]\)]", "", (k or ''))
                        nk = re.sub(r"\s+", " ", nk).strip().lower()
                        col_names_norm[nk] = v
                except Exception:
                    col_names_norm = {}

                # visible positions fallback
                visible_positions = header_visible_positions or list(range(len(hdr)))

                # mapped_indices previously computed map visible indices->actual hdr indices
                effective_indices = mapped_indices if isinstance(mapped_indices, dict) and mapped_indices else {}

                for ridx, cells in enumerate(rows, start=1):
                    rowvals = {}
                    # Apply explicit indices first (these are actual hdr indices).
                    # If the extracted value doesn't look like the expected type (e.g. a
                    # school name should contain letters), try to find a better column
                    # in the same row. This helps when header th/td counts don't align
                    # exactly with data td counts.
                    if effective_indices:
                        for field, actual_idx in effective_indices.items():
                            try:
                                if not (isinstance(actual_idx, int) and 0 <= actual_idx < len(cells)):
                                    continue
                                val = cells[actual_idx].strip()
                                if explicit_indices_provided:
                                    # Use explicit indices verbatim when provided by JSON or manual input.
                                    rowvals[field] = val
                                    continue
                                # Basic validation heuristics when indices are not explicit
                                # (allow auto-correction if the value doesn't match expectations).
                                if field == 'school':
                                    if not re.search(r'[A-Za-z]', val):
                                        # find first column with alphabetic content
                                        found = False
                                        for j, c in enumerate(cells):
                                            if re.search(r'[A-Za-z]', c):
                                                val = c.strip()
                                                actual_idx = j
                                                found = True
                                                break
                                        if not found:
                                            pass
                                elif field in ('conf_record', 'overall_record'):
                                    # expect patterns like '9-1-2' or '9-1'
                                    if not re.search(r'\d+\s*[-–]\s*\d+', val):
                                        # try to find a nearby column that looks like a record
                                        found = False
                                        for j, c in enumerate(cells):
                                            if re.search(r'\d+\s*[-–]\s*\d+', c):
                                                val = c.strip()
                                                actual_idx = j
                                                found = True
                                                break
                                        if not found:
                                            pass
                                # assign validated/adjusted value
                                rowvals[field] = val
                            except Exception:
                                pass

                    # Name-based mapping: only consider visible header positions
                    for vp in visible_positions:
                        if vp < 0 or vp >= len(hdr):
                            continue
                        header_name = hdr[vp] or ''
                        val = cells[vp] if vp < len(cells) else ''
                        src_norm = re.sub(r"[\[\(].*?[\]\)]", "", header_name).strip().lower()
                        mapped = col_names_norm.get(src_norm)
                        if mapped is None:
                            # fallback to legacy exact header key lookup
                            mapped = (info.get('col_map') or {}).get(header_name)
                        if not mapped:
                            continue
                        if isinstance(mapped, list):
                            used = header_usage.get(src_norm, 0)
                            if used < len(mapped):
                                target = mapped[used]
                            else:
                                target = mapped[-1]
                            header_usage[src_norm] = used + 1
                            if target and target not in rowvals:
                                rowvals[target] = val.strip()
                        else:
                            if mapped not in rowvals:
                                rowvals[mapped] = val.strip()

                    # capture pts value: prefer explicit effective_indices then detected pts_col_idx
                    pts_val = None
                    if isinstance(effective_indices, dict) and 'pts' in effective_indices:
                        pidx = effective_indices.get('pts')
                        if isinstance(pidx, int) and pidx < len(cells):
                            raw_pts = cells[pidx]
                            m = re.search(r"([-+]?[0-9]*\.?[0-9]+)", raw_pts or '')
                            if m:
                                try:
                                    pts_val = float(m.group(1))
                                except Exception:
                                    pts_val = None
                    elif pts_col_idx is not None and pts_col_idx < len(cells):
                        raw_pts = cells[pts_col_idx]
                        m = re.search(r"([-+]?[0-9]*\.?[0-9]+)", raw_pts or '')
                        if m:
                            try:
                                pts_val = float(m.group(1))
                            except Exception:
                                pts_val = None
                    if pts_val is not None:
                        rowvals['pts'] = pts_val

                    # ensure school present: prefer explicit 'school', then first VISIBLE column, then first column overall
                    school = rowvals.get('school')
                    if not school:
                        if visible_positions:
                            first_vis = visible_positions[0]
                            if first_vis < len(cells):
                                school = cells[first_vis]
                        if not school and len(cells) > 0:
                            school = cells[0]
                    school = re.sub(r"[\[\(].*?[\]\)]", "", (school or '')).replace('*', '').strip()
                    if school:
                        rowvals['school'] = school

                    conf_record = rowvals.get('conf_record', '')
                    overall = rowvals.get('overall_record', '')
                    standing = rowvals.get('standing')
                    if not standing:
                        standing = str(ridx)

                    if not rowvals.get('school'):
                        continue

                    entry_obj = {'conference': conf_name, 'standing': standing, 'school': rowvals.get('school'), 'conf_record': conf_record, 'overall_record': overall}
                    if 'pts' in rowvals:
                        entry_obj['pts'] = rowvals['pts']
                    conf_entries.append(entry_obj)

                # If a PTS column was detected and entries have pts, compute standings by sorting by pts desc
                try:
                    if any('pts' in e for e in conf_entries):
                        indexed = list(enumerate(conf_entries))
                        # sort by pts desc, then original order as tiebreaker
                        sorted_by_pts = sorted(indexed, key=lambda x: (-(x[1].get('pts') or 0.0), x[0]))
                        for rank, (orig_i, _e) in enumerate(sorted_by_pts, start=1):
                            conf_entries[orig_i]['standing'] = str(rank)
                        print(f"Assigned standings for '{conf_name}' by PTS column (desc).")
                except Exception:
                    pass

                # Present assumed mappings for the conference (auto top-1) and allow batch review
                if not conf_entries:
                    continue

                assumed = []  # list of (row, name, score) or None
                for entry in conf_entries:
                    sname = entry.get('school')
                    cands = find_best_candidates(sname, universities, top_n=1, preferred_conf=conf_name)
                    if cands:
                        assumed.append(cands[0])
                    else:
                        assumed.append(None)

                print(f"\nAssumed mappings for conference '{conf_name}':")
                for i, (entry, asp) in enumerate(zip(conf_entries, assumed), start=1):
                    s = entry.get('school')
                    conf_rec = entry.get('conf_record','') or ''
                    overall = entry.get('overall_record','') or ''
                    standing = entry.get('standing','') or ''
                    if asp:
                        r, mname, score = asp
                        print(f" {i}) {s} ^ -> {mname} (score={score:.3f})  [{conf_rec} / {overall} / standing {standing}]")
                    else:
                        print(f" {i}) {s} ^ -> (no candidate)  [{conf_rec} / {overall} / standing {standing}]")

                sel = input('Enter comma-separated numbers to FIX (or Enter to accept all, or "a" to accept all): ').strip()
                to_fix = set()
                if sel and sel.lower() != 'a':
                    parts = [p for p in re.split(r"[,\s]+", sel) if p]
                    for p in parts:
                        if '-' in p:
                            a, b = p.split('-', 1)
                            if a.isdigit() and b.isdigit():
                                for n in range(int(a), int(b) + 1):
                                    if 1 <= n <= len(conf_entries):
                                        to_fix.add(n-1)
                        elif p.isdigit():
                            n = int(p)
                            if 1 <= n <= len(conf_entries):
                                to_fix.add(n-1)

                # Prepare chosen_mappings: for entries not in to_fix, accept assumed; for entries in to_fix (or where no assumed candidate), run interactive_confirm
                chosen_mappings = {}
                for idx_e, entry in enumerate(conf_entries):
                    sname = entry.get('school')
                    # Decide whether to prompt for this entry:
                    # - If the user entered 'a' or blank, accept assumed mapping when available.
                    # - If the user specified particular indices (to_fix), prompt only those indices.
                    # - Always prompt when there is no assumed candidate.
                    if assumed[idx_e] is None:
                        # no auto candidate -> prompt
                        cands = find_best_candidates(sname, universities, top_n=10, preferred_conf=conf_name)
                        mapping = interactive_confirm(sname, cands, universities, scraped_conf=conf_name)
                        if mapping:
                            chosen_mappings[idx_e] = mapping
                        else:
                            print(f"No mapping confirmed for '{sname}'; this team will be skipped.")
                        continue

                    # At this point we have an assumed candidate. If the user asked to fix specific indices,
                    # only prompt those; otherwise accept the assumed mapping.
                    if idx_e in to_fix:
                        cands = find_best_candidates(sname, universities, top_n=10, preferred_conf=conf_name)
                        mapping = interactive_confirm(sname, cands, universities, scraped_conf=conf_name)
                        if mapping:
                            chosen_mappings[idx_e] = mapping
                        else:
                            print(f"No mapping confirmed for '{sname}'; this team will be skipped.")
                    else:
                        # accept assumed mapping
                        chosen_mappings[idx_e] = (assumed[idx_e][0], assumed[idx_e][1])

                # Perform updates for all mapped entries for this conference
                for idx_e, entry in enumerate(conf_entries):
                    mapping = chosen_mappings.get(idx_e)
                    if not mapping:
                        continue
                    target_row, matched_name = mapping
                    update_data = {
                        'conf_record': '' if entry.get('conf_record') is None else str(entry.get('conf_record')),
                        'overall_record': '' if entry.get('overall_record') is None else str(entry.get('overall_record')),
                        'standing': '' if entry.get('standing') is None else str(entry.get('standing')),
                    }
                    ok = update_university_record(ws_universities, target_row, update_data, progress, progress_file)
                    if not ok:
                        print(f"Update for row {target_row} failed or was deferred (rate limit).")
                    time.sleep(1)

                # mark this conference as completed in progress
                completed_confs.add(conf_name)
                progress['completed_conferences'] = list(completed_confs)
                try:
                    with open(progress_file, 'w') as f:
                        json.dump(progress, f)
                except Exception:
                    pass
    # If we have no standings at this point, something went wrong
    if not standings:
        print('No standings data available after parsing. Aborting.')
        return

    # --- Begin interactive mapping of scraped standings to sheet rows ---
    print('\nBeginning interactive mapping of scraped standings to sheet...')
    for idx, entry in enumerate(standings, start=1):
        # periodic progress output
        if idx > 1 and idx % 10 == 0:
            print(f"Processed {idx} scraped entries...")

        conf_name = (entry.get('conference') or '').strip()
        scraped_school = (entry.get('school') or '').strip()
        scraped_conf_record = entry.get('conf_record', '')
        scraped_overall_record = entry.get('overall_record', '')
        scraped_standing = entry.get('standing', '')

        if not scraped_school:
            print(f"Skipping empty school entry at index {idx}.")
            continue

        print(f"\nScraped: '{scraped_school}' (Conference: '{conf_name}') -> {scraped_conf_record} / {scraped_overall_record} / standing {scraped_standing}")

        # Find candidate matches from sheet, preferring same conference when available
        candidates = find_best_candidates(scraped_school, universities, top_n=10, preferred_conf=conf_name)
        if not candidates:
            print(f"No candidates found in sheet for '{scraped_school}'.")
            # allow manual entry via interactive_confirm by passing empty list to show message
            chosen = interactive_confirm(scraped_school, [], universities, scraped_conf=conf_name)
        else:
            # Present filtered candidates (show top ones); allow interactive confirmation
            chosen = interactive_confirm(scraped_school, candidates, universities, scraped_conf=conf_name)

        if not chosen:
            print(f"No mapping confirmed for '{scraped_school}'; skipping update.")
            continue

        target_row, matched_name = chosen
        print(f"Confirmed mapping: '{scraped_school}' -> sheet row {target_row}: {matched_name}")

        # Prepare update payload using the standard column names (coerce to strings)
        update_data = {
            'conf_record': '' if scraped_conf_record is None else str(scraped_conf_record),
            'overall_record': '' if scraped_overall_record is None else str(scraped_overall_record),
            'standing': '' if scraped_standing is None else str(scraped_standing),
        }
        print(f"Writing to row {target_row}: {update_data}")

        # Perform the update (this will handle rate limits and pending writes)
        ok = update_university_record(ws_universities, target_row, update_data, progress, progress_file)
        if not ok:
            print(f"Update for row {target_row} failed or was deferred (rate limit).")
        # small pause to be polite
        time.sleep(1)

    print('\nInteractive mapping complete.')

# Helper: from a <tr> element, extract all th texts and the subset of "visible" ths
# (those with class 'hide-on-medium-down'). Returns (all_texts, visible_texts, visible_positions)
def _extract_visible_headers_from_tr(tr):
    try:
        ths = tr.xpath('./th')
    except Exception:
        return [], [], []
    all_texts = [th.text_content().strip() for th in ths]
    visible_positions = []
    visible_texts = []
    for pos, th in enumerate(ths):
        cls = (th.get('class') or '')
        if 'hide-on-medium-down' in cls:
            visible_positions.append(pos)
            visible_texts.append(th.text_content().strip())
    # If none explicitly marked, exclude ones with 'hide-on-large'
    if not visible_positions:
        for pos, th in enumerate(ths):
            cls = (th.get('class') or '')
            if 'hide-on-large' in cls:
                continue
            visible_positions.append(pos)
            visible_texts.append(th.text_content().strip())
    return all_texts, visible_texts, visible_positions

if __name__ == "__main__":
    main()