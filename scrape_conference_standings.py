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
            print(f"Updated row {row}: {data}")

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


def gather_division_conference_xpaths():
    """Gather standings URL and XPath for Division 2 and Division 3 conferences from the sheet.

    This function reads the `Universities` tab and extracts distinct conference
    names where the division column equals 'Division 2' or 'Division 3' for both
    men and women (columns expected: 'men_ncaa_division', 'men_conference',
    'women_ncaa_division', 'women_conference'). For each distinct conference it
    prompts the user to enter the standings page URL and the full XPath to the
    conference container/table. Results are saved to
    '.division_conference_xpaths.json'.
    """
    print('\nGather Division 2 and Division 3 conference standings URLs and XPaths from sheet')
    if not SHEET_ID:
        print('SHEET_ID not set. Set SHEET_ID environment variable and retry.')
        return {}

    # Authorize and open the sheet
    try:
        client = gspread.service_account(filename=CREDS_FILE)
        sh = client.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet(UNIVERSITIES_TAB)
        except Exception:
            ws = sh.get_worksheet(0)
    except Exception as e:
        print('Failed to open Google Sheet:', e)
        return {}

    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        print('Universities tab appears empty or malformed.')
        return {}

    header = [h.strip().lower() for h in vals[1]]

    def find_col(names):
        for n in names:
            if n in header:
                return header.index(n)
        return None

    men_div_idx = find_col(['men_ncaa_division', 'men_division', 'men_ncaa_div'])
    men_conf_idx = find_col(['men_conference', 'men_conf'])
    women_div_idx = find_col(['women_ncaa_division', 'women_division', 'women_ncaa_div'])
    women_conf_idx = find_col(['women_conference', 'women_conf'])

    if men_div_idx is None or men_conf_idx is None:
        print('Warning: could not find men division/conference columns by expected names; men gathering may be incomplete.')
    if women_div_idx is None or women_conf_idx is None:
        print('Warning: could not find women division/conference columns by expected names; women gathering may be incomplete.')

    # Collect conferences per gender/division
    target_divs = ['Division 2', 'Division 3']
    mappings = {'men': {}, 'women': {}}
    for g in ('men', 'women'):
        for d in target_divs:
            mappings[g].setdefault(d, {})

    for row in vals[2:]:
        # men
        if men_div_idx is not None and men_conf_idx is not None and len(row) > max(men_div_idx, men_conf_idx):
            div = row[men_div_idx].strip()
            conf = row[men_conf_idx].strip()
            if div in target_divs and conf:
                mappings['men'].setdefault(div, {})
                mappings['men'][div].setdefault(conf, None)
        # women
        if women_div_idx is not None and women_conf_idx is not None and len(row) > max(women_div_idx, women_conf_idx):
            div = row[women_div_idx].strip()
            conf = row[women_conf_idx].strip()
            if div in target_divs and conf:
                mappings['women'].setdefault(div, {})
                mappings['women'][div].setdefault(conf, None)

    # Now prompt the user for URL and XPath for each discovered conference
    # Non-interactive mode: load existing JSON (if any) and add missing entries.
    out_file = os.path.join(os.path.dirname(__file__), '.division_conference_xpaths.json')
    try:
        with open(out_file, 'r') as f:
            existing = json.load(f)
    except Exception:
        existing = {}

    # Ensure top-level structure
    existing.setdefault('men', {})
    existing.setdefault('women', {})

    for g in ('men', 'women'):
        for div in target_divs:
            confs = sorted(mappings[g].get(div, {}).keys())
            if not confs:
                continue
            print(f"Found {len(confs)} {g.upper()} conferences in {div}.")
            # ensure division exists in existing mapping
            existing[g].setdefault(div, {})
            for conf in confs:
                # Decide whether to add: Only add for men's Division 3 and women's Division 2/3
                if g == 'men' and div == 'Division 2':
                    # preserve existing men's Division 2 entries; do not add or overwrite
                    if conf in existing['men'].get('Division 2', {}):
                        # keep existing
                        mappings[g][div][conf] = existing['men']['Division 2'][conf]
                    else:
                        # user indicated men's D2 already filled; skip adding empty
                        print(f"Skipping add for men's Division 2 conference '{conf}' (preserving existing).")
                        mappings[g][div].pop(conf, None)
                    continue

                # For other target groups (men D3, women D2/D3), add empty entry if not present
                existing_entry = existing.get(g, {}).get(div, {}).get(conf)
                if existing_entry:
                    mappings[g][div][conf] = existing_entry
                else:
                    mappings[g][div][conf] = {'url': '', 'xpath': ''}

    # Persist results
    # Write file atomically and verify
    try:
        import tempfile
        dirpath = os.path.dirname(out_file) or '.'
        with tempfile.NamedTemporaryFile('w', dir=dirpath, delete=False, encoding='utf-8') as tf:
            json.dump(existing, tf, indent=2)
            tempname = tf.name
        os.replace(tempname, out_file)
        print(f"Wrote {added} new conference entries to {out_file}")
    except Exception as e:
        print(f"Failed to write to {out_file}: {e}")

    # Reload to confirm
    try:
        with open(out_file, 'r', encoding='utf-8') as f:
            reloaded = json.load(f)
    except Exception as e:
        print('Failed to reload written file:', e)
        reloaded = existing

    # Print summary of presence
    if men_d3_confs:
        print(f"\nMen Division 3 conferences added/seen: {len(men_d3_confs)}")
        for c in sorted(men_d3_confs):
            present = 'present' if c in reloaded.get('men', {}).get('Division 3', {}) else 'MISSING'
            print(' -', c, '->', present)
    for div in target_women_divs:
        if women_confs.get(div):
            print(f"\nWomen {div} conferences added/seen: {len(women_confs[div])}")
            for c in sorted(women_confs[div]):
                present = 'present' if c in reloaded.get('women', {}).get(div, {}) else 'MISSING'
                print(' -', c, '->', present)

    return existing


def add_conferences_from_sheet():
    """Read distinct conferences from the sheet and add empty url/xpath entries for:
    - men's Division 3
    - women's Division 2
    - women's Division 3

    Do not modify men's Division 2 entries. Persist to .division_conference_xpaths.json.
    """
    print('\nAdding conferences from sheet for men D3 and women D2/D3 (empty url/xpath)')
    if not SHEET_ID:
        print('SHEET_ID not set. Set SHEET_ID environment variable and retry.')
        return {}

    # Authorize and open the sheet
    try:
        client = gspread.service_account(filename=CREDS_FILE)
        sh = client.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet(UNIVERSITIES_TAB)
        except Exception:
            ws = sh.get_worksheet(0)
    except Exception as e:
        print('Failed to open Google Sheet:', e)
        return {}

    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        print('Universities tab appears empty or malformed.')
        return {}

    header = [h.strip().lower() for h in vals[1]]

    def find_col(names):
        for n in names:
            if n in header:
                return header.index(n)
        return None

    men_div_idx = find_col(['men_ncaa_division', 'men_division', 'men_ncaa_div'])
    men_conf_idx = find_col(['men_conference', 'men_conf'])
    women_div_idx = find_col(['women_ncaa_division', 'women_division', 'women_ncaa_div'])
    women_conf_idx = find_col(['women_conference', 'women_conf'])

    if men_div_idx is None or men_conf_idx is None:
        print('Warning: could not find men division/conference columns by expected names; men gathering may be incomplete.')
    if women_div_idx is None or women_conf_idx is None:
        print('Warning: could not find women division/conference columns by expected names; women gathering may be incomplete.')

    target_men_div = 'Division 3'
    target_women_divs = ['Division 2', 'Division 3']

    men_d3_confs = set()
    women_confs = {d: set() for d in target_women_divs}

    for row in vals[2:]:
        if men_div_idx is not None and men_conf_idx is not None and len(row) > max(men_div_idx, men_conf_idx):
            div = row[men_div_idx].strip()
            conf = row[men_conf_idx].strip()
            if div == target_men_div and conf:
                men_d3_confs.add(conf)
        if women_div_idx is not None and women_conf_idx is not None and len(row) > max(women_div_idx, women_conf_idx):
            div = row[women_div_idx].strip()
            conf = row[women_conf_idx].strip()
            if div in target_women_divs and conf:
                women_confs[div].add(conf)

    # Load existing mappings and merge
    out_file = os.path.join(os.path.dirname(__file__), '.division_conference_xpaths.json')
    try:
        with open(out_file, 'r') as f:
            existing = json.load(f)
    except Exception:
        existing = {}

    existing.setdefault('men', {})
    existing.setdefault('women', {})
    existing['men'].setdefault('Division 2', {})
    existing['men'].setdefault('Division 3', {})
    existing['women'].setdefault('Division 2', {})
    existing['women'].setdefault('Division 3', {})

    # Add men's D3 conferences with empty url/xpath if missing
    added = 0
    for conf in sorted(men_d3_confs):
        # Always set/overwrite men's Division 3 entries to ensure they're present
        if conf in existing['men'].get('Division 3', {}):
            print(f"Overwriting existing men Division 3 entry for '{conf}' with empty url/xpath")
        else:
            print(f"Adding men Division 3 entry for '{conf}'")
        existing['men']['Division 3'][conf] = {'url': '', 'xpath': ''}
        added += 1

    # Add women's D2/D3 conferences
    for div in target_women_divs:
        for conf in sorted(women_confs.get(div, [])):
            if conf in existing['women'].get(div, {}):
                print(f"Overwriting existing women {div} entry for '{conf}' with empty url/xpath")
            else:
                print(f"Adding women {div} entry for '{conf}'")
            existing['women'][div][conf] = {'url': '', 'xpath': ''}
            added += 1

    # Write file atomically and verify
    try:
        import tempfile
        dirpath = os.path.dirname(out_file) or '.'
        with tempfile.NamedTemporaryFile('w', dir=dirpath, delete=False, encoding='utf-8') as tf:
            json.dump(existing, tf, indent=2)
            tempname = tf.name
        os.replace(tempname, out_file)
        print(f"Wrote {added} new conference entries to {out_file}")
    except Exception as e:
        print(f"Failed to write to {out_file}: {e}")

    # Reload to confirm
    try:
        with open(out_file, 'r', encoding='utf-8') as f:
            reloaded = json.load(f)
    except Exception as e:
        print('Failed to reload written file:', e)
        reloaded = existing

    # Print summary of presence
    if men_d3_confs:
        print(f"\nMen Division 3 conferences added/seen: {len(men_d3_confs)}")
        for c in sorted(men_d3_confs):
            present = 'present' if c in reloaded.get('men', {}).get('Division 3', {}) else 'MISSING'
            print(' -', c, '->', present)
    for div in target_women_divs:
        if women_confs.get(div):
            print(f"\nWomen {div} conferences added/seen: {len(women_confs[div])}")
            for c in sorted(women_confs[div]):
                present = 'present' if c in reloaded.get('women', {}).get(div, {}) else 'MISSING'
                print(' -', c, '->', present)

    return existing


def main():
    # allow quick test mode
    if len(sys.argv) > 1 and sys.argv[1].strip().lower() in ('test-io', 'test-gs', 'test'):
        validate_sheet_io()
        return

    # allow interactive gathering of D2/D3 conference xpaths
    if any(arg.strip().lower() in ('gather-xpaths', '--gather-xpaths', 'gather') for arg in sys.argv[1:]):
        gather_division_conference_xpaths()
        return

    # support a non-interactive add-confs command to add men D3 and women D2/D3 conferences
    if any(arg.strip().lower() in ('add-confs', '--add-confs') for arg in sys.argv[1:]):
        add_conferences_from_sheet()
        return

    # parse optional command-line args early so we can load the sheet with correct gender
    gender = 'men'
    division = 'd1'
    for a in sys.argv[1:3]:
        if not a:
            continue
        aa = a.strip().lower()
        if aa in ('men', 'women'):
            gender = aa
        elif aa in ('d1', 'd2', 'd3', 'naia'):
            division = aa

    # allow interactive conference mapping only when user passes -conference / --conference / -c
    confirm_conference = any(arg.strip().lower() in ('-conference', '--conference', '-c') for arg in sys.argv[1:])

    target = f"{division}_{gender}"

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

    # Load university list from sheet using gender-aware conference column
    universities, ws_universities = load_university_list(client, SHEET_ID, gender=gender)
    if not universities:
        print('No universities found in sheet. Aborting.')
        return

    # Progress file to remember pending writes and last processed index
    progress_file = os.path.join(os.path.dirname(__file__), '.conference_progress.json')
    try:
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    except Exception:
        progress = {}

    # Attempt to flush any pending writes from previous run (rate-limit interrupted)
    flush_pending_writes(ws_universities, progress, progress_file)

    # Fetch and parse standings page
    try:
        html_text = fetch_page(URLS[target])
        standings = parse_topdrawersoccer_table(html_text)
        if not standings:
            print('No standings data found.')
            return
    except Exception as e:
        print('Failed to fetch or parse standings page:', e)
        return

    page_key = target
    start_idx = int(progress.get(page_key, 0))
    print(f'Resuming from index {start_idx}')

    # --- Load conference name mappings (assume mappings already completed); do not print them ---
    # collect unique scraped conferences (preserve order)
    scraped_confs = []
    for ent in standings:
        c = (ent.get('conference') or '').strip()
        if c and c not in scraped_confs:
            scraped_confs.append(c)

    # collect unique sheet conferences (D1 conferences only) and sort alphabetically
    sheet_confs = sorted({conf for (_r, _n, conf) in universities if conf}, key=lambda s: s.lower())

    # Load existing mapping if present; do not interactively ask
    map_file = os.path.join(os.path.dirname(__file__), '.conference_map.json')
    try:
        with open(map_file, 'r') as f:
            conf_map = json.load(f)
    except Exception:
        conf_map = {}

    # Interactive conference mapping is performed for men only when the user requests it
    if gender == 'men' and confirm_conference:
         # Build display rows with suggested matches and existing mappings
         display = []
         for sc in scraped_confs:
             suggested = None
             best_score = -1.0
             for sconf in sheet_confs:
                 s = combined_similarity(sc, sconf)
                 if s > best_score:
                     best_score = s
                     suggested = sconf
             existing = conf_map.get(sc, '')
             display.append({'scraped': sc, 'suggested': suggested, 'score': best_score, 'existing': existing})

         # Print numbered list
         print('\nScraped conferences and suggested sheet mappings:')
         for i, d in enumerate(display, start=1):
             existing_note = f" [existing -> '{d['existing']}']" if d['existing'] else ''
             print(f"{i}) '{d['scraped']}' -> suggested: '{d['suggested']}' (score={d['score']:.3f}){existing_note}")

         to_edit = input("Enter comma-separated numbers to edit specific mappings (or Enter to accept all suggestions): ").strip()
         edit_indices = [int(x.strip()) for x in to_edit.split(',') if x.strip().isdigit()] if to_edit else []

         # Apply suggestions or edit selected ones
         for idx, d in enumerate(display, start=1):
             sc = d['scraped']
             if idx not in edit_indices:
                 # apply existing mapping if present, else suggested
                 if d['existing']:
                     conf_map[sc] = d['existing']
                 else:
                     conf_map[sc] = d['suggested'] or ''
                 continue

             # User chose to edit this mapping
             print(f"\nEditing mapping for scraped conference '{sc}'")
             print('Available sheet conferences (alphabetical):')
             for i, sconf in enumerate(sheet_confs, start=1):
                 print(f"{i}) {sconf}")
             ans = input("Choose number to map, 'm' to show suggestions, 'e' to enter manual, or Enter to skip: ").strip().lower()
             if ans.isdigit():
                 p = int(ans)
                 if 1 <= p <= len(sheet_confs):
                     conf_map[sc] = sheet_confs[p-1]
                     continue
             if ans == 'm':
                 # show top 10 suggestions (by similarity)
                 scored = [(sconf, combined_similarity(sc, sconf)) for sconf in sheet_confs]
                 scored.sort(key=lambda x: x[1], reverse=True)
                 for i, (sconf, s) in enumerate(scored[:10], start=1):
                     print(f"{i}) {sconf} (score={s:.3f})")
                 pick = input('Choose number to accept (or Enter to cancel): ').strip()
                 if pick.isdigit():
                     p = int(pick)
                     if 1 <= p <= min(10, len(scored)):
                         conf_map[sc] = scored[p-1][0]
                         continue
                 print('No selection made; leaving mapping empty for now.')
                 conf_map[sc] = ''
                 continue
             if ans == 'e':
                 manual = input('Type manual sheet conference name (exact as in sheet) or Enter to skip: ').strip()
                 if manual:
                     conf_map[sc] = manual
                 else:
                     conf_map[sc] = ''
                 continue
             # default: skip / leave empty
             conf_map[sc] = ''

         # Save conference mapping
         try:
             with open(map_file, 'w') as f:
                 json.dump(conf_map, f)
             print('\nSaved conference mappings to', map_file)
         except Exception as e:
             print('Warning: could not save conference mapping:', e)
    else:
        if gender == 'men' and not confirm_conference:
            print("Interactive conference mapping skipped. To enable mapping confirmation, run with the '-conference' flag.")

    # Determine start conference from command-line (optional third/fourth arg)
    start_conf = None
    if len(sys.argv) > 3:
        start_conf = sys.argv[3].strip()
    # normalize for comparisons
    start_conf_norm = (start_conf or '').strip().lower() if start_conf else None

    # Determine which conferences are already completed for this page_key
    completed = progress.get('completed_confs', {})
    # Use normalized lowercase conference names for robust comparisons
    completed_for_page = set([c.strip().lower() for c in completed.get(page_key, [])]) if completed else set()

    # If a start conference was provided, force restart from that conference by clearing
    # any completed markers for that conference and any that come after it in the
    # scraped conferences ordering.
    if start_conf_norm:
        # Find the index of the start conference in the scraped conferences list
        start_idx = None
        try:
            # 1) exact normalized match
            start_idx = next((i for i, c in enumerate(scraped_confs) if c.strip().lower() == start_conf_norm), None)
            # 2) containment (user provided shorter or longer form)
            if start_idx is None:
                start_idx = next((i for i, c in enumerate(scraped_confs) if start_conf_norm in (c or '').strip().lower() or (c or '').strip().lower() in start_conf_norm), None)
            # 3) fuzzy match fallback
            if start_idx is None:
                scores = [(i, combined_similarity(start_conf_norm, (c or '').strip().lower())) for i, c in enumerate(scraped_confs)]
                scores.sort(key=lambda x: x[1], reverse=True)
                if scores and scores[0][1] >= 0.65:
                    start_idx = scores[0][0]
        except Exception:
            start_idx = None

        if start_idx is not None:
            # Build a normalized set of conferences to clear
            to_clear_norm = set(c.strip().lower() for c in scraped_confs[start_idx:])
            old_list = completed.get(page_key, [])
            # Remove any completed entries whose normalized form is in to_clear_norm
            new_list = [c for c in old_list if c.strip().lower() not in to_clear_norm]
            if new_list != old_list:
                completed[page_key] = new_list
                progress['completed_confs'] = completed
                # persist the change immediately
                try:
                    with open(progress_file, 'w') as f:
                        json.dump(progress, f)
                    print(f"Cleared completed markers for {page_key} from '{start_conf}' onward ({len(old_list)-len(new_list)} removed)")
                except Exception as e:
                    print('Warning: failed to update progress file when clearing completed conferences:', e)
                # Recompute the in-memory completed_for_page so subsequent checks use the updated list
                completed_for_page = set([c.strip().lower() for c in completed.get(page_key, [])]) if completed else set()
        else:
            # If we couldn't locate the start conference among scraped_confs, still try to remove
            # any completed entries that match the start_conf by normalized containment.
            old_list = completed.get(page_key, [])
            new_list = [c for c in old_list if start_conf_norm not in c.strip().lower()]
            if new_list != old_list:
                completed[page_key] = new_list
                progress['completed_confs'] = completed
                try:
                    with open(progress_file, 'w') as f:
                        json.dump(progress, f)
                    print(f"Cleared completed markers for {page_key} matching '{start_conf}' ({len(old_list)-len(new_list)} removed)")
                except Exception as e:
                    print('Warning: failed to update progress file when clearing completed conferences:', e)
                completed_for_page = set([c.strip().lower() for c in completed.get(page_key, [])]) if completed else set()
            else:
                print(f"Warning: start conference '{start_conf}' not found among scraped conferences; no completed markers cleared.")

    # Group standings by conference preserving original order
    conf_groups = {}
    for idx, ent in enumerate(standings):
        conf = (ent.get('conference') or '').strip()
        conf_groups.setdefault(conf, []).append((idx, ent))

    # Process each conference group: batch suggest school mappings then allow corrections
    started = False if start_conf_norm else True
    for conf_name, entries in conf_groups.items():
        # If start_conf specified, skip until we reach it
        if start_conf_norm and not started:
            if conf_name.strip().lower() != start_conf_norm:
                print(f"Skipping conference '{conf_name}' until start conference ('{start_conf}') is reached")
                continue
            else:
                started = True

        # skip conferences already marked completed
        conf_norm = conf_name.strip().lower()
        if conf_norm in completed_for_page and not (start_conf_norm and conf_norm == start_conf_norm):
            print(f"Conference '{conf_name}' already completed for {page_key}; skipping")
            continue

        mapped_sheet_conf = conf_map.get(conf_name, '')
        print(f"\nProcessing conference: '{conf_name}' -> mapped sheet conference: '{mapped_sheet_conf}'")
        # build filtered universities list for this conference if mapping present
        if mapped_sheet_conf:
            filtered_unis = [u for u in universities if (u[2] or '').strip().lower() == mapped_sheet_conf.lower()]
        else:
            filtered_unis = universities

        # Build suggestions for all entries in this group
        suggestions = []  # list of dicts: {idx,data,row,name,score}
        for (global_idx, ent) in entries:
            school = ent['school']
            # find best candidate among filtered_unis
            cand_list = find_best_candidates(school, filtered_unis, top_n=10)
            if cand_list:
                row, name, score = cand_list[0]
                suggestions.append({'global_idx': global_idx, 'data': ent, 'row': row, 'name': name, 'score': score})
            else:
                suggestions.append({'global_idx': global_idx, 'data': ent, 'row': None, 'name': None, 'score': 0.0})

        # Print all suggestions for this conference
        print('\nSuggestions for conference', conf_name)
        for i, s in enumerate(suggestions, start=1):
            ent = s['data']
            print(f"{i}) {ent['school']}  -> {s['name'] or 'NO SUGGESTION'} (score={s['score']:.3f})")

        # Allow user to correct multiple entries by index
        to_correct = input("Enter comma-separated numbers to correct (or Enter to accept all): ").strip()
        if to_correct:
            indices = [int(x.strip()) for x in to_correct.split(',') if x.strip().isdigit()]
            for ind in indices:
                if not (1 <= ind <= len(suggestions)):
                    print(f"Index {ind} out of range")
                    continue
                s = suggestions[ind-1]
                ent = s['data']
                print(f"Correcting {ent['school']}. Current suggestion: {s['name']} (score={s['score']:.3f})")
                # Ask whether to restrict suggestions to mapped conference only
                restrict = input("Restrict suggestions to mapped conference only? [Y/n]: ").strip().lower()
                if restrict in ('', 'y', 'yes'):
                    cand_source = filtered_unis
                else:
                    cand_source = universities
                # build candidate list for this school among selected cand_source
                cand_list = find_best_candidates(ent['school'], cand_source, top_n=10)
                choice = interactive_confirm(ent['school'], cand_list, universities=cand_source, scraped_conf=mapped_sheet_conf)
                if choice:
                    chosen_row, chosen_name = choice
                    s['row'] = chosen_row
                    s['name'] = chosen_name
                else:
                    print('No selection made; leaving suggestion as-is')

        # After corrections, apply updates for each suggestion in order
        for s in suggestions:
            global_idx = s['global_idx']
            ent = s['data']
            chosen_row = s['row']
            chosen_name = s['name']
            if not chosen_row:
                print(f"Skipping {ent['school']} (no matched row)")
            else:
                ok = update_university_record(ws_universities, chosen_row, ent, progress, progress_file)
                if not ok:
                    print('Aborting further writes due to write limit. Re-run after waiting or adjust SHEETS_WRITE_LIMIT.')
                    return

            # update progress
            progress[page_key] = global_idx + 1
            # mark this conference as completed (if not already)
            completed = progress.get('completed_confs', {})
            conf_list = completed.get(page_key, [])
            if conf_name and conf_name not in conf_list:
                conf_list.append(conf_name)
                completed[page_key] = conf_list
                progress['completed_confs'] = completed
            try:
                with open(progress_file, 'w') as f:
                    json.dump(progress, f)
            except Exception as e:
                print('Warning: failed to write progress file:', e)

    print('\nAll conferences processed.')


if __name__ == '__main__':
    main()
