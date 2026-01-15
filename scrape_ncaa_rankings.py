#!/usr/bin/env python3
"""
scrape_ncaa_rankings.py

Scrapes men's soccer rankings from NCAA pages (D1 national, D2 regional, D3 regional),
fuzzy-matches the school names to the Universities sheet in your Google Sheet, prompts
for confirmation, and if accepted writes the ranking value into column AB ("men_top_25").

Usage: python3 scrape_ncaa_rankings.py

Requires:
 - a Google service account JSON credentials file named 'cspscraping.json' in the repo
 - environment variable SHEET_ID set to the Google Sheet ID

This script is interactive and will prompt you to confirm or choose matches.
"""

import os
import re
import sys
import time
import requests
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
import gspread
from urllib.parse import urljoin

# Config
CREDS_FILE = 'cspscraping.json'  # update if your credentials file has a different name
SHEET_ID = '1sZPoX0x7zJ0QCgr9G-qpXmeIqugOr9xj5WaPiSl_avU'
UNIVERSITIES_TAB = 'Universities'
# default output for men (AB -> 28)
DEFAULT_OUTPUT_COLUMN_INDEX = 28  # AB column -> 28 (1-indexed)
WOMEN_OUTPUT_COLUMN_INDEX = 51   # AY column -> 51 (1-indexed)

MEN_URLS = {
    'd1': 'https://www.ncaa.com/rankings/soccer-men/d1/united-soccer-coaches',
    'd2': 'https://www.ncaa.com/rankings/soccer-men/d2/regional-rankings',
    'd3': 'https://unitedsoccercoaches.org/rankings/college-rankings/ncaa-diii-men/',
    'naia': 'https://www.naia.org/sports/msoc/2025-26/releases/Poll_6'
}

WOMEN_URLS = {
    'd1': 'https://www.ncaa.com/rankings/soccer-women/d1/united-soccer-coaches',
    'd2': 'https://www.ncaa.com/rankings/soccer-women/d2/regional-rankings',
    'd3': 'https://unitedsoccercoaches.org/rankings/college-rankings/ncaa-diii-women/',
    'naia': 'https://www.naia.org/sports/wsoc/2025-26/releases/Poll_6'
}

USER_AGENT = 'Mozilla/5.0 (compatible; ncaa-rank-scraper/1.0)'


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


def parse_rankings_from_text(html_text: str):
    """Attempt a robust extraction of (rank, school_name) from NCAA ranking HTML.

    First try to extract rows using the provided XPath for the rankings table:
    //*[@id="block-bespin-content"]/div/article/table

    Falls back to previous heuristic text parsing when XPath/lxml isn't available or
    the table structure doesn't produce results.

    Returns list of (int(rank), school_name)
    """
    # Try lxml + XPath first (uses the XPath supplied by the user)
    try:
        from lxml import html as lxml_html
        tree = lxml_html.fromstring(html_text)
        # Try several XPaths in order until we find table rows
        xpaths_to_try = [
            '//*[@id="block-bespin-content"]/div/article/table//tr',
            '//*[@id="poll-id-14416"]//tr',
            '//*[@id="poll-id-14417"]//tr',
            '/html/body/div/div[2]/div/article/div/div/div/div[2]/div/div[1]/div[2]/div/div[2]/div[1]/table//tr',
            # NAIA table xpaths
            '//*[@id="mainbody"]/div/div[2]/div[1]/div/div[2]/table[2]//tr',
            '/html/body/div[2]/main/div/div[2]/div[1]/div/div[2]/table[2]//tr'
        ]
        tr_nodes = []
        for xp in xpaths_to_try:
            tr_nodes = tree.xpath(xp)
            if tr_nodes:
                break
        results = []
        for tr in tr_nodes:
            # get cells
            tds = tr.xpath('./td')
            # Try to detect rank and name robustly. Some tables (NAIA) place rank in td[1]
            # and school in td[3] (0-based indexing), while others use td[0] and td[1].
            rank_text = None
            name_text = None
            if len(tds) >= 2:
                # preferred: first td contains a number
                left = ''.join(td.xpath('string()') for td in [tds[0]]).strip()
                right = ''.join(td.xpath('string()') for td in [tds[1]]).strip()
                m = re.match(r"^\s*(\d{1,2})", left)
                if m:
                    rank_text = left
                    name_text = right
                else:
                    # try NAIA-style: rank in second cell, name in fourth
                    if len(tds) >= 4:
                        left2 = ''.join(td.xpath('string()') for td in [tds[1]]).strip()
                        right4 = ''.join(td.xpath('string()') for td in [tds[3]]).strip()
                        m2 = re.match(r"^\s*(\d{1,2})", left2)
                        if m2:
                            rank_text = left2
                            name_text = right4
                    # fallback: try to find any td that starts with number
                    if rank_text is None:
                        for td in tds[:3]:
                            txt = ''.join(td.xpath('string()') for td in [td]).strip()
                            if re.match(r"^\s*(\d{1,2})", txt):
                                rank_text = txt
                                break
                        # choose the next non-empty td as name
                        if rank_text:
                            for td in tds:
                                txt = ''.join(td.xpath('string()') for td in [td]).strip()
                                if txt and txt != rank_text:
                                    name_text = txt
                                    break
            if rank_text and name_text:
                try:
                    m = re.match(r"^\s*(\d{1,2})", rank_text)
                    if m:
                        rank = int(m.group(1))
                        name = name_text.strip()
                        if name:
                            results.append((rank, name))
                except Exception:
                    pass
        if results:
            # de-duplicate and sort
            seen = set()
            cleaned = []
            for rank, name in results:
                key = (rank, name.lower())
                if key in seen:
                    continue
                seen.add(key)
                cleaned.append((rank, name.strip()))
            cleaned.sort(key=lambda x: x[0])
            return cleaned
    except Exception:
        # if lxml not installed or xpath fails, fall back to BeautifulSoup heuristics below
        pass

    # Fallback: previous BeautifulSoup-based heuristics
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')

    # Strategy 1: look for ranking tables or list items
    results = []

    # Common pattern: number + team name. Search for nodes that contain a leading number.
    text = soup.get_text(separator='\n')
    # Regex: lines starting with 1 or 2 digit followed by dot or ')', then space, then name
    pattern = re.compile(r'^\s*(\d{1,2})[\.)]\s*([A-Za-z0-9&\'".\- ,/]{3,120})$', re.MULTILINE)
    for m in pattern.finditer(text):
        rank = int(m.group(1))
        name = m.group(2).strip()
        # skip lines that look like 'Rank 1' or header noise
        if len(name) < 2:
            continue
        results.append((rank, name))

    # Strategy 2: look for team-name or team elements
    if not results:
        # try common classes
        candidates = []
        for cls in ('team-name', 'team', 'school', 'rank-team'):
            for el in soup.select(f'.{cls}'):
                txt = el.get_text(strip=True)
                if txt:
                    candidates.append(txt)
        # look for sibling numbers
        if candidates:
            # try to find numbers nearby in the DOM
            for tag in soup.find_all():
                if tag.name in ('div', 'li', 'tr', 'p'):
                    t = tag.get_text(separator=' ', strip=True)
                    m = re.match(r'^(\d{1,2})[\.)]?\s+(.*)$', t)
                    if m:
                        try:
                            rank = int(m.group(1))
                            name = m.group(2).strip()
                            results.append((rank, name))
                        except Exception:
                            pass

    # Strategy 3: fallback to scanning <a> tags that look like team links
    if not results:
        for a in soup.find_all('a'):
            txt = a.get_text(strip=True)
            if txt and len(txt) > 3:
                m = re.match(r'^(?:#?)(\d{1,2})\s*[-.:)]\s*(.+)$', txt)
                if m:
                    try:
                        rank = int(m.group(1))
                        name = m.group(2).strip()
                        results.append((rank, name))
                    except Exception:
                        pass

    # De-duplicate and sort by rank
    seen = set()
    cleaned = []
    for rank, name in results:
        key = (rank, name.lower())
        if key in seen:
            continue
        seen.add(key)
        cleaned.append((rank, name))
    cleaned.sort(key=lambda x: x[0])
    return cleaned


def load_university_list(client, sheet_id, uni_tab=UNIVERSITIES_TAB, gender='men'):
    try:
        ws = client.open_by_key(sheet_id).worksheet(uni_tab)
    except Exception as e:
        print('Failed to open Universities tab:', e)
        return [], None

    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return [], ws

    # Treat the SECOND row (index 1) as the header row and ignore the first row.
    header = vals[1]

    # Enforce column B (index 1) for university_name as requested
    uni_idx = 1

    # Determine division column index based on gender
    division_idx = None
    target_header_name = 'men_ncaa_division' if gender == 'men' else 'women_ncaa_division'
    for i, h in enumerate(header):
        if h and h.strip().lower() == target_header_name:
            division_idx = i
            break
    if division_idx is None:
        # fallback defaults: U for men (0-based 20), AR for women (0-based 43)
        division_idx = 20 if gender == 'men' else 43

    universities = []  # list of (row_number, name, division)
    # Data rows start after the header (row index 2 -> spreadsheet row 3)
    for r_idx, row in enumerate(vals[2:], start=3):
        name = row[uni_idx].strip() if len(row) > uni_idx else ''
        division = row[division_idx].strip() if len(row) > division_idx else ''
        if name:
            universities.append((r_idx, name, division))
    return universities, ws


def find_sheet_row_for_uni(universities, uni_name):
    """Return the sheet row number (1-indexed) for a university name matched exactly (case-insensitive)
    within the provided universities list of (row, name, division).
    """
    target = (uni_name or '').strip().lower()
    for rownum, name, _div in universities:
        if name.strip().lower() == target:
            return rownum
    return None


def interactive_confirm(candidate, scraped_name, top_candidates=None):
    """Prompt the user to confirm the best candidate.

    candidate: (row, name, score)
    top_candidates: list of (row, name, score) sorted desc

    Returns (row, name) or None.
    """
    cand_row, cand_name, cand_score = candidate

    while True:
        print('\nScraped name:', scraped_name)
        print('Suggested match:', cand_name, f'(score={cand_score:.3f})')

        ans = input("Confirm this match? ([y]es / [n]o / s=show top 10 and pick / e=enter manual / q=skip): ").strip().lower()
        if ans in ('y', 'yes', ''):
            return (cand_row, cand_name)

        if ans == 's':
            if not top_candidates:
                print('No candidates available to show.')
                continue
            for idx, (_r, u, s) in enumerate(top_candidates[:10], start=1):
                print(f"{idx}) {u} (score={s:.3f})")
            pick = input('Choose number (or press Enter to go back): ').strip()
            if pick.isdigit():
                p = int(pick)
                if 1 <= p <= min(10, len(top_candidates)):
                    r, u, s = top_candidates[p-1]
                    return (r, u)
            # If user pressed Enter (empty), return to initial menu loop so they can choose 'e' or other options
            if pick == '':
                continue
            # any other input falls back to initial menu
            continue

        if ans == 'e':
            while True:
                manual = input('Type manual university name (exact as in sheet) or Enter to cancel: ').strip()
                if not manual:
                    break
                # try to find exact match in top_candidates first, then in full list via name
                for r, u, s in (top_candidates or []):
                    if u.strip().lower() == manual.strip().lower():
                        return (r, u)
                # otherwise ask user to confirm they want to use this manual text as-is (may not exist in sheet)
                confirm = input(f"Use '{manual}' as the sheet name to search for? [y/N]: ").strip().lower()
                if confirm in ('y', 'yes'):
                    return (None, manual)
                print('Try again or press Enter to cancel.')
            # cancelled manual entry -> go back to initial menu
            continue

        if ans in ('n', 'no'):
            if not top_candidates or len(top_candidates) < 2:
                print('No additional candidates available. Skipping.')
                return None
            # iterate starting from the second candidate
            for idx, (nxt_row, nxt_name, nxt_score) in enumerate(top_candidates[1:], start=2):
                pick_ans = input(f"Try next candidate '{nxt_name}' (score={nxt_score:.3f})? [y/n/s/e/q]: ").strip().lower()
                if pick_ans in ('y', 'yes'):
                    return (nxt_row, nxt_name)
                if pick_ans == 's':
                    # show top 10 from the provided candidates
                    for j, (_r, u, s) in enumerate(top_candidates[:10], start=1):
                        print(f"{j}) {u} (score={s:.3f})")
                    pick = input('Choose number (or press Enter to go back): ').strip()
                    if pick.isdigit():
                        p = int(pick)
                        if 1 <= p <= min(10, len(top_candidates)):
                            r, u, s = top_candidates[p-1]
                            return (r, u)
                    # If user pressed Enter, break out to the initial menu loop so they can select 'e' or otherwise
                    if pick == '':
                        break
                    # otherwise continue to next candidate
                    continue
                if pick_ans == 'e':
                    while True:
                        manual = input('Type manual university name (exact as in sheet) or Enter to cancel: ').strip()
                        if not manual:
                            break
                        for r, u, s in (top_candidates or []):
                            if u.strip().lower() == manual.strip().lower():
                                return (r, u)
                        confirm = input(f"Use '{manual}' as the sheet name to search for? [y/N]: ").strip().lower()
                        if confirm in ('y', 'yes'):
                            return (None, manual)
                        print('Try again or press Enter to cancel.')
                    # user cancelled manual entry -> break to initial menu
                    break
                if pick_ans in ('q', 'quit'):
                    print('Aborting candidate selection.')
                    return None
            # After iterating candidates, return to initial prompt (not auto-skip)
            continue

        if ans in ('q', 'quit'):
            return None

        # unknown input -> show options again
        print('Unrecognized option; please choose y, n, s, e, or q.')
        continue


def update_sheet_cell(ws, row, col, value):
    try:
        ws.update_cell(row, col, value)
        return True
    except Exception as e:
        print('Failed to update sheet cell:', e)
        return False


def main():
    # parse gender and optional division from command-line
    gender = 'men'
    division_filter = 'all'  # 'd1', 'd2', 'd3', or 'all'
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip().lower()
        if arg in ('men', 'women'):
            gender = arg
        else:
            print("Usage: python3 scrape_ncaa_rankings.py [men|women] [d1|d2|d3|naia|all]")
            sys.exit(1)
    if len(sys.argv) > 2:
        arg2 = sys.argv[2].strip().lower()
        if arg2 in ('d1', 'd2', 'd3', 'naia', 'all'):
            division_filter = arg2
        else:
            print("Usage: python3 scrape_ncaa_rankings.py [men|women] [d1|d2|d3|naia|all]")
            sys.exit(1)

    print(f"Running NCAA/NAIA rankings scraper for: {gender} divisions: {division_filter}")

    if not os.path.exists(CREDS_FILE):
        print(f"Credentials file '{CREDS_FILE}' not found. Place your service account JSON there.")
        sys.exit(1)
    if not SHEET_ID:
        print('SHEET_ID environment variable not set.')
        sys.exit(1)

    client = gspread.service_account(filename=CREDS_FILE)
    universities, ws = load_university_list(client, SHEET_ID, UNIVERSITIES_TAB, gender=gender)
    if not universities:
        print('No university names found in sheet. Aborting.')
        sys.exit(1)

    # uni column index is fixed to column B (0-based index 1)
    uni_idx = 1

    uni_names = [name for (_, name, _d) in universities]
    print(f'Loaded {len(uni_names)} universities from sheet (data starts at row 3). Using column B for university_name.')

    division_map = {'d1': 'Division 1', 'd2': 'Division 2', 'd3': 'Division 3'}

    urls = MEN_URLS if gender == 'men' else WOMEN_URLS
    output_col = DEFAULT_OUTPUT_COLUMN_INDEX if gender == 'men' else WOMEN_OUTPUT_COLUMN_INDEX

    # If a specific division was requested, restrict the urls
    if division_filter != 'all':
        urls = {division_filter: urls.get(division_filter)}

    for key, url in urls.items():
        if not url:
            print(f'No URL configured for {key}; skipping.')
            continue
        print('\nFetching rankings from', url)
        try:
            html_text = fetch_page(url)
        except Exception as e:
            print('Failed to fetch', url, e)
            continue
        rankings = parse_rankings_from_text(html_text)
        if not rankings:
            print('No rankings parsed from', url)
            continue
        print(f'Parsed {len(rankings)} ranking rows from {key} ({url})')

        target_div = division_map.get(key, '')

        # For each ranking, find best match
        for rank, scraped in rankings:
            # compute top candidates as list of (row, name, score) filtered by division
            cand_list = []
            for rownum, name, div in universities:
                if target_div and div and div.strip().lower() != target_div.strip().lower():
                    continue
                score = combined_similarity(scraped, name)
                cand_list.append((rownum, name, score))
            if not cand_list:
                print(f'No candidate universities in sheet for division {target_div}. Skipping {scraped}.')
                continue
            cand_list.sort(key=lambda x: x[2], reverse=True)
            best_row, best_name, best_score = cand_list[0]
            top_candidates = cand_list

            chosen = interactive_confirm((best_row, best_name, best_score), scraped, top_candidates)
            if not chosen:
                print('Skipping this ranking.')
                continue

            chosen_row, chosen_name = chosen
            # If chosen_row is None, the user provided a manual name; try to locate it in the sheet
            if chosen_row is None:
                # try to find exact match among universities
                found_row = None
                for rnum, nm, dv in universities:
                    if nm.strip().lower() == (chosen_name or '').strip().lower():
                        found_row = rnum
                        break
                if not found_row:
                    print(f"Manual entry '{chosen_name}' not found in sheet. Skipping update.")
                    continue
                chosen_row = found_row

            # update the output column
            val_to_write = str(rank)
            success = update_sheet_cell(ws, chosen_row, output_col, val_to_write)
            if success:
                print(f"Wrote ranking {val_to_write} to row {chosen_row}, column {output_col} for '{chosen_name}'.")
            # polite pause
            time.sleep(0.5)

    print('\nDone.')


if __name__ == '__main__':
    main()
