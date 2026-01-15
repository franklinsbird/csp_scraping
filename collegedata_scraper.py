#!/usr/bin/env python3
"""
collegedata_scraper.py

Scrapes four fields from a CollegeData page URL:
 - number of undergraduates: div.StatBlock_body__3x6Pr
 - in-state tuition: div.TitleValue_value__1JT0d (text classification)
 - out-of-state tuition: div.TitleValue_value__1JT0d (text classification)
 - average admission GPA: div.TitleValue_value__1JT0d (text classification)

Usage: python collegedata_scraper.py <url> [--out out.csv]
"""
import argparse
import csv
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Optional Google Sheets support
try:
    import gspread
except Exception:
    gspread = None

SPREADSHEET_ID = "1sZPoX0x7zJ0QCgr9G-qpXmeIqugOr9xj5WaPiSl_avU"
SHEET_NAME = "Universities"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; collegedata-scraper/1.0)"}

CURRENCY_RE = re.compile(r"\$[0-9,]+")
YEAR_TOKEN_RE = re.compile(r"\d{4}")


def clean_text(s: str) -> str:
    return " ".join(s.split()).strip()


def parse_currency(s: str):
    m = CURRENCY_RE.search(s)
    if not m:
        return None
    val = m.group(0)
    digits = re.sub(r"[^0-9]", "", val)
    try:
        return int(digits)
    except Exception:
        return None


def is_decimal_number(s: str):
    s2 = s.strip()
    return bool(re.match(r"^\d+\.\d+$(?!\d)", s2)) or bool(re.match(r"^\d\.\d+$", s2))


def extract_from_url(url: str):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Undergraduates
    undergrad = None
    ug_el = soup.find("div", class_="StatBlock_body__3x6Pr")
    if ug_el:
        t = clean_text(ug_el.get_text(" ", strip=True))
        # extract digits
        m = re.search(r"[0-9,]+", t)
        if m:
            try:
                undergrad = int(re.sub(r"[^0-9]", "", m.group(0)))
            except Exception:
                undergrad = None

    # Tuition / GPA elements (same class) - gather candidates
    in_state = None
    out_state = None
    avg_gpa = None

    elems = soup.find_all("div", class_="TitleValue_value__1JT0d")
    for el in elems:
        # Determine the title/label associated with this value element. It is often a nearby div with class TitleValue_title__2-afK
        # Search previous sibling, next sibling, then parent and one ancestor to be robust to markup variations.
        title_text = ""
        try:
            prev = el.find_previous_sibling("div", class_="TitleValue_title__2-afK")
            if prev and prev.get_text():
                title_text = clean_text(prev.get_text())
            else:
                nxt = el.find_next_sibling("div", class_="TitleValue_title__2-afK")
                if nxt and nxt.get_text():
                    title_text = clean_text(nxt.get_text())
                else:
                    parent = el.parent
                    if parent:
                        t_el = parent.find("div", class_="TitleValue_title__2-afK")
                        if t_el and t_el.get_text():
                            title_text = clean_text(t_el.get_text())
                        else:
                            gp = parent.parent if parent else None
                            if gp:
                                t_el2 = gp.find("div", class_="TitleValue_title__2-afK")
                                if t_el2 and t_el2.get_text():
                                    title_text = clean_text(t_el2.get_text())
        except Exception:
            title_text = ""

        lower_title = title_text.lower() if title_text else ""
        text = clean_text(el.get_text(" ", strip=True))
        lower = text.lower()

        # Skip Cost of Attendance entries explicitly
        if "cost of attendance" in lower_title:
            continue

        # If title indicates Tuition / Tuition and Fees, prioritize this block for tuition parsing
        if "tuition" in lower_title:
            # parse for in/out state or single dollar amount
            if "in-state" in lower or "in state" in lower:
                val = parse_currency(text)
                if val is not None:
                    in_state = val
                else:
                    m = CURRENCY_RE.search(text)
                    if m:
                        in_state = parse_currency(m.group(0))
                continue

            if "out-of-state" in lower or "out of state" in lower or "out-of state" in lower:
                val = parse_currency(text)
                if val is not None:
                    out_state = val
                else:
                    m = CURRENCY_RE.search(text)
                    if m:
                        out_state = parse_currency(m.group(0))
                continue

            # If text is just a dollar value, treat as both in/out state unless specific in/out are set
            if CURRENCY_RE.search(text):
                val = parse_currency(text)
                if val is not None:
                    if in_state is None:
                        in_state = val
                    if out_state is None:
                        out_state = val
                continue

            # otherwise fall through to other heuristics

        # check for decimal -> GPA (also allow title to indicate GPA)
        if is_decimal_number(text) or ("gpa" in lower_title):
            if avg_gpa is None:
                # try to extract a decimal token
                m = re.search(r"\d\.\d+", text)
                if m:
                    try:
                        avg_gpa = float(m.group(0))
                    except Exception:
                        pass
            continue

        # If no title detected and value looks like GPA, accept it
        if not lower_title and is_decimal_number(text) and avg_gpa is None:
            try:
                avg_gpa = float(text)
            except Exception:
                pass
            continue

    # Final heuristic: if no GPA found but some TitleValue entries contain a decimal-looking token, pick first
    if avg_gpa is None:
        for el in elems:
            t = clean_text(el.get_text(" ", strip=True))
            m = re.search(r"\b(\d\.\d{2})\b", t)
            if m:
                try:
                    avg_gpa = float(m.group(1))
                    break
                except Exception:
                    pass

    return {
        "url": url,
        "undergraduates": undergrad,
        "in_state_tuition": in_state,
        "out_state_tuition": out_state,
        "avg_gpa": avg_gpa,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", help="Output CSV file (optional)", default="collegedata_out.csv")
    p.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds")
    p.add_argument("--write-back-only", action="store_true", help="Skip scraping and write results from the CSV back to the Google Sheet")
    p.add_argument("--creds", help="Path to service account JSON for sheet writeback", default="cspscraping.json")
    args = p.parse_args()

    # If requested, skip scraping and write existing CSV results back to the sheet
    if args.write_back_only:
        write_results_to_sheet(args.out, creds_file=args.creds, spreadsheet_id=SPREADSHEET_ID, sheet_name=SHEET_NAME, verbose=True)
        return

    # Always read URLs from the spreadsheet specified by the module-level constants
    if not SPREADSHEET_ID or not SHEET_NAME:
        p.error("SPREADSHEET_ID and SHEET_NAME must be set in the script to read university_url values from the sheet.")

    if gspread is None:
        raise RuntimeError("gspread is required to read Google Sheets. Install gspread and google-auth to use the spreadsheet constants.")

    sa = None
    try:
        sa = gspread.service_account(filename="cspscraping.json")
    except Exception as e:
        raise RuntimeError(f"Failed to authenticate with cspscraping.json: {e}")

    try:
        sh = sa.open_by_key(SPREADSHEET_ID)
    except Exception:
        # fallback: try open by key or URL
        sh = sa.open(SPREADSHEET_ID)

    try:
        ws = sh.worksheet(SHEET_NAME)
    except Exception:
        ws = sh.sheet1

    # Read all values from the sheet and ignore the first row (duplicate headers).
    rows = ws.get_all_values()
    urls = []
    if len(rows) >= 2:
        # Use second row as header, data starts on row 3
        header = rows[1]
        data_rows = rows[2:]

        # find column index for academic_info_url (case-insensitive)
        col_idx = None
        for i, h in enumerate(header):
            if h and h.strip().lower() in ("academic_info_url", "academic info url", "academicinfo_url", "academicinfo url", "academic_infourl", "academicinfo", "url", "website"):
                col_idx = i
                break

        # If not found in second row, try first row as a fallback
        if col_idx is None:
            for i, h in enumerate(rows[0]):
                if h and h.strip().lower() in ("academic_info_url", "academic info url", "academicinfo_url", "academicinfo url", "academic_infourl", "academicinfo", "url", "website"):
                    col_idx = i
                    break

        if col_idx is not None:
            for r in data_rows:
                if len(r) > col_idx:
                    val = r[col_idx]
                    if val is not None:
                        sval = val if isinstance(val, str) else str(val)
                        sval = sval.strip()
                        if sval:
                            urls.append(sval)
    else:
        # fallback: use get_all_records if sheet is small
        records = ws.get_all_records()
        for r in records:
            for key in list(r.keys()):
                if key and key.strip().lower() in ("academic_info_url", "academic info url", "academicinfo_url", "academicinfo url", "academic_infourl", "academicinfo", "url", "website"):
                    val = r.get(key)
                    if val is not None:
                        sval = val if isinstance(val, str) else str(val)
                        sval = sval.strip()
                        if sval:
                            urls.append(sval)
                    break

    if not urls:
        p.error("No URLs found in the sheet's academic_info_url column.")

    results = []
    for idx, u in enumerate(urls, 1):
        original_u = u
        try:
            u_strip = u.strip()
        except Exception:
            u_strip = str(u)

        target_url = u_strip
        lower_u = u_strip.lower()

        # If already starts with waf (with or without scheme), don't prepend again
        if lower_u.startswith("https://waf.") or lower_u.startswith("http://waf.") or lower_u.startswith("waf."):
            target_url = u_strip
        # If starts with bare collegedata.com (no scheme, no waf), prepend waf with https
        elif lower_u.startswith("collegedata.com"):
            target_url = "https://waf." + u_strip
        else:
            parsed = urlparse(u_strip)
            netloc = parsed.netloc or parsed.path
            # If netloc endswith collegedata.com, ensure waf. prefix and https scheme
            if netloc and netloc.lower().endswith("collegedata.com"):
                if netloc.lower().startswith("waf."):
                    # already has waf prefix; ensure https scheme
                    scheme = "https"
                    new_parsed = parsed._replace(scheme=scheme, netloc=netloc)
                else:
                    # avoid producing waf.www.collegedata.com — drop a leading www. before adding waf.
                    base = netloc
                    if base.lower().startswith("www."):
                        base = base.split('.', 1)[1]
                    new_netloc = "waf." + base
                    scheme = "https"
                    if parsed.netloc:
                        new_parsed = parsed._replace(scheme=scheme, netloc=new_netloc)
                    else:
                        parts = parsed.path.split('/', 1)
                        path = ('/' + parts[1]) if len(parts) > 1 else ''
                        new_parsed = parsed._replace(scheme=scheme, netloc=new_netloc, path=path)
                target_url = new_parsed.geturl()

        print(f"[{idx}/{len(urls)}] Scraping: {target_url} (source: {original_u})")
        try:
            res = extract_from_url(target_url)
            results.append(res)
            # Print successful scrape result immediately
            print(f"OK: url: {res.get('url')}, undergraduates: {res.get('undergraduates')}, in_state: {res.get('in_state_tuition')}, out_state: {res.get('out_state_tuition')}, avg_gpa: {res.get('avg_gpa')}")
        except Exception as e:
            print(f"Failed to scrape {target_url}: {e}")
            results.append({
                "url": target_url,
                "undergraduates": None,
                "in_state_tuition": None,
                "out_state_tuition": None,
                "avg_gpa": None,
                "error": str(e),
            })
        time.sleep(args.delay)

    # Print first 10 rows (url, undergraduates, tuitions, gpa)
    print('\nFirst 10 results:')
    for r in results[:10]:
        print(f"url: {r.get('url')}, undergraduates: {r.get('undergraduates')}, in_state_tuition: {r.get('in_state_tuition')}, out_state_tuition: {r.get('out_state_tuition')}, avg_gpa: {r.get('avg_gpa')}")

    # Write results to CSV
    fieldnames = ["url", "undergraduates", "in_state_tuition", "out_state_tuition", "avg_gpa"]
    # include error column if present
    if any("error" in r for r in results):
        fieldnames.append("error")

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

def _canonical_url_for_sheet(u: str) -> str:
    if not u:
        return ""
    s = str(u).strip()
    # remove scheme
    if s.startswith("http://"):
        s = s[len("http://"):]
    elif s.startswith("https://"):
        s = s[len("https://"):]
    s = s.lower()
    # strip waf. and www.
    if s.startswith("waf."):
        s = s[len("waf."):]
    if s.startswith("www."):
        s = s[len("www."):]
    if s.endswith("/"):
        s = s[:-1]
    return s

def _update_cell_with_retries(ws, row: int, col: int, value, retries: int = 4, backoff: float = 1.0) -> bool:
    """Update a sheet cell with retry/backoff. Returns True if update succeeded."""
    for attempt in range(1, retries + 1):
        try:
            ws.update_cell(row, col, value)
            return True
        except Exception:
            if attempt == retries:
                return False
            time.sleep(backoff * (2 ** (attempt - 1)))

def write_results_to_sheet(csv_path: str, creds_file: str = "cspscraping.json", spreadsheet_id: str = SPREADSHEET_ID, sheet_name: str = SHEET_NAME, verbose: bool = False) -> None:
    """Write results from csv_path back into the Google Sheet by matching the URL in the academic_info_url column.

    The CSV should contain a column named 'url' (or similar) and any of the result columns
    'undergraduates','in_state_tuition','out_state_tuition','avg_gpa'. Rows in the sheet will be updated
    in-place. The sheet is assumed to have a duplicate first header row (data starts at row 3); the second row
    is treated as header.
    """
    if gspread is None:
        raise RuntimeError("gspread is required to write back to Google Sheets")

    sa = gspread.service_account(filename=creds_file)
    try:
        sh = sa.open_by_key(spreadsheet_id)
    except Exception:
        sh = sa.open(spreadsheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        ws = sh.sheet1

    sheet_values = ws.get_all_values()
    if len(sheet_values) >= 2:
        header = sheet_values[1]
        data_rows = sheet_values[2:]
        data_start_idx = 2
    else:
        header = sheet_values[0] if sheet_values else []
        data_rows = sheet_values[1:]
        data_start_idx = 1

    if verbose:
        print(f"Sheet '{sheet_name}' read: {len(sheet_values)} total rows (including header rows)")

    # find academic_info_url column index
    url_col_idx = None
    for i, h in enumerate(header):
        if h and h.strip().lower() in ("academic_info_url", "academic info url", "academicinfo_url", "academicinfo url", "academic_infourl", "academicinfo", "url", "website"):
            url_col_idx = i
            break
    if url_col_idx is None:
        raise RuntimeError("Could not find academic_info_url column in sheet header")
    if verbose:
        print(f"Detected academic_info_url at column index: {url_col_idx}")

    # ensure result columns exist in sheet header (use exact sheet column names)
    result_sheet_cols = ["undergraduates", "in_state_tuition", "out_of_state_tuition", "avg_admission_gpa"]
    header_lower = [(h.lower() if h else '') for h in header]
    col_indices = {}
    for rc in result_sheet_cols:
        if rc in header_lower:
            col_indices[rc] = header_lower.index(rc)
        else:
            new_col = len(header)
            header.append(rc)
            header_lower.append(rc)
            col_indices[rc] = new_col
            # write header cell to sheet (header row is data_start_idx)
            _update_cell_with_retries(ws, data_start_idx, new_col + 1, rc)
            if verbose:
                print(f"Added sheet header column '{rc}' at index {new_col}")

    # ensure duplicate column exists in sheet
    dup_col_name = "duplicate_url"
    if dup_col_name in header_lower:
        dup_col_idx = header_lower.index(dup_col_name)
    else:
        dup_col_idx = len(header)
        header.append(dup_col_name)
        header_lower.append(dup_col_name)
        _update_cell_with_retries(ws, data_start_idx, dup_col_idx + 1, dup_col_name)
        if verbose:
            print(f"Added duplicate indicator column '{dup_col_name}' at index {dup_col_idx}")

    updates = 0
    # mapping from CSV column names to sheet column names
    csv_to_sheet_map = {
        "undergraduates": "undergraduates",
        "in_state_tuition": "in_state_tuition",
        "out_state_tuition": "out_of_state_tuition",
        "avg_gpa": "avg_admission_gpa",
    }

    # Read CSV rows
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        csv_rows = list(reader)
    if verbose:
        print(f"CSV '{csv_path}' loaded: {len(csv_rows)} data rows, fields: {reader.fieldnames}")

    # find CSV url column
    csv_url_col = None
    for c in (reader.fieldnames or []):
        if c and c.strip().lower() in ("url", "academic_info_url", "academic info url", "academicinfo_url", "academicinfo url", "academic_infourl", "website"):
            csv_url_col = c
            break
    if csv_url_col is None:
        raise RuntimeError("CSV does not contain a URL column to match against sheet 'academic_info_url'")
    if verbose:
        print(f"Using CSV URL column: {csv_url_col}")

    # build canonical counts to detect duplicates
    can_counts = {}
    for crow in csv_rows:
        raw_url = crow.get(csv_url_col)
        can = _canonical_url_for_sheet(raw_url) if raw_url is not None else ""
        if not can:
            continue
        can_counts[can] = can_counts.get(can, 0) + 1

    sheet_map = {}
    for i, srow in enumerate(data_rows, start=data_start_idx + 1):
        if len(srow) > url_col_idx:
            cell = srow[url_col_idx]
            can = _canonical_url_for_sheet(cell)
            if can:
                sheet_map[can] = i
    if verbose:
        print(f"Found {len(sheet_map)} matching sheet rows with canonical URLs")

    # process CSV rows and update sheet
    for crow in csv_rows:
        raw_url = crow.get(csv_url_col)
        can = _canonical_url_for_sheet(raw_url) if raw_url is not None else ""
        row_idx = sheet_map.get(can)
        if row_idx is None:
            # no matching sheet row; check for duplicates
            dup_count = can_counts.get(can, 0)
            if dup_count > 1:
                # duplicate found; mark all duplicates in sheet
                print(f"Duplicate URL detected in CSV: {raw_url} (canonical: {can})")
                for dup_row_idx in sheet_map.values():
                    _update_cell_with_retries(ws, dup_row_idx, dup_col_idx + 1, "Duplicate")
                updates += 1
            else:
                print(f"No sheet row found for CSV URL: {raw_url} (canonical: {can})")
            continue

        # update mapped row in sheet
        updated = False
        for csv_col, sheet_col in csv_to_sheet_map.items():
            csv_val = crow.get(csv_col)
            if csv_val is not None:
                # special case for tuition: convert to float if it's a currency string
                if "tuition" in csv_col.lower():
                    csv_val = parse_currency(csv_val)
                _update_cell_with_retries(ws, row_idx, col_indices[sheet_col] + 1, csv_val)
                updated = True

        if updated:
            # mark duplicate column if there are duplicates
            can_count = can_counts.get(can, 0)
            if can_count > 1:
                _update_cell_with_retries(ws, row_idx, dup_col_idx + 1, "Duplicate")
            else:
                _update_cell_with_retries(ws, row_idx, dup_col_idx + 1, "")
            updates += 1

    if updates > 0:
        print(f"Sheet updated: {updates} rows modified")
    else:
        print("No updates needed for the sheet")
