#!/usr/bin/env python3
"""
Houston health inspection importer.

Data source: https://houston-tx.healthinspections.us/media/ (Tyler Technologies HealthSpace)

This is NOT a REST API — it's a session-based ColdFusion HTML portal.

Flow:
  1. POST to search.cfm with a date range → receive CFID/CFTOKEN session cookies
     and the first page of search results HTML
  2. Parse result HTML to collect (facility_id, inspection_id) pairs; paginate
  3. GET search.cfm?q=d&f={fid}&i={iid} with session cookies → inspection detail HTML
  4. Parse violations: code, description, severity, corrected-on-site status

Violation severity (Houston Code of Ordinances Ch. 20-21):
  Substantial Health Violation → critical  (weight 3)
  Serious Health Violation     → major     (weight 2)
  General Health Violation     → minor     (weight 1)

Score formula (same as RI/NYC):
  risk_score = sum of violation weights
  score      = round(100 × exp(−risk_score × 0.05))

Usage:
  python3 scripts/import_houston.py              # last 7 days (default)
  python3 scripts/import_houston.py --days=30    # last N days
  python3 scripts/import_houston.py --full       # from 2022-01-01 to today
  python3 scripts/import_houston.py --dry-run    # parse only, no DB writes
"""

import http.cookiejar
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

BASE_URL   = 'https://houston-tx.healthinspections.us/media'
SEARCH_URL = f'{BASE_URL}/search.cfm'
REGION     = 'houston'
STATE      = 'TX'
DELAY      = 1.0   # seconds between requests (be polite; server is CF and rate-limits)

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer':    BASE_URL + '/search.cfm',
}


# ── Severity ──────────────────────────────────────────────────────────────────

_SEV_WEIGHTS = {'critical': 3, 'major': 2, 'minor': 1}

def _map_severity(text: str) -> str:
    t = text.lower()
    if 'substantial' in t:
        return 'critical'
    if 'serious' in t:
        return 'major'
    return 'minor'


# ── Score ─────────────────────────────────────────────────────────────────────

def compute_score(violations: list) -> tuple:
    risk  = sum(_SEV_WEIGHTS.get(v['severity'], 1) for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score


def score_to_result(score: int) -> str:
    if score >= 80:
        return 'Pass'
    if score >= 60:
        return 'Pass with Conditions'
    return 'Fail'


# ── Slug helpers ──────────────────────────────────────────────────────────────

def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'houston').lower().replace(' ', '-'))
    return f'{s}-{c}'


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug


# ── Address parsing ───────────────────────────────────────────────────────────

_STATE_ZIP_RE = re.compile(r'\b([A-Z]{2})[,\s]+(\d{5})\b')

def parse_address(raw: str):
    """
    '609 W GULF BANK RD HOUSTON TX 77037' → (street, city, state, zip)
    """
    raw = re.sub(r'<[^>]+>', ' ', raw)   # strip any leftover HTML tags
    raw = re.sub(r'\s+', ' ', raw).strip().upper()
    raw = raw.replace(',', ' ')

    m = _STATE_ZIP_RE.search(raw)
    if not m:
        return raw.title(), 'Houston', STATE, None

    state = m.group(1)
    zip5  = m.group(2)
    before = raw[:m.start()].strip()

    # Last word before state abbreviation = city
    parts = before.rsplit(None, 1)
    if len(parts) == 2:
        return parts[0].title(), parts[1].title(), state, zip5
    return before.title(), 'Houston', state, zip5


# ── HTTP session ──────────────────────────────────────────────────────────────

def _make_opener():
    jar    = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = list(_HEADERS.items())
    return opener, jar


def _cf_params(jar) -> dict:
    """
    Extract CFID/CFTOKEN from the cookie jar and return them as a dict so
    they can be appended to GET URLs. Older ColdFusion installs require session
    tokens as URL params, not just cookies.
    """
    params = {}
    for cookie in jar:
        if cookie.name.upper() == 'CFID':
            params['CFID'] = cookie.value
        elif cookie.name.upper() == 'CFTOKEN':
            params['CFTOKEN'] = cookie.value
    return params


def _post(opener, url: str, params: dict) -> str:
    data = urllib.parse.urlencode(params).encode()
    req  = urllib.request.Request(url, data=data, method='POST',
                                  headers={'Content-Type': 'application/x-www-form-urlencoded'})
    with opener.open(req, timeout=25) as r:
        return r.read().decode('utf-8', errors='replace')


def _get(opener, url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        req = urllib.request.Request(url, headers={'Referer': SEARCH_URL})
        try:
            with opener.open(req, timeout=25) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as exc:
            if exc.code == 503 and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  503 on attempt {attempt+1}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ── HTML parsing ──────────────────────────────────────────────────────────────

# Links to inspection detail pages: search.cfm?q=d&f=FACID&i=INSPID&...
_DETAIL_LINK_RE = re.compile(
    r'search\.cfm\?[^"\'<>]*?(?:&amp;|&)f=([^&"\'<>\s]+)[^"\'<>]*?(?:&amp;|&)i=([^&"\'<>\s]+)',
    re.IGNORECASE,
)

# Total results count: "123 records found" or similar
_TOTAL_RE = re.compile(r'(\d[\d,]*)\s+(?:records?|establishments?|results?)', re.IGNORECASE)

# Violation entry: ddrivetip tooltip + "Houston Ordinance Violation: CODE" text
# Captures (tooltip_text, code) — code may be empty for category-header rows
_VIOLATION_RE = re.compile(
    r"ddrivetip\('([^']+)',\s*'white',\s*400\)[^>]*>Houston Ordinance Violation:\s*([^<]*?)\s*</a>",
    re.IGNORECASE | re.DOTALL,
)

# "Corrected on site" status in a table cell
_COS_RE = re.compile(r'corrected[\s\-]+on[\s\-]+site', re.IGNORECASE)

# Inspection date anywhere on detail page (MM/DD/YYYY)
_DATE_RE = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b')

# Inspection type from table header row (Routine / Reinspection / etc.)
_TYPE_RE = re.compile(
    r'<td[^>]*ge_tableData[^>]*>[^<]*(?:Routine|Reinspection|Complaint|Follow[- ]?up|Initial)[^<]*</td>',
    re.IGNORECASE,
)


def _strip(html: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    s = re.sub(r'<[^>]+>', ' ', html)
    return re.sub(r'\s+', ' ', s).strip()


def parse_pairs(html: str) -> list:
    """Extract (facility_id, inspection_id) pairs from search results HTML."""
    # Normalise &amp; so regex works regardless of encoding
    html_norm = html.replace('&amp;', '&')
    seen, pairs = set(), []
    for m in _DETAIL_LINK_RE.finditer(html_norm):
        key = (m.group(1).strip(), m.group(2).strip())
        if key not in seen:
            seen.add(key)
            pairs.append({'facility_id': key[0], 'inspection_id': key[1]})
    return pairs


def parse_total(html: str) -> int:
    m = _TOTAL_RE.search(html)
    return int(m.group(1).replace(',', '')) if m else 0


def parse_detail(html: str, facility_id: str) -> dict | None:
    """
    Parse a Houston inspection detail page.

    Key HTML patterns observed:
      - Facility name:  <h3>NAME</h3>
      - Address:        plain text immediately after </h3>, e.g. "KINGWOOD TX, 77339 <br>"
      - Violations:     ddrivetip('DESCRIPTION','white',400) tooltip inside each
                        "Houston Ordinance Violation: CODE" link
      - Status:         separate <td> containing "Violation" / "Violation Corrected On Site"
      - Severity:       tooltip text starting with "Foodborne Illness Risk Factors" → critical;
                        otherwise minor (Houston doesn't label rows as Substantial/Serious/General
                        in the HTML — they use FDA item category numbers)
    """
    if len(html) < 500:
        return None   # empty / error page

    # ── Name ─────────────────────────────────────────────────────────────────
    h3_m = re.search(r'<h3>(.*?)</h3>', html, re.IGNORECASE | re.DOTALL)
    if not h3_m:
        return None
    name = _strip(h3_m.group(1)).title()
    if not name or len(name) < 2:
        return None

    # ── Address ───────────────────────────────────────────────────────────────
    # The address follows </h3> as two text nodes separated by <br>:
    #   "911 SOUTHERN HILLS RD\nKINGWOOD TX, 77339 <br>"
    # Grab up to 300 chars, replace <br> with space, strip other tags.
    addr_raw = ''
    after_h3 = html[h3_m.end():h3_m.end() + 300]
    after_h3 = re.sub(r'<br\s*/?>', ' ', after_h3, flags=re.IGNORECASE)
    after_h3 = re.sub(r'<[^>]+>', '', after_h3)
    addr_raw = re.sub(r'\s+', ' ', after_h3).strip()[:150]

    # ── Inspection date ───────────────────────────────────────────────────────
    insp_date = None
    for dm in _DATE_RE.finditer(html):
        try:
            d = datetime.strptime(dm.group(1), '%m/%d/%Y').date()
            if 2015 <= d.year <= date.today().year:
                insp_date = d
                break
        except ValueError:
            continue

    # ── Inspection type ───────────────────────────────────────────────────────
    insp_type = 'Routine'
    type_m = _TYPE_RE.search(html)
    if type_m:
        raw = _strip(type_m.group(0))
        if raw:
            insp_type = raw

    # ── Violations ────────────────────────────────────────────────────────────
    # Each violation has:
    #   ddrivetip('DESCRIPTION','white',400)...>Houston Ordinance Violation: CODE </a>
    # Followed (somewhere after) by a status <td>:
    #   <td ...>Violation<br> Corrected On Site</td>  OR  <td ...>Violation</td>
    #
    # We walk through all violation matches and pair each with the nearest
    # following status cell.

    violations = []
    seen_codes   = set()
    current_category = ''   # tracks the most recent category-header tooltip

    # Split the HTML into segments at each violation link so we can check
    # what status follows each one.
    segments = list(_VIOLATION_RE.finditer(html))

    for idx, m in enumerate(segments):
        tooltip = m.group(1).strip()   # full ddrivetip text
        code    = m.group(2).strip().rstrip('.')

        # Category-header rows have an empty code — update current category and skip.
        # e.g. "Foodborne Illness Risk Factors and Public Health Interventions: 13"
        if not code:
            current_category = tooltip.lower()
            continue
        if code in seen_codes:
            continue
        seen_codes.add(code)

        # Description: the ddrivetip tooltip for a real violation starts with the
        # code itself, e.g. "3-302.11(A)(1)(a) FOOD shall be protected..."
        # Strip the leading code from the tooltip to get the description.
        desc = tooltip
        if tooltip.startswith(code):
            desc = tooltip[len(code):].strip().lstrip('.-– ')
        if not desc:
            desc = code

        # Severity: all violations under "Foodborne Illness Risk Factors" are critical.
        # All others (Good Retail Practices, etc.) are minor.
        severity = 'critical' if 'foodborne illness risk factors' in current_category else 'minor'

        # Corrected on site: look in the HTML between this match and the next
        end_pos   = segments[idx + 1].start() if idx + 1 < len(segments) else len(html)
        between   = html[m.end():end_pos]
        corrected = bool(_COS_RE.search(between))

        violations.append({
            'code':      code,
            'desc':      desc.capitalize(),
            'severity':  severity,
            'corrected': corrected,
        })

    return {
        'facility_id': facility_id,
        'name':        name,
        'address_raw': addr_raw,
        'date':        insp_date,
        'type':        insp_type,
        'violations':  violations,
    }


# ── Fetching ──────────────────────────────────────────────────────────────────

def fetch_pairs_for_range(from_date: date, to_date: date, maxrows: int = 50):
    """
    POST a date-range search, paginate, return (opener, jar, pairs, sd, ed).
    opener+jar must be reused for subsequent detail-page GETs.
    """
    sd = from_date.strftime('%m/%d/%Y')
    ed = to_date.strftime('%m/%d/%Y')
    print(f"  Searching Houston portal {sd} – {ed}...")

    opener, jar = _make_opener()
    post_params = {
        'q': 's', 'e': '', 'k': '', 'r': '', 'tp': 'ALL',
        'sd': sd, 'ed': ed, 'z': 'ALL', 'm': 'LIKE',
        'maxrows': str(maxrows), 'Submit': 'Search',
    }

    try:
        html = _post(opener, SEARCH_URL, post_params)
    except Exception as exc:
        print(f"  Initial POST failed: {exc}")
        return opener, jar, [], sd, ed

    # Log cookies so we can debug session issues
    cf = _cf_params(jar)
    print(f"  Session cookies: {cf}")

    all_pairs = parse_pairs(html)
    print(f"  Page 1: {len(all_pairs)} pairs.")

    if os.environ.get('HOUSTON_DEBUG_HTML'):
        with open('/tmp/houston_search.html', 'w') as fh:
            fh.write(html)
        print("  Search HTML dumped to /tmp/houston_search.html")

    # Paginate until a page returns fewer than maxrows pairs (portal has no reliable total count)
    start = maxrows + 1
    while len(parse_pairs(html)) >= maxrows:
        time.sleep(DELAY)
        qs = urllib.parse.urlencode({
            'start': start, 'Q': 's', 'E': '', 'K': '', 'R': '',
            'TP': 'ALL', 'Z': 'ALL', 'M': 'LIKE', 'MAXROWS': maxrows,
        })
        qs += f'&SD={sd}&ED={ed}'
        try:
            html = _get(opener, f'{SEARCH_URL}?{qs}')
        except Exception as exc:
            print(f"  Error at start={start}: {exc}")
            break
        page = parse_pairs(html)
        if not page:
            break
        all_pairs.extend(page)
        start += maxrows
        print(f"  {len(all_pairs)} pairs collected...", end='\r')

    if len(all_pairs) >= 500:
        print(f"  WARNING: collected {len(all_pairs)} pairs — portal cap likely hit. Reduce --chunk-days.")
    print(f"\n  Collected {len(all_pairs)} (facility, inspection) pairs.")
    return opener, jar, all_pairs, sd, ed


def fetch_detail(opener, jar, facility_id: str, inspection_id: str, sd: str, ed: str,
                 debug: bool = False) -> dict | None:
    # Cookies (CFID/CFTOKEN) are sent automatically by the opener's CookieJar.
    # Do NOT add them as URL params — some CF setups reject duplicate session tokens.
    # NOTE: sd/ed use raw slashes (not %2F) — CF does string-comparison on dates
    # and rejects the encoded form, falling back to the blank search page.
    qs  = urllib.parse.urlencode({
        'q': 'd', 'f': facility_id, 'i': inspection_id,
        'z': 'ALL', 'm': 'LIKE', 'maxrows': '50', 'e': '', 'tp': 'ALL',
    })
    qs += f'&sd={sd}&ed={ed}'
    url = f'{SEARCH_URL}?{qs}'
    if debug:
        print(f"  DEBUG detail URL: {url}")
    try:
        html = _get(opener, url)
        if debug:
            with open('/tmp/houston_detail.html', 'w') as fh:
                fh.write(html)
            print(f"  DEBUG detail HTML ({len(html)} chars) dumped to /tmp/houston_detail.html")
    except urllib.error.HTTPError as exc:
        if debug:
            try:
                body = exc.read().decode('utf-8', errors='replace')[:800]
            except Exception:
                body = '(could not read body)'
            print(f"  DEBUG HTTP {exc.code} body:\n{body}\n")
        if exc.code in (403, 404, 500, 503):
            return None
        raise
    except Exception as exc:
        if debug:
            print(f"  DEBUG exception: {exc}")
        return None
    return parse_detail(html, facility_id)


# ── DB write ──────────────────────────────────────────────────────────────────

def write_to_db(records: list, app, db, Restaurant, Inspection, Violation):
    with app.app_context():
        existing = {
            r.source_id: r
            for r in Restaurant.query.filter_by(region=REGION)
                                     .filter(Restaurant.source_id.isnot(None)).all()
        }
        seen_slugs = {r.slug for r in existing.values()}
        seen_slugs |= {
            r.slug for r in
            Restaurant.query.filter(Restaurant.region != REGION)
                            .with_entities(Restaurant.slug).all()
        }
        new_r = new_i = skipped = 0

        for rec in records:
            fid  = rec['facility_id']
            name = (rec.get('name') or '').strip()
            if not name:
                skipped += 1
                continue

            insp_date = rec.get('date')
            if not insp_date:
                skipped += 1
                continue

            street, city, state, zip5 = parse_address(rec.get('address_raw', ''))

            # ── Get or create restaurant ──────────────────────────────────────
            if fid in existing:
                restaurant = existing[fid]
            else:
                slug = unique_slug(make_slug(name, city or 'houston'), seen_slugs)
                restaurant = Restaurant(
                    source_id    = fid,
                    name         = name,
                    slug         = slug,
                    address      = street,
                    city         = city,
                    state        = state,
                    zip          = zip5,
                    latitude     = None,
                    longitude    = None,
                    cuisine_type = None,
                    region       = REGION,
                )
                db.session.add(restaurant)
                db.session.flush()
                existing[fid] = restaurant
                new_r += 1

            # ── Skip duplicate inspections ────────────────────────────────────
            if Inspection.query.filter_by(
                restaurant_id=restaurant.id, inspection_date=insp_date
            ).first():
                skipped += 1
                continue

            # ── Score and write inspection ────────────────────────────────────
            violations = rec.get('violations', [])
            risk, score = compute_score(violations)

            insp = Inspection(
                restaurant_id   = restaurant.id,
                inspection_date = insp_date,
                score           = score,
                risk_score      = risk,
                grade           = None,
                result          = score_to_result(score),
                inspection_type = rec.get('type') or 'Routine',
            )
            db.session.add(insp)
            db.session.flush()
            new_i += 1

            for v in violations:
                db.session.add(Violation(
                    inspection_id     = insp.id,
                    violation_code    = v['code'],
                    description       = v['desc'],
                    severity          = v['severity'],
                    corrected_on_site = v['corrected'],
                ))

            old_latest = restaurant.latest_inspection_date
            if old_latest is None or insp_date > old_latest:
                if old_latest != insp_date:
                    restaurant.ai_summary = None
                restaurant.latest_inspection_date = insp_date

            if new_i % 250 == 0 and new_i > 0:
                db.session.commit()
                print(f"  Committed {new_i} inspections so far...")

        db.session.commit()
        return new_r, new_i, skipped


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    full_mode = '--full'    in sys.argv
    dry_run   = '--dry-run' in sys.argv
    debug     = '--debug'   in sys.argv
    days      = 7
    since     = None
    until     = None
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--since='):
            since = date.fromisoformat(arg.split('=', 1)[1])
        elif arg.startswith('--until='):
            until = date.fromisoformat(arg.split('=', 1)[1])

    today     = date.today()
    if full_mode:
        from_date = since or date(2018, 1, 1)
    else:
        from_date = since or (today - timedelta(days=days))
    to_date   = until or today

    # Chunk size: Houston portal hard-caps at 500 results per search.
    # 2018 data averages ~70 inspections/day → 2-day chunks ≈ 140, safely under cap.
    # The --chunk-days flag overrides if needed.
    chunk_days = 2
    for arg in sys.argv[1:]:
        if arg.startswith('--chunk-days='):
            chunk_days = int(arg.split('=', 1)[1])

    if full_mode or (to_date - from_date).days > chunk_days:
        chunks = []
        cur = from_date
        while cur < to_date:
            nxt = min(cur + timedelta(days=chunk_days), to_date)
            chunks.append((cur, nxt))
            cur = nxt + timedelta(days=1)
        chunks.reverse()   # process most recent first — recent data matters more
    else:
        chunks = [(from_date, to_date)]

    if not dry_run:
        from app import create_app
        from app.db import db
        from app.models.restaurant import Restaurant
        from app.models.inspection import Inspection
        from app.models.violation import Violation
        app = create_app()
    else:
        app = db = Restaurant = Inspection = Violation = None

    total_r = total_i = total_skipped = 0

    for chunk_idx, (chunk_from, chunk_to) in enumerate(chunks):
        opener, jar, pairs, sd, ed = fetch_pairs_for_range(chunk_from, chunk_to)
        if not pairs:
            continue

        chunk_records = []
        print(f"  Fetching {len(pairs)} detail pages...")
        if debug:
            print(f"  First 3 pairs: {pairs[:3]}")
        for idx, pair in enumerate(pairs):
            is_debug = debug and idx == 0
            detail = fetch_detail(opener, jar, pair['facility_id'], pair['inspection_id'],
                                  sd, ed, debug=is_debug)
            if detail:
                chunk_records.append(detail)
            if (idx + 1) % 20 == 0:
                print(f"  {idx+1}/{len(pairs)} fetched ({len(chunk_records)} valid)...", end='\r')
            time.sleep(DELAY)
            if debug and idx == 0:
                break   # only fetch one detail page in debug mode
        print()

        if dry_run:
            print(f"  --dry-run: {len(chunk_records)} records in chunk {chunk_idx+1}/{len(chunks)}")
            for r in chunk_records[:3]:
                print(f"    {r['name']} | {r['date']} | {len(r['violations'])} violations")
            continue

        if not chunk_records:
            continue

        new_r, new_i, skipped = write_to_db(
            chunk_records, app, db, Restaurant, Inspection, Violation
        )
        total_r       += new_r
        total_i       += new_i
        total_skipped += skipped
        print(f"  Chunk {chunk_idx+1}/{len(chunks)}: +{new_r} restaurants, +{new_i} inspections, {skipped} skipped")

    if dry_run:
        print("--dry-run complete.")
        return

    print(f"\nDone.")
    print(f"  {total_r:,} new restaurants")
    print(f"  {total_i:,} new inspections")
    print(f"  {total_skipped:,} skipped (duplicate or no date)")


if __name__ == '__main__':
    main()
