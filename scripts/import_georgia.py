#!/usr/bin/env python3
"""
Georgia (GA DPH statewide) health inspection importer.

Data source: https://ga.healthinspections.us/stateofgeorgia/

This is a Tyler Technologies HealthSpace SPA backed by a JSON API — no HTML
scraping required. Three endpoints are used:

  GET  API/index.cfm/facilities/{page}
       → 5 facility rows per page, sorted by last_inspection_date DESC.
         Each row has: id (base64 numeric), name, mapAddress, columns{...}
         where columns hold permit type, permit number, last score, last date.

  GET  API/index.cfm/inspectionsData/{base64_facility_id}
       → list of inspections for one facility. Each inspection includes
         columns{date, purpose, score, inspector} AND a fully-populated
         violations dict (no second fetch required to get violation detail).

GA permit types covered (we ingest all of them — restaurants, hotels, pools):
  Food Service          (Rule 511-6-1)
  Tourist Accommodation (Rule 511-6-2)
  Swimming Pool         (Rule 511-3-5)

Severity (uniform across permit types — derived from inspector points):
  >= 9 points → critical (weight 3)
  >= 4 points → major    (weight 2)
  <  4 points → minor    (weight 1)

  Food Service violations also include explicit (p)/(pf)/(c) markers in the
  citation line, but we don't rely on them — they're absent on Tourist
  Accommodation and Swimming Pool inspections, and the points field is
  authoritative for all three. Points are what GA actually subtracts from 100.

Score:
  GA gives us a real numeric score per inspection (e.g. "Score: 87"). We use
  it directly rather than recomputing — it matches what the official portal
  displays and what shows on the inspector's report.

Inspector notes cleanup:
  Inspectors sometimes paste the full regulatory boilerplate into the Notes
  field (5000+ chars of "(b) Equipment Food-Contact Surfaces and Utensils. 1.
  ...(i) Before each use ..."). We strip the "Inspector Notes:" prefix and
  truncate at the first newline-then-citation-code boundary, which is where
  the inspector's own observation ends and the canned regulatory dump begins.
  Hard cap of 800 chars catches anything that slips past the heuristic.

Usage:
  python3 scripts/import_georgia.py              # last 7 days (default)
  python3 scripts/import_georgia.py --days=30
  python3 scripts/import_georgia.py --full       # walk entire portal listing
  python3 scripts/import_georgia.py --full --start-page=1200   # resume
  python3 scripts/import_georgia.py --dry-run    # parse only, no DB writes
  python3 scripts/import_georgia.py --debug      # extra logging, no early exit
"""

import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()


# ── Constants ────────────────────────────────────────────────────────────────

REGION  = 'georgia'
STATE   = 'GA'

BASE_URL          = 'https://ga.healthinspections.us/stateofgeorgia'
FACILITIES_URL    = f'{BASE_URL}/API/index.cfm/facilities/{{page}}'
INSPECTIONS_URL   = f'{BASE_URL}/API/index.cfm/inspectionsData/{{fid}}'

PAGE_SIZE = 5      # GA portal returns exactly 5 facilities per page
DELAY     = 0.6    # seconds between requests (1.6 req/s, ~one page+inspections per second)

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'application/json, text/javascript, */*; q=0.01',
    'Referer':    f'{BASE_URL}/',
    'X-Requested-With': 'XMLHttpRequest',
}

_SEV_WEIGHTS = {'critical': 3, 'major': 2, 'minor': 1}


# ── HTTP ─────────────────────────────────────────────────────────────────────

def _get_json(url: str, retries: int = 3) -> object | None:
    """GET a JSON endpoint with backoff. Returns parsed JSON or None on failure."""
    req = urllib.request.Request(url, headers=_HEADERS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read().decode('utf-8', errors='replace')
                if not raw.strip():
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code in (502, 503, 504) and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  HTTP {exc.code}, retry {attempt+1}/{retries} in {wait}s")
                time.sleep(wait)
                continue
            print(f"  HTTP {exc.code} on {url}")
            return None
        except urllib.error.URLError as exc:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  URLError ({exc}), retry {attempt+1}/{retries} in {wait}s")
                time.sleep(wait)
                continue
            print(f"  URLError on {url}: {exc}")
            return None
        except json.JSONDecodeError as exc:
            print(f"  JSON parse error on {url}: {exc}")
            return None
    return None


# ── Severity ─────────────────────────────────────────────────────────────────

def severity_for_points(points: int) -> str:
    """
    Map GA inspector-assigned points to our internal severity tiers.

    GA Food Service form: 9 = Priority, 4 = Priority Foundation, 1 = Core.
    GA Tourist Accommodation / Swimming Pool forms use comparable scales —
    high-impact items still carry 9, intermediate items 4-5, minor items 1-3.
    Single uniform mapping is fine because the important property is rank
    consistency within an inspection, not cross-form calibration.
    """
    if points >= 9:
        return 'critical'
    if points >= 4:
        return 'major'
    return 'minor'


def compute_risk(violations: list) -> int:
    return sum(_SEV_WEIGHTS.get(v['severity'], 1) for v in violations)


def score_to_result(score: int | None) -> str:
    if score is None:
        return 'Unknown'
    if score >= 80:
        return 'Pass'
    if score >= 60:
        return 'Pass with Conditions'
    return 'Fail'


# ── ID + slug helpers ────────────────────────────────────────────────────────

def decode_facility_id(b64_id: str) -> str | None:
    """The GA portal uses base64-encoded numeric facility IDs. Decode for source_id."""
    try:
        return base64.b64decode(b64_id + '===').decode('utf-8').strip()
    except Exception:
        return None


def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', (name or '').lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'georgia').lower().replace(' ', '-'))
    return f'{s}-{c}' if c else s


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug


# ── Address parsing ──────────────────────────────────────────────────────────

# GA mapAddress format:  "120 PAUL FRANKLIN RD \r\nCLARKESVILLE, GA 30523"
_ADDR_LINE2_RE = re.compile(
    r'^(?P<city>[^,]+?),\s*(?P<state>[A-Z]{2})\s+(?P<zip>\d{5})(?:-\d{4})?\s*$',
    re.IGNORECASE,
)


def parse_address(raw: str) -> tuple:
    """
    Parse "120 PAUL FRANKLIN RD \\r\\nCLARKESVILLE, GA 30523"
      → ('120 Paul Franklin Rd', 'Clarkesville', 'GA', '30523').
    Falls back to ('Atlanta', 'GA', None) if the second line is unparseable —
    Atlanta is GA's largest city and the safest default for SEO grouping.
    """
    if not raw:
        return None, 'Atlanta', STATE, None

    # Normalize all whitespace variants to a single \n
    s = re.sub(r'\r\n|\r', '\n', raw).strip()
    parts = [p.strip() for p in s.split('\n') if p.strip()]

    if len(parts) >= 2:
        street = parts[0].title() or None
        m = _ADDR_LINE2_RE.match(parts[1])
        if m:
            return (
                street,
                (m.group('city') or 'Atlanta').strip().title(),
                m.group('state').upper(),
                m.group('zip'),
            )
        # Second line exists but doesn't match — at least keep the street
        return street, 'Atlanta', STATE, None

    if len(parts) == 1:
        # Single-line address: try to peel state+zip off the end
        m = re.search(r'([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$', parts[0], re.IGNORECASE)
        if m:
            before = parts[0][:m.start()].rstrip(', ').strip()
            if ',' in before:
                street, city = before.rsplit(',', 1)
                return street.strip().title(), city.strip().title(), m.group(1).upper(), m.group(2)
        return parts[0].title(), 'Atlanta', STATE, None

    return None, 'Atlanta', STATE, None


# ── Facility column parsing ──────────────────────────────────────────────────

def _strip_label(text: str, label: str) -> str:
    """Remove a 'Label: ' prefix from a column value."""
    if text.lower().startswith(label.lower()):
        return text[len(label):].strip()
    return text.strip()


def parse_facility(row: dict) -> dict | None:
    """
    Convert a raw facility dict from /facilities/{page} into the shape we
    write to DB. Returns None if the row is missing required fields.
    """
    cols = row.get('columns', {})
    name = (row.get('name') or '').strip()
    if not name:
        return None

    fid_b64 = row.get('id') or ''
    fid_num = decode_facility_id(fid_b64)
    if not fid_num:
        return None

    permit_type = _strip_label(cols.get('2', ''), 'Permit Type:')
    permit_num  = _strip_label(cols.get('3', ''), 'Permit Number:')

    last_date_str = _strip_label(cols.get('5', ''), 'Last Inspection Date:')
    last_date     = None
    if last_date_str and last_date_str.upper() != 'NA':
        try:
            last_date = datetime.strptime(last_date_str, '%m-%d-%Y').date()
        except ValueError:
            last_date = None

    return {
        'fid_b64':     fid_b64,
        'source_id':   fid_num,
        'name':        name,
        'address_raw': row.get('mapAddress', ''),
        'permit_type': permit_type or None,
        'permit_num':  permit_num or None,
        'last_date':   last_date,
    }


# ── Inspection + violation parsing ───────────────────────────────────────────

# GA citations look like: 511-6-1.04(1), 511-6-1.05(7)(b), 511-6-2-.13(1),
# 511-3-5-.11(3)(d). We capture the leading code before any sub-paren groups.
_CITATION_CORE_RE = re.compile(r'^\s*(\d+-\d+(?:-?\.\d+)*)')

# Used to chop boilerplate that begins with a citation on a new line.
_NL_CITATION_RE = re.compile(r'\n\s*\d+-\d+(?:-?\.\d+)*')


def _extract_violation_code(citation_line: str) -> str | None:
    """Pull the citation code (e.g. '511-6-1.04(1)') off a v[1] line."""
    if not citation_line:
        return None
    # Try to grab the code with all paren groups attached
    m = re.match(r'\s*((?:\d+-)+\d+(?:-?\.\d+)*(?:\([A-Za-z0-9]+\))*)', citation_line)
    return m.group(1) if m else None


def _parse_points(points_line: str) -> int:
    """Pull the integer out of 'Points: 9'. Defaults to 1 (minor) if missing."""
    if not points_line:
        return 1
    m = re.search(r'(\d+)', points_line)
    return int(m.group(1)) if m else 1


def clean_inspector_notes(raw: str, citation_code: str | None) -> str | None:
    """
    Strip the 'Inspector Notes' prefix and any pasted-in regulatory boilerplate,
    leaving just the inspector's actual observation (and any short corrective
    action text that follows).

    Heuristic: the boilerplate dump always begins with the citation code on a
    new line — e.g. observation ends "...mold-like substance.\\n511-6-1.05(7)(b)
    - Food Contact Surfaces and Utensils - Cleaning Frequency..." — so we cut
    at the first such boundary. Hard-capped at 800 chars regardless.
    """
    if not raw:
        return None

    # The portal sometimes prints "Inspector Notes:" and sometimes the colon is
    # missing entirely — the value is just glued to the label as
    # "Inspector NotesObserved...". Match both.
    s = re.sub(r'^Inspector\s+Notes:?\s*', '', raw, flags=re.IGNORECASE)

    # First-pass truncation: try the EXACT citation code from this violation.
    if citation_code:
        m_core = _CITATION_CORE_RE.match(citation_code)
        if m_core:
            core = m_core.group(1)
            # Look for the code prefix preceded by a newline or paragraph break.
            pat = re.compile(r'\s*\n+\s*' + re.escape(core))
            mm = pat.search(s)
            if mm:
                s = s[:mm.start()]

    # Second-pass: any GA-style citation sitting at the start of a new line
    # (catches cases where the boilerplate begins with a SIBLING rule, e.g.
    # the inspector cited 511-6-1.04 but pasted the text of 511-6-1.05).
    mm = _NL_CITATION_RE.search(s)
    if mm:
        s = s[:mm.start()]

    # Collapse internal whitespace; the API delivers \r\n\r\n paragraph breaks
    # that look ugly in the rendered template.
    s = re.sub(r'\s+', ' ', s).strip()

    if not s:
        return None
    if len(s) > 800:
        s = s[:797].rstrip() + '...'
    return s


def parse_violation(v_array: list) -> dict | None:
    """
    Turn one element of inspection['violations'] into our internal shape.

    The portal returns each violation as an array of strings, in this fixed
    order. Field 5 (Inspector Notes) may be missing on the rare row where
    the inspector skipped writing anything.
    """
    if not v_array:
        return None

    desc_line     = v_array[0] if len(v_array) > 0 else ''   # form item title
    citation_line = v_array[1] if len(v_array) > 1 else ''   # 511-x-x... + (p)/(pf)/(c)
    points_line   = v_array[2] if len(v_array) > 2 else ''   # "Points: N"
    corrected     = v_array[3] if len(v_array) > 3 else ''   # "Corrected during inspection?: Yes/No"
    notes_line    = v_array[5] if len(v_array) > 5 else ''   # "Inspector Notes: ..."

    points = _parse_points(points_line)
    code   = _extract_violation_code(citation_line)
    severity = severity_for_points(points)

    # Description: prefer the form item title (v[0]) since it's the standard
    # GA form-item phrasing. Fall back to the citation line if v[0] is empty.
    description = (desc_line or citation_line or '').strip()
    if len(description) > 500:
        description = description[:497].rstrip() + '...'

    notes = clean_inspector_notes(notes_line, code)

    is_corrected = 'yes' in (corrected or '').lower()

    return {
        'code':        code,
        'desc':        description or None,
        'notes':       notes,
        'severity':    severity,
        'points':      points,
        'corrected':   is_corrected,
    }


def parse_inspection(ins: dict) -> dict | None:
    """Parse one element of the inspectionsData response into our internal shape."""
    cols = ins.get('columns', {})

    # Inspection date — column 0 is "Date: MM-DD-YYYY"
    date_str = _strip_label(cols.get('0', ''), 'Date:')
    insp_date = None
    if date_str:
        try:
            insp_date = datetime.strptime(date_str, '%m-%d-%Y').date()
        except ValueError:
            insp_date = None
    if not insp_date:
        return None

    purpose = _strip_label(cols.get('1', ''), 'Inspection Purpose:') or 'Routine'

    score_str = _strip_label(cols.get('2', ''), 'Score:')
    score = None
    if score_str:
        try:
            score = int(score_str)
        except ValueError:
            score = None

    violations = []
    raw_v = ins.get('violations') or {}
    # Keys are stringified ints "0","1","2" — preserve their order.
    for key in sorted(raw_v.keys(), key=lambda k: int(k) if k.isdigit() else 0):
        v = parse_violation(raw_v[key])
        if v:
            violations.append(v)

    return {
        'inspection_id': str(ins.get('inspectionId') or ''),
        'date':          insp_date,
        'purpose':       purpose,
        'score':         score,
        'violations':    violations,
    }


# ── Walking the portal ───────────────────────────────────────────────────────

def fetch_facility_page(page_idx: int) -> list:
    """Fetch one page of facilities. Returns parsed facility dicts."""
    data = _get_json(FACILITIES_URL.format(page=page_idx))
    if not isinstance(data, list):
        return []
    parsed = []
    for row in data:
        f = parse_facility(row)
        if f:
            parsed.append(f)
    return parsed


def fetch_facility_inspections(fid_b64: str, since: date) -> list:
    """
    Fetch every inspection for one facility and parse those dated >= `since`.
    Returns a list of inspection dicts.
    """
    data = _get_json(INSPECTIONS_URL.format(fid=urllib.parse.quote(fid_b64)))
    if not isinstance(data, list):
        return []
    out = []
    for ins in data:
        parsed = parse_inspection(ins)
        if parsed and parsed['date'] >= since:
            out.append(parsed)
    return out


# ── DB write ─────────────────────────────────────────────────────────────────

def load_db_state(db, Restaurant, Inspection) -> tuple:
    """
    Load the dedup state needed by write_batch. Called once at the start of
    a run, then the dicts/sets are mutated in place across subsequent batches
    so we never re-query the DB during the walk.
    """
    existing = {
        r.source_id: r
        for r in Restaurant.query.filter_by(region=REGION)
                                 .filter(Restaurant.source_id.isnot(None)).all()
    }
    # Region-scoped slug set — the uq_restaurant_region_slug constraint is
    # (region, slug), so we only need to avoid collisions within our own
    # region. Full-table scan would be wasteful on a 6h --full run.
    seen_slugs = set(
        slug for (slug,) in
        db.session.query(Restaurant.slug).filter(Restaurant.region == REGION).all()
    )
    existing_insp_ids = set(
        sid for (sid,) in
        db.session.query(Inspection.source_id)
                  .filter(Inspection.region == REGION,
                          Inspection.source_id.isnot(None)).all()
    )
    return existing, seen_slugs, existing_insp_ids


def write_batch(records: list, db, Restaurant, Inspection, Violation,
                existing: dict, seen_slugs: set, existing_insp_ids: set) -> tuple:
    """
    Write one batch of (facility, [inspection, ...]) tuples to the DB, mutating
    the shared dedup state so subsequent batches stay consistent.

    Caller is responsible for opening the app_context. Commits once at the end
    of the batch so a crash mid-batch rolls the batch back but leaves all prior
    committed batches intact — that's what makes --full resumable.

    Returns (new_restaurants, new_inspections, skipped).
    """
    new_r = new_i = skipped = 0

    for fac, inspections in records:
        if not inspections:
            continue

        source_id = fac['source_id']
        street, city, state, zip5 = parse_address(fac.get('address_raw', ''))

        # ── Get or create restaurant ────────────────────────────────────
        if source_id in existing:
            restaurant = existing[source_id]
            # Backfill license_type if it wasn't set on a previous run
            if not restaurant.license_type and fac.get('permit_type'):
                restaurant.license_type = fac['permit_type']
        else:
            slug = unique_slug(make_slug(fac['name'], city), seen_slugs)
            restaurant = Restaurant(
                source_id    = source_id,
                name         = fac['name'],
                slug         = slug,
                address      = street,
                city         = city,
                state        = state,
                zip          = zip5,
                latitude     = None,
                longitude    = None,
                cuisine_type = None,
                license_type = fac.get('permit_type'),
                region       = REGION,
            )
            db.session.add(restaurant)
            db.session.flush()
            existing[source_id] = restaurant
            new_r += 1

        for rec in inspections:
            iid = rec['inspection_id']
            if not iid or iid in existing_insp_ids:
                skipped += 1
                continue

            violations = rec.get('violations', [])
            risk       = compute_risk(violations)
            # Use GA's reported score directly when available — it's the
            # number that appears on the official inspection report. Fall
            # back to (100 - sum_points) when missing, then to 0.
            score = rec['score']
            if score is None:
                pts_total = sum(v['points'] for v in violations)
                score = max(0, 100 - pts_total)

            insp = Inspection(
                restaurant_id   = restaurant.id,
                inspection_date = rec['date'],
                source_id       = iid,
                score           = score,
                risk_score      = risk,
                grade           = None,
                result          = score_to_result(score),
                inspection_type = rec.get('purpose') or 'Routine',
                region          = REGION,
            )
            db.session.add(insp)
            db.session.flush()
            new_i += 1
            existing_insp_ids.add(iid)

            for v in violations:
                db.session.add(Violation(
                    inspection_id     = insp.id,
                    violation_code    = v['code'],
                    description       = v['desc'],
                    inspector_notes   = v['notes'],
                    severity          = v['severity'],
                    corrected_on_site = v['corrected'],
                ))

            old_latest = restaurant.latest_inspection_date
            if old_latest is None or rec['date'] > old_latest:
                if old_latest != rec['date']:
                    restaurant.ai_summary = None
                restaurant.latest_inspection_date = rec['date']

    db.session.commit()
    return new_r, new_i, skipped


# ── Main ─────────────────────────────────────────────────────────────────────

# How many facilities (not inspections) to collect before flushing to DB.
# Small enough that a crash only loses a minute or two of work; large enough
# to amortize the transaction overhead. At ~1 facility/sec this is ~8 min/flush.
FLUSH_EVERY = 500


def main():
    full_mode = '--full'    in sys.argv
    dry_run   = '--dry-run' in sys.argv
    debug     = '--debug'   in sys.argv

    days = 7
    start_page = 0
    since_arg: date | None = None
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--since='):
            since_arg = date.fromisoformat(arg.split('=', 1)[1])
        elif arg.startswith('--start-page='):
            start_page = int(arg.split('=', 1)[1])

    today = date.today()
    if full_mode:
        cutoff = since_arg or date(2024, 1, 1)
    else:
        cutoff = since_arg or (today - timedelta(days=days))

    print(f"Fetching Georgia inspections since {cutoff} (today={today})")
    if start_page:
        print(f"  (resuming from page {start_page})")
    if dry_run:
        print("  (--dry-run: no DB writes)")

    # ── Dry-run short circuit ───────────────────────────────────────────────
    if dry_run:
        _dry_run_walk(cutoff, start_page)
        return

    # ── Real run: open app context once, load dedup state once ─────────────
    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation
    app = create_app()

    total_new_r = total_new_i = total_skipped = 0
    facilities_seen = 0
    facilities_in_window = 0
    permit_type_counts: Counter = Counter()

    with app.app_context():
        print("Loading existing Georgia state from DB...")
        existing, seen_slugs, existing_insp_ids = load_db_state(
            db, Restaurant, Inspection
        )
        print(f"  {len(existing):,} existing restaurants, "
              f"{len(existing_insp_ids):,} existing inspections, "
              f"{len(seen_slugs):,} used slugs")

        records_batch: list = []   # list of (facility_dict, [inspection_dict, ...])
        batch_facilities = 0
        page = start_page

        def flush():
            nonlocal records_batch, batch_facilities
            nonlocal total_new_r, total_new_i, total_skipped
            if not records_batch:
                return
            try:
                new_r, new_i, skipped = write_batch(
                    records_batch, db, Restaurant, Inspection, Violation,
                    existing, seen_slugs, existing_insp_ids,
                )
            except Exception as exc:
                db.session.rollback()
                print(f"  !! batch flush failed: {exc} — rolled back, continuing")
                records_batch = []
                batch_facilities = 0
                return
            total_new_r      += new_r
            total_new_i      += new_i
            total_skipped    += skipped
            print(f"  [flush @ page {page}] +{new_r} restaurants, "
                  f"+{new_i} inspections, {skipped} dup "
                  f"(cumulative: {total_new_r:,} / {total_new_i:,} / {total_skipped:,})")
            records_batch = []
            batch_facilities = 0

        while True:
            facs = fetch_facility_page(page)
            if not facs:
                print(f"\nReached end of facility listing at page {page}.")
                break

            # Full run: the listing is sorted by last_inspection_date DESC, so
            # once a full page is past the cutoff, all subsequent pages are too.
            # Incremental run: same rule gets us an early exit once we're past
            # the window.
            page_max_date = max(
                (f['last_date'] for f in facs if f['last_date']), default=None
            )
            if page_max_date is not None and page_max_date < cutoff:
                print(f"\nPage {page} max date {page_max_date} < cutoff — stopping.")
                break

            for f in facs:
                facilities_seen += 1
                if f['last_date'] is None or f['last_date'] < cutoff:
                    continue
                facilities_in_window += 1
                permit_type_counts[f['permit_type'] or 'Unknown'] += 1

                time.sleep(DELAY)
                inspections = fetch_facility_inspections(f['fid_b64'], cutoff)
                if not inspections:
                    continue

                records_batch.append((f, inspections))
                batch_facilities += 1

                if batch_facilities >= FLUSH_EVERY:
                    flush()

            if (page + 1) % 20 == 0:
                pending = sum(len(r[1]) for r in records_batch)
                print(f"  page {page+1}: seen {facilities_seen}, "
                      f"{facilities_in_window} in window, "
                      f"{pending} pending in batch, "
                      f"{total_new_i:,} inspections written so far")

            page += 1
            time.sleep(DELAY)

        # Final flush for anything still in the buffer
        flush()

    print(f"\nDone.")
    print(f"  Walked {page} pages, {facilities_seen} facilities "
          f"({facilities_in_window} in window)")
    print(f"  {total_new_r:,} new restaurants")
    print(f"  {total_new_i:,} new inspections")
    print(f"  {total_skipped:,} skipped (duplicates)")
    if permit_type_counts:
        print("  Permit type breakdown:")
        for pt, n in permit_type_counts.most_common():
            print(f"    {n:5d}  {pt}")


def _dry_run_walk(cutoff: date, start_page: int) -> None:
    """Collect up to ~5 facilities from the live portal and print parsed output."""
    print("Walking portal (dry-run)...")
    collected: list = []
    page = start_page
    while len(collected) < 5:
        facs = fetch_facility_page(page)
        if not facs:
            break
        for f in facs:
            if f['last_date'] is None or f['last_date'] < cutoff:
                continue
            time.sleep(DELAY)
            inspections = fetch_facility_inspections(f['fid_b64'], cutoff)
            if inspections:
                collected.append((f, inspections))
                if len(collected) >= 5:
                    break
        page += 1
        time.sleep(DELAY)

    print(f"\n--dry-run: showing {len(collected)} records:")
    for fac, inspections in collected:
        print(f"  {fac['name']} ({fac.get('permit_type')}) — {fac['source_id']}")
        for ins in inspections[:2]:
            print(f"    {ins['date']} score={ins['score']} "
                  f"({len(ins['violations'])} violations)")
            for v in ins['violations'][:3]:
                note = (v['notes'] or '')[:80]
                print(f"      [{v['severity']}] {v['code']}: {note}")


if __name__ == '__main__':
    main()
