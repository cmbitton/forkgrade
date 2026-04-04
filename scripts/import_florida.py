#!/usr/bin/env python3
"""
Florida food inspection importer.

Data sources (Florida DBPR — Division of Hotels and Restaurants):

  Historical statewide XLSX (one file per fiscal year):
    https://www2.myfloridalicense.com/sto/file_download/hr/fdinspi_{YYZZ}.xlsx
    e.g. fdinspi_2223.xlsx = FY 2022-2023, fdinspi_2425.xlsx

  Current fiscal year by district (CSV, districts 1-7):
    https://www2.myfloridalicense.com/sto/file_download/extracts/{N}fdinspi.csv

  Violation detail portal (fetched per-inspection in phase 3):
    https://www.myfloridalicense.com/inspectionDetail.asp?InspVisitID={visit_id}&id={license_id}

Scoring (same formula as all regions):
  risk  = High Priority × 3 + Intermediate × 2 + Basic × 1
  score = round(100 × exp(−risk × 0.05))

Usage:
  python3 scripts/import_florida.py              # current district CSVs + portal
  python3 scripts/import_florida.py --full       # historical XLSXs + current CSVs + portal
  python3 scripts/import_florida.py --skip-portal  # skip portal phase (faster, vague descriptions)
  python3 scripts/import_florida.py --dry-run
"""

import asyncio
import csv
import io
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))

# ── Constants ─────────────────────────────────────────────────────────────────

REGION   = 'florida'
DISTRICTS = list(range(1, 8))   # 1 .. 7

CURRENT_URL  = ('https://www2.myfloridalicense.com'
                '/sto/file_download/extracts/{n}fdinspi.csv')

# Historical XLSX files (FY YYZZ = fiscal year 20YY-20ZZ).
# 2324 lives at a different path. 2425 is not archived yet (current FY) — skip it.
HISTORICAL_FILES = [
    ('https://www2.myfloridalicense.com/sto/file_download/hr/fdinspi_2223.xlsx',
     'statewide FY 22-23'),
    ('https://www2.myfloridalicense.com/hr/inspections/fdinspi_2324.xlsx',
     'statewide FY 23-24'),
    ('https://www2.myfloridalicense.com/hr/inspections/fdinspi_2425.xlsx',
     'statewide FY 24-25'),
]

PORTAL_URL   = ('https://www.myfloridalicense.com'
                '/inspectionDetail.asp?InspVisitID={visit_id}&id={license_id}')

WRITE_BATCH       = 1000   # DB write batch size
PORTAL_CONCURRENCY = 50   # async concurrent portal requests
PORTAL_WRITE_EVERY = 2000 # stream writes to DB every N portal completions

_SEV_WEIGHT = {'critical': 3, 'major': 2, 'minor': 1}

# ── HTTP ──────────────────────────────────────────────────────────────────────

def _download(url: str, label: str = '') -> bytes | None:
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={'User-Agent': 'Mozilla/5.0 (compatible)'}
            )
            with urllib.request.urlopen(req, timeout=180) as r:
                data = r.read()
            print(f'  Downloaded {label or url} ({len(data) // 1024:,} KB)', flush=True)
            return data
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f'  404 (skipping): {label or url}')
                return None
            print(f'  HTTP {e.code} on attempt {attempt + 1}', flush=True)
            if attempt < 2:
                time.sleep(5)
        except Exception as exc:
            print(f'  Error on attempt {attempt + 1}: {exc}', flush=True)
            if attempt < 2:
                time.sleep(5)
    return None


def _fetch_html(url: str, retries: int = 3) -> str | None:
    """Sync fetch — used only for bulk file downloads, not portal pages."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={'User-Agent': 'Mozilla/5.0 (compatible)'}
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if attempt < retries - 1:
                time.sleep(2)
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return None


async def _fetch_portal_async(session, visit_id: str, license_id: str,
                               sem: asyncio.Semaphore,
                               retries: int = 3) -> tuple[str, list[dict] | None]:
    """Async fetch of one portal page. Returns (visit_id, violations | None)."""
    import aiohttp
    url = PORTAL_URL.format(visit_id=visit_id, license_id=license_id)
    async with sem:
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status == 404:
                        return visit_id, None
                    if r.status >= 400:
                        if attempt < retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return visit_id, None
                    html = await r.text(errors='replace')
                    viols = parse_portal_page(html)
                    return visit_id, viols or None
            except Exception:
                if attempt < retries - 1:
                    await asyncio.sleep(1)
    return visit_id, None

# ── HTML helpers ──────────────────────────────────────────────────────────────

_TAGS_RE   = re.compile(r'<[^>]+>')
_SPACES_RE = re.compile(r'\s+')

def _strip_html(text: str) -> str:
    return _SPACES_RE.sub(' ', _TAGS_RE.sub(' ', text)).strip()

# ── Portal parsing ────────────────────────────────────────────────────────────

_PORTAL_SEV = {
    'high priority': 'critical',
    'intermediate':  'major',
    'basic':         'minor',
}

_SEV_PREFIX_RE = re.compile(r'^(High Priority|Intermediate|Basic)\s*[-:]\s*', re.IGNORECASE)
# Florida violation codes like "12B-12-4", "03A-01-4", "01B-02-5", "16-21-4"
_VIOL_CODE_RE  = re.compile(r'^\d{1,3}[A-Za-z]?-\d{1,3}-\d{1,2}$')
_TR_RE         = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
_TD_RE         = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL | re.IGNORECASE)


def _portal_severity(desc: str) -> str:
    m = _SEV_PREFIX_RE.match(desc.strip())
    if not m:
        return 'minor'
    return _PORTAL_SEV.get(m.group(1).lower(), 'minor')


def parse_portal_page(html: str) -> list[dict]:
    """
    Extract violations from a Florida DBPR inspectionDetail.asp page.

    Each violation row has three cells:
      Cell 0: FL violation code (e.g. "16-21-4") inside <u> tag via showList link
      Cell 1: spacer (&nbsp;&nbsp;)
      Cell 2: description text starting with severity keyword
               e.g. "Basic - Accumulation of debris on exterior..."
               e.g. "High Priority - Raw animal foods not properly separated..."
    """
    violations = []
    for tr_m in _TR_RE.finditer(html):
        cells = [_strip_html(td_m.group(1)) for td_m in _TD_RE.finditer(tr_m.group(1))]
        if len(cells) < 3:
            continue
        viol_code = cells[0].strip()
        desc      = cells[2].strip()

        # Only violation rows: code matches FL pattern, desc starts with severity keyword
        if not _VIOL_CODE_RE.match(viol_code):
            continue
        if not _SEV_PREFIX_RE.match(desc):
            continue

        violations.append({
            'code':     viol_code,
            'desc':     desc,
            'severity': _portal_severity(desc),
        })
    return violations



# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_date(s) -> date | None:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%Y%m%d'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _int(val) -> int:
    try:
        return int(str(val).strip() or '0')
    except (ValueError, TypeError):
        return 0


def _get(row: dict, *keys: str) -> str:
    """Return the first non-empty value for any of the given keys."""
    for k in keys:
        v = row.get(k)
        if v is not None:
            s = str(v).strip()
            if s and s.lower() not in ('none', 'nan'):
                return s
    return ''


_TITLE_RE = re.compile(r"[A-Za-z]+('[A-Za-z]+)?")

def _title(s: str) -> str:
    """Title-case a string, handling apostrophes (e.g. MCDONALD'S → Mcdonald's)."""
    return _TITLE_RE.sub(lambda m: m.group().capitalize(), s.lower())


def _extract_violation_items(row: dict) -> list[int]:
    """Return item numbers (1-58) that have a non-zero violation count (CSV fallback).
    Handles both CSV format ('Violation 01') and XLSX format ('V_01').
    """
    items = []
    for i in range(1, 59):
        val = row.get(f'Violation {i:02d}') or row.get(f'V_{i:02d}', '')
        if val is not None:
            val = str(val).strip()
        if val and val != '0':
            items.append(i)
    return items


def row_to_record(row: dict) -> dict | None:
    license_num = _get(row,
        'License Number', 'License_Number', 'LICENSE NUMBER', 'LICENSE_NUMBER',
        'LICENSE_NO',
    )
    insp_num = _get(row,
        'Inspection Number', 'Inspection_Number', 'INSPECTION NUMBER', 'INSPECTION_NUMBER',
        'INSP_NO',
    )
    name = _get(row,
        'Business (DBA-Does Business As) Name',
        'Business Name', 'DBA Name', 'DBA_NAME', 'BUSINESS_NAME',
    )
    address = _get(row,
        'Location Address', 'LOCATION ADDRESS', 'LOCATION_ADDRESS',
        'LOC_ADDRESS',
    )
    city = _get(row,
        'Location City', 'LOCATION CITY', 'LOCATION_CITY',
        'LOC_CITY',
    )
    zipcode = _get(row,
        'Location Zip Code', 'LOCATION ZIP', 'LOCATION_ZIP', 'ZIP',
        'LOC_ZIP',
    )
    insp_date_s = _get(row,
        'Inspection Date', 'INSPECTION DATE', 'INSPECTION_DATE',
        'INSP_DATE',
    )
    insp_type = _get(row,
        'Inspection Type', 'INSPECTION TYPE', 'INSPECTION_TYPE',
        'INSPTYPE',
    )

    # For portal URL — CSV: "Inspection Visit ID" / "License ID"
    #                  XLSX: "INSP_VST_ID" / "LIC_ID"
    visit_id   = _get(row,
        'Inspection Visit ID', 'INSPECTION_VISIT_ID', 'Inspection_Visit_ID',
        'INSP_VST_ID',
    )
    license_id = _get(row,
        'License ID', 'LICENSE_ID', 'License_ID',
        'LIC_ID',
    )

    n_high = _int(_get(row,
        'Number of High Priority Violations',
        'High Priority Violations', 'HIGH_PRIORITY_VIOLATIONS',
        'HIGH_VIOL',
    ))
    n_int = _int(_get(row,
        'Number of Intermediate Violations',
        'Intermediate Violations', 'INTERMEDIATE_VIOLATIONS',
        'INTERMED_VIOL',
    ))
    n_basic = _int(_get(row,
        'Number of Basic Violations',
        'Basic Violations', 'BASIC_VIOLATIONS',
        'BASIC_VIOL',
    ))

    if not license_num or not insp_num or not name:
        return None

    insp_date = _parse_date(insp_date_s)
    if not insp_date:
        return None
    if insp_date > date.today() + timedelta(days=1):
        return None

    # Score from instance counts — Florida counts each occurrence separately
    # (e.g. same violation found in two prep areas = 2 instances), which is
    # consistent with how RI and other regions handle repeat occurrences.
    risk  = n_high * 3 + n_int * 2 + n_basic
    score = round(100 * math.exp(-risk * 0.05))

    # Keep item list for CSV fallback when portal is unavailable
    viol_items = _extract_violation_items(row)

    return {
        'license_num': license_num,
        'insp_num':    insp_num,
        'name':        _title(name),
        'address':     _title(address),
        'city':        _title(city),
        'zipcode':     zipcode[:10] if zipcode else '',
        'insp_date':   insp_date,
        'insp_type':   insp_type or 'Routine Inspection',
        'n_high':      n_high,
        'n_int':       n_int,
        'n_basic':     n_basic,
        'risk':        risk,
        'score':       score,
        'visit_id':    visit_id,
        'license_id':  license_id,
        'viol_items':  viol_items,   # fallback: list of item numbers with violations
    }


def parse_csv_data(data: bytes, debug: bool = False) -> list[dict]:
    text   = data.decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(text))
    # Normalize header names — some columns have leading/trailing spaces
    reader.fieldnames = [f.strip() for f in (reader.fieldnames or [])]
    records = []
    for i, row in enumerate(reader):
        row = {k.strip(): v for k, v in row.items()}
        if debug and i == 0:
            print(f'  DEBUG headers: {list(row.keys())[:25]}')
            print(f'  DEBUG row 0:   {dict(list(row.items())[:8])}')
        rec = row_to_record(row)
        if rec:
            records.append(rec)
    return records


def parse_xlsx_data(data: bytes) -> list[dict]:
    # XLSX files are ZIP archives — bail early if it's an HTML error page
    if not data.startswith(b'PK\x03\x04'):
        print(f'  WARNING: file is not a valid XLSX (got {len(data):,} bytes, '
              f'starts with {data[:20]!r}) — skipping', flush=True)
        return []
    import openpyxl
    wb      = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws      = wb.active
    rows    = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h is not None else f'col_{i}'
               for i, h in enumerate(next(rows))]
    records = []
    for raw in rows:
        row = {k.strip(): v for k, v in zip(headers, raw)}
        rec = row_to_record(row)
        if rec:
            records.append(rec)
    wb.close()
    return records

# ── Slug helpers ──────────────────────────────────────────────────────────────

def _make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'florida').lower().replace(' ', '-'))
    return f'{s}-{c}'


def _unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug

# ── Score → result tier ───────────────────────────────────────────────────────

def _result(score: int) -> str:
    if score >= 75:
        return 'Pass'
    if score >= 55:
        return 'Pass with Conditions'
    return 'Fail'

# ── CSV fallback violations (when portal unavailable) ─────────────────────────

def _csv_fallback_violations(rec: dict) -> list[dict]:
    """
    Generate violation records from the CSV item list when portal data is unavailable.
    One record per unique violation item, severity assigned proportionally from
    the aggregate High/Intermediate/Basic counts.
    """
    viol_items = rec.get('viol_items', [])
    n_unique   = len(viol_items)
    n_h, n_i, n_b = rec['n_high'], rec['n_int'], rec['n_basic']
    total = n_h + n_i + n_b

    if n_unique > 0 and total > 0:
        n_crit = round(n_unique * n_h / total)
        n_maj  = round(n_unique * n_i / total)
        n_crit = max(min(n_crit, n_unique), 1 if n_h > 0 else 0)
        n_maj  = max(min(n_maj,  n_unique - n_crit), 1 if n_i > 0 else 0)
        n_min  = n_unique - n_crit - n_maj
    else:
        n_crit = n_maj = n_min = 0

    severities = (['critical'] * n_crit +
                  ['major']    * n_maj  +
                  ['minor']    * max(n_min, 0))

    violations = []
    for item_num, sev in zip(viol_items, severities):
        violations.append({
            'code':     f'FL-{item_num:02d}',
            'desc':     f'Violation Item {item_num:02d}',
            'severity': sev,
        })
    return violations

# ── DB write ──────────────────────────────────────────────────────────────────

def write_batch(records: list[dict], dry_run: bool,
                existing: dict, seen_slugs: set, known_insp: set,
                db, Restaurant, Inspection, Violation) -> tuple[int, int, int]:
    new_r = new_i = skipped = 0

    for rec in records:
        insp_num = rec['insp_num']

        if insp_num in known_insp:
            skipped += 1
            continue

        # Determine violations and effective score
        portal_viols = rec.get('portal_viols')
        if portal_viols is not None:
            # Portal data: real violation codes and descriptions, re-score from them
            violations = portal_viols
            risk  = sum(_SEV_WEIGHT[v['severity']] for v in violations)
            score = round(100 * math.exp(-risk * 0.05))
        else:
            # Fallback: CSV item numbers with proportional severity assignment
            violations = _csv_fallback_violations(rec)
            score = rec['score']
            risk  = rec['risk']

        if dry_run:
            src = '(portal)' if portal_viols is not None else '(csv fallback)'
            print(f"  [dry] {rec['name']} | {rec['city']} | "
                  f"{rec['insp_date']} | score={score} | "
                  f"viols={len(violations)} {src}")
            new_i += 1
            known_insp.add(insp_num)
            continue

        lic = rec['license_num']

        # ── Get or create restaurant ──────────────────────────────────────────
        if lic in existing:
            restaurant = existing[lic]
        else:
            slug = _unique_slug(
                _make_slug(rec['name'], rec['city']),
                seen_slugs,
            )
            restaurant = Restaurant(
                region    = REGION,
                source_id = lic,
                name      = rec['name'],
                slug      = slug,
                address   = rec['address'],
                city      = rec['city'],
                state     = 'FL',
                zip       = rec.get('zipcode', '') or '',
            )
            db.session.add(restaurant)
            db.session.flush()
            existing[lic] = restaurant
            new_r += 1

        known_insp.add(insp_num)

        insp = Inspection(
            restaurant_id   = restaurant.id,
            inspection_date = rec['insp_date'],
            source_id       = insp_num,
            inspection_type = rec['insp_type'],
            score           = score,
            risk_score      = risk,
            result          = _result(score),
        )
        db.session.add(insp)
        db.session.flush()

        for v in violations:
            db.session.add(Violation(
                inspection_id     = insp.id,
                violation_code    = v['code'] or None,
                description       = v['desc'],
                severity          = v['severity'],
                corrected_on_site = False,
            ))

        insp_date = rec['insp_date']
        old_latest = restaurant.latest_inspection_date
        if old_latest is None or insp_date > old_latest:
            if old_latest != insp_date:
                restaurant.ai_summary = None
            restaurant.latest_inspection_date = insp_date

        new_i += 1

    return new_r, new_i, skipped

# ── Import runner ─────────────────────────────────────────────────────────────

def run_import(sources: list[tuple[str, str]], dry_run: bool, skip_portal: bool,
               app, db, Restaurant, Inspection, Violation):
    """
    sources: list of (url, label) to download and parse.
    """

    # ── Download and parse all files ─────────────────────────────────────────
    all_records: list[dict] = []
    for url, label in sources:
        data = _download(url, label)
        if not data:
            continue
        is_xlsx = url.lower().endswith('.xlsx') or url.lower().endswith('.xls')
        if is_xlsx:
            records = parse_xlsx_data(data)
        else:
            records = parse_csv_data(data, debug=(len(all_records) == 0))
        print(f'  Parsed {len(records):,} records from {label}', flush=True)
        all_records.extend(records)

    print(f'\n  Total records collected: {len(all_records):,}', flush=True)

    if not all_records:
        print('Nothing to import.')
        return

    with app.app_context():
        # Pre-load all FL restaurants
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

        # Bulk dedup inspections
        all_insp_ids = [r['insp_num'] for r in all_records]
        if not dry_run and all_insp_ids:
            known_insp = {
                row[0] for row in db.session.execute(
                    db.text('SELECT source_id FROM inspections '
                            'WHERE source_id = ANY(:ids)'),
                    {'ids': all_insp_ids}
                ).fetchall()
            }
        else:
            known_insp = set()

        # Filter to only new inspections
        new_records = [r for r in all_records if r['insp_num'] not in known_insp]
        print(f'  {len(new_records):,} new inspections after dedup', flush=True)

        if not new_records:
            print('\nDone: nothing new.')
            return

        total_r = total_i = total_skip = 0

        def _flush(batch: list[dict]) -> tuple[int, int, int]:
            nonlocal total_r, total_i, total_skip
            new_r, new_i, skip = write_batch(
                batch, dry_run, existing, seen_slugs, known_insp,
                db, Restaurant, Inspection, Violation,
            )
            if not dry_run:
                db.session.commit()
            total_r    += new_r
            total_i    += new_i
            total_skip += skip
            return new_r, new_i, skip

        # ── Phase 3: write records with no portal eligibility immediately ─────
        no_portal = [r for r in new_records
                     if not (r.get('visit_id') and r.get('license_id'))]
        if no_portal and not dry_run:
            for batch_start in range(0, len(no_portal), WRITE_BATCH):
                _flush(no_portal[batch_start: batch_start + WRITE_BATCH])
            print(f'  Wrote {len(no_portal):,} records with no portal ID '
                  f'(CSV fallback)', flush=True)

        # ── Phase 4: async portal fetch + streaming DB writes ─────────────────
        portal_eligible = [r for r in new_records
                           if r.get('visit_id') and r.get('license_id')]
        if portal_eligible and not skip_portal and not dry_run:
            import aiohttp

            total_portal = len(portal_eligible)
            print(f'  Fetching {total_portal:,} portal pages '
                  f'({PORTAL_CONCURRENCY} concurrent)...', flush=True)

            # Build lookup: visit_id → record (for attaching results)
            vid_to_rec: dict[str, dict] = {r['visit_id']: r for r in portal_eligible}
            hits = fetched = 0
            pending: list[dict] = []

            async def _run_portal():
                nonlocal hits, fetched, pending
                sem = asyncio.Semaphore(PORTAL_CONCURRENCY)
                connector = aiohttp.TCPConnector(limit=PORTAL_CONCURRENCY)
                headers = {'User-Agent': 'Mozilla/5.0 (compatible)'}
                async with aiohttp.ClientSession(connector=connector,
                                                 headers=headers) as session:
                    tasks = [
                        _fetch_portal_async(session, r['visit_id'],
                                            r['license_id'], sem)
                        for r in portal_eligible
                    ]
                    for coro in asyncio.as_completed(tasks):
                        visit_id, viols = await coro
                        rec = vid_to_rec.get(visit_id)
                        if rec is not None:
                            rec['portal_viols'] = viols
                            pending.append(rec)
                        if viols:
                            hits += 1
                        fetched += 1

                        if fetched % PORTAL_WRITE_EVERY == 0 or fetched == total_portal:
                            if pending:
                                for b in range(0, len(pending), WRITE_BATCH):
                                    _flush(pending[b: b + WRITE_BATCH])
                                pending = []
                            print(f'  [{fetched:,}/{total_portal:,}] '
                                  f'+{total_r:,} restaurants  '
                                  f'+{total_i:,} inspections  '
                                  f'{hits:,} with portal violations',
                                  flush=True)

            asyncio.run(_run_portal())

            # Flush any remainder
            if pending:
                for b in range(0, len(pending), WRITE_BATCH):
                    _flush(pending[b: b + WRITE_BATCH])

        elif portal_eligible and (skip_portal or dry_run):
            # skip_portal or dry_run: write with CSV fallback
            for batch_start in range(0, len(portal_eligible), WRITE_BATCH):
                _flush(portal_eligible[batch_start: batch_start + WRITE_BATCH])

    print(f'\nDone: +{total_r} restaurants, +{total_i} inspections, '
          f'{total_skip} skipped.')

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    full_mode   = '--full'        in sys.argv
    dry_run     = '--dry-run'     in sys.argv
    skip_portal = '--skip-portal' in sys.argv

    sources: list[tuple[str, str]] = []

    if full_mode:
        sources.extend(HISTORICAL_FILES)

    for n in DISTRICTS:
        url   = CURRENT_URL.format(n=n)
        label = f'district {n} (current)'
        sources.append((url, label))

    if full_mode:
        print(f'=== Florida: Full import '
              f'({len(HISTORICAL_FILES)} historical + {len(DISTRICTS)} current) ===')
    else:
        print(f'=== Florida: Current import ({len(DISTRICTS)} district files) ===')

    if skip_portal:
        print('  (Portal phase skipped)')

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    app = create_app()
    run_import(sources, dry_run, skip_portal, app, db, Restaurant, Inspection, Violation)


if __name__ == '__main__':
    main()
