#!/usr/bin/env python3
"""
Scrub the "« Back" navigation text that SAMHD's detail-page parser leaked
into San Antonio restaurant addresses.

Root cause: the detail-page address block ends with a "« Back" link back to
search results. The importer's HTML tag-strip left the literal text glued to
the ZIP, and the end-anchored state/zip regex in parse_address refused to
match, so the whole raw string ended up in the `address` column. The template
then appended city+state again, producing lines like:

    "321 Fredsbg Rd San Antonio, Tx 78201 « Back, San Antonio, TX"

This script rewrites every affected row in place, re-deriving street / city /
state / zip from the merged blob. Safe to re-run (idempotent on clean rows).

Usage:
    python3 scripts/fix_san_antonio_addresses.py --dry-run
    python3 scripts/fix_san_antonio_addresses.py
"""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant


REGION = 'texas'
STATE = 'TX'

_NAV_TAIL_RE = re.compile(
    r'\s*[«‹\u00ab\u2039]+\s*back\s*$',
    re.IGNORECASE,
)
_STATE_ZIP_RE = re.compile(
    r'\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$',
    re.IGNORECASE,
)


def scrub(raw_address: str) -> tuple[str | None, str | None, str | None, str | None]:
    """Return (street, city, state, zip) parsed from a polluted address blob.

    The blob looks like:
        "321 Fredsbg Rd San Antonio, Tx 78201 « Back"
    or occasionally:
        "321 Fredsbg Rd San Antonio, Tx 78201"  (already clean)
    """
    s = (raw_address or '').strip()
    s = _NAV_TAIL_RE.sub('', s).strip()
    # Collapse any whitespace mangling.
    s = re.sub(r'\s+', ' ', s).strip().rstrip(',').strip()
    if not s:
        return None, 'San Antonio', STATE, None

    m = _STATE_ZIP_RE.search(s)
    if not m:
        # Give up — just return the scrubbed blob as street.
        return s, 'San Antonio', STATE, None

    state = m.group(1).upper()
    zip5 = m.group(2)
    before = s[:m.start()].rstrip(', ').strip()

    if ',' in before:
        street_part, city_part = before.rsplit(',', 1)
        city = city_part.strip() or 'San Antonio'
        street = street_part.strip() or None
        return street, city, state, zip5

    # No comma — peel off a trailing "San Antonio" if present.
    m2 = re.search(r'\s+san\s+antonio\s*$', before, re.IGNORECASE)
    if m2:
        street = before[:m2.start()].strip() or None
        return street, 'San Antonio', state, zip5
    return before or None, 'San Antonio', state, zip5


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Print changes without committing.')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        # Match both literal "« Back" and any latin-1 variant.
        candidates = (
            Restaurant.query
            .filter(Restaurant.region == REGION)
            .filter(Restaurant.address.op('~*')(r'[«‹\u00ab\u2039].*back'))
            .all()
        )
        print(f'Found {len(candidates)} Texas restaurants with "« Back" in address.')
        if not candidates:
            return

        changed = 0
        examples = 0
        for r in candidates:
            street, city, state, zip5 = scrub(r.address)
            before = (r.address, r.city, r.state, r.zip)
            new_address = street
            new_city = city or r.city or 'San Antonio'
            new_state = state or r.state or STATE
            new_zip = zip5 or r.zip
            after = (new_address, new_city, new_state, new_zip)
            if before == after:
                continue
            if examples < 10:
                print(f'  id={r.id}')
                print(f'    before: address={before[0]!r} city={before[1]!r} '
                      f'state={before[2]!r} zip={before[3]!r}')
                print(f'    after:  address={after[0]!r} city={after[1]!r} '
                      f'state={after[2]!r} zip={after[3]!r}')
                examples += 1
            if not args.dry_run:
                r.address = new_address
                r.city = new_city
                r.state = new_state
                r.zip = new_zip
            changed += 1

        if args.dry_run:
            print(f'\nWould update {changed} rows.')
            return

        db.session.commit()
        print(f'\nUpdated {changed} rows.')


if __name__ == '__main__':
    main()
