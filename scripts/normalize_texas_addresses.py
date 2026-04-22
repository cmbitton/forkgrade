#!/usr/bin/env python3
"""
One-shot cleanup: apply app.helpers.address.normalize_street() to every
Texas restaurant's street address.

Expands SA-source abbreviations (Fredsbg → Fredericksburg, Jdtn →
Jourdanton, Brnfls → Braunfels, Maltsbrg → Maltsberger), normalizes
TX-highway casing (Ih → IH, Us → US, Fm → FM, Nw/Se/etc), inserts the
hyphen in interstate names (IH 10 → IH-10), and fixes Mc-casing
("Mcmullen" → "McMullen", also affects Houston).

Idempotent — re-running after completion is a no-op (no rows need
update on the second pass).

Usage:
    python3 scripts/normalize_texas_addresses.py --dry-run
    python3 scripts/normalize_texas_addresses.py
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.helpers.address import normalize_street


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Report counts + up to 20 examples; no DB writes.')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        rows = (
            Restaurant.query
            .filter(Restaurant.region == 'texas')
            .filter(Restaurant.address.isnot(None))
            .filter(Restaurant.address != '')
            .all()
        )
        print(f'{len(rows)} Texas restaurants with non-empty addresses.')

        changes = []
        for r in rows:
            new_addr = normalize_street(r.address)
            if new_addr and new_addr != r.address:
                changes.append((r, r.address, new_addr))

        print(f'{len(changes)} rows would change.')
        for r, old, new in changes[:20]:
            print(f'  id={r.id} {old!r} -> {new!r}')

        if args.dry_run:
            return
        if not changes:
            print('Nothing to do.')
            return

        for r, _, new_addr in changes:
            r.address = new_addr
        db.session.commit()
        print(f'Committed {len(changes)} row updates.')


if __name__ == '__main__':
    main()
