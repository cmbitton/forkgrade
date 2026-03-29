#!/usr/bin/env python3
"""
One-off rescore: recalculate inspection.score for all rows using decay=0.05.

risk_score is already stored correctly (weights unchanged).
We only recompute: score = round(100 * exp(-risk_score * 0.05))

Safe to re-run — idempotent.
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import create_app
from app.db import db
from app.models.inspection import Inspection

DECAY = 0.05
BATCH = 500


def new_score(risk):
    return round(100 * math.exp(-risk * DECAY))


def main():
    app = create_app()
    with app.app_context():
        total = db.session.query(Inspection).filter(
            Inspection.risk_score.isnot(None)
        ).count()
        print(f"Inspections with risk_score: {total:,}")

        updated = 0
        offset  = 0

        while True:
            batch = (
                db.session.query(Inspection)
                .filter(Inspection.risk_score.isnot(None))
                .order_by(Inspection.id)
                .limit(BATCH)
                .offset(offset)
                .all()
            )
            if not batch:
                break

            for insp in batch:
                s = new_score(insp.risk_score)
                if insp.score != s:
                    insp.score = s
                    updated += 1

            db.session.commit()
            offset += BATCH
            print(f"  {min(offset, total):,}/{total:,} processed, {updated:,} changed so far")

        print(f"\nDone. {updated:,} rows updated.")


if __name__ == "__main__":
    main()
