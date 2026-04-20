"""Regression check for the summary truncator.

Pulls 50 random NYC facilities (Brooklyn + Manhattan — longest raw violation
descriptions in the dataset, where the truncator is most likely to mangle a
phrase) and inspects the P3 paragraph for trailing dangling prepositions or
conjunctions. Each hit prints the URL and the offending paragraph.

Exit code 0 = no hits, patch is solid.
Exit code 1 = at least one bad render — investigate before shipping.

Run with:  ./nat-health/bin/python scripts/check_summary_quality.py
Or on prod: fly ssh console -C "python scripts/check_summary_quality.py"
"""
import sys, os, re, random
sys.path.insert(0, os.path.abspath('.'))

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from sqlalchemy import select, func
from app.helpers.summary import build_summary, _TRAILING_BAD


SAMPLE_SIZE = 50
RNG_SEED = None  # set an int for repeatable runs


def main() -> int:
    app = create_app()
    with app.app_context():
        # Pull a wide pool, sample to size — same approach as the cold reads.
        # Restrict to Brooklyn/Manhattan (NYC) where the descriptions are
        # both longest and most numerous.
        insp_count = (
            select(Inspection.restaurant_id, func.count(Inspection.id).label('n'))
            .group_by(Inspection.restaurant_id).subquery()
        )
        pool = (
            db.session.query(Restaurant.id, Restaurant.region, Restaurant.slug)
            .join(insp_count, insp_count.c.restaurant_id == Restaurant.id)
            .filter(Restaurant.region == 'nyc',
                    Restaurant.city.in_(['Brooklyn', 'Manhattan']),
                    insp_count.c.n >= 2)
            .order_by(func.random())
            .limit(SAMPLE_SIZE * 3)
            .all()
        )

        if RNG_SEED is not None:
            random.seed(RNG_SEED)
        random.shuffle(pool)
        sample = pool[:SAMPLE_SIZE]

        bad: list[tuple[str, str]] = []
        checked = 0
        for fid, region, slug in sample:
            out = build_summary(fid)
            if not out or len(out['paragraphs']) < 3:
                continue
            # P3 is the violation-pattern paragraph (or clean_record).
            # Skip facilities where P3 is the clean-record line — it can't
            # have a truncation issue.
            p3 = out['paragraphs'][2]
            if 'No violations' in p3 or 'clean inspection sheet' in p3:
                continue
            checked += 1
            # Look for any "<word> [comma|period|space]<remainder>" where
            # <word> is in the trailing-bad set, AND the dangling word
            # immediately precedes the template's stitched continuation
            # ("comes up most often", "showing up", etc.). Match on the bad
            # word followed by a space and any non-stop word — that's the
            # template glue.
            tokens = re.findall(r"\b[\w'-]+\b", p3.lower())
            # Walk every adjacent pair; if bad-word is followed by a verb-
            # like template phrase, flag it. Cheaper heuristic: just check
            # that no bad word appears immediately before the known closer
            # phrases the violation-pattern slots use.
            url = f'https://forkgrade.fly.dev/{region}/{slug}/'
            for closer in (' comes up most often', ' has been the most frequent',
                           ' is the issue that surfaces most often',
                           ' is the recurring theme', ' accounts for the largest share',
                           ' showing up ', ' recorded ', ' flagged ', ' cited '):
                idx = p3.lower().find(closer)
                if idx <= 0:
                    continue
                prefix_words = re.findall(r"\b[\w'-]+\b", p3[:idx].lower())
                if prefix_words and prefix_words[-1] in _TRAILING_BAD:
                    bad.append((url, p3))
                    break

        print(f'Checked {checked} of {SAMPLE_SIZE} sampled facilities (skipped clean records / short summaries).')
        if not bad:
            print('PASS — zero trailing-preposition cases in P3.')
            return 0
        print(f'FAIL — {len(bad)} broken paragraphs:')
        for url, p3 in bad:
            print()
            print(f'  {url}')
            print(f'  P3: {p3}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
