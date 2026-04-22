#!/usr/bin/env python3
"""
Dedupe restaurant rows that represent the same physical establishment
under different license numbers (e.g., license renewals, permit reissues).

Two-pass strategy:

  Pass A — Sequential licenses:
    A group of rows at the same address with similar names is eligible
    when every row's inspection date range is pairwise disjoint from
    every other row in the group. This is the classic "old license went
    dormant, new one took over" pattern.

  Pass B — Shadow licenses inside a dominant row:
    A group is also eligible if exactly one row is "dominant" (≥5
    inspections AND span ≥365 days) and every other row has ≤2
    inspections whose dates are either contained within the dominant
    range or entirely disjoint from it. This catches cases where a
    short-lived shadow license briefly coexisted with the main one
    (e.g. Penny's Noodle Shop on N Damen Ave).

Rows at the same address are considered to have "similar names" when
any of the following hold (union-find clustering):
  - normalized names are equal
  - one normalized name contains the other (both ≥ 8 chars)
  - difflib SequenceMatcher ratio ≥ 0.85

The canonical row = the row with the most recent last inspection
(ties: most inspections, then lowest id). Inspection FKs are reassigned
to the canonical; stale rows are deleted. The canonical's
latest_inspection_date is refreshed and ai_summary is cleared so it
regenerates against the new inspection set.

Usage:
    nat-health/bin/python3 scripts/dedup_license_renewals.py                    # dry run, all regions
    nat-health/bin/python3 scripts/dedup_license_renewals.py --region=chicago   # dry run, one region
    nat-health/bin/python3 scripts/dedup_license_renewals.py --execute          # apply
    nat-health/bin/python3 scripts/dedup_license_renewals.py --verbose          # print every group

DO NOT run --execute concurrently with any importer that writes to the same
region. The importers snapshot `existing = {source_id: row}` and
`seen_slugs` once at the start of write_to_db and cache them in-memory for
the rest of the batch. Dedup's DELETEs invalidate that snapshot — the
importer will happily try to UPDATE rows that no longer exist or INSERT
rows whose slugs were silently freed, producing spurious duplicates and
uq_restaurant_region_slug violations. Stop the importer first, run dedup,
then resume.
"""

import argparse
import datetime as _dt
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

_NORM_RE = re.compile(r'[^a-z0-9]+')

MIN_DOMINANT_INSP = 5           # dominant row must have at least this many inspections
MIN_DOMINANT_SPAN_DAYS = 365    # and span at least this many days
MAX_SHADOW_INSP = 2             # shadow rows can have at most this many inspections
NAME_SIM_THRESHOLD = 0.85       # SequenceMatcher ratio for fuzzy name match
MIN_SUBSTRING_LEN = 8           # both names must be at least this long for substring match


def norm(s: str | None) -> str:
    if not s:
        return ''
    return _NORM_RE.sub('', s.lower())


def names_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= MIN_SUBSTRING_LEN and len(b) >= MIN_SUBSTRING_LEN:
        if a in b or b in a:
            return True
    if SequenceMatcher(None, a, b).ratio() >= NAME_SIM_THRESHOLD:
        return True
    return False


def pairwise_disjoint(ranges: list[tuple]) -> bool:
    n = len(ranges)
    for i in range(n):
        a_first, a_last = ranges[i]
        for j in range(i + 1, n):
            b_first, b_last = ranges[j]
            if a_first <= b_last and b_first <= a_last:
                return False
    return True


def _sort_key(row):
    last = row.last_date or _dt.date.min
    return (-last.toordinal(), -row.insp_count, row.id)


def classify(rows) -> str | None:
    """Return 'A' (sequential), 'B-candidate' (may be shadows), or None."""
    with_insp = [r for r in rows if r.insp_count > 0]
    if len(with_insp) < 2:
        return None

    # Pass A: all pairwise disjoint
    ranges = [(r.first_date, r.last_date) for r in with_insp]
    if pairwise_disjoint(ranges):
        return 'A'

    # Pass B candidate: exactly one dominant row absorbing shadows
    dominants = [
        r for r in with_insp
        if r.insp_count >= MIN_DOMINANT_INSP
        and (r.last_date - r.first_date).days >= MIN_DOMINANT_SPAN_DAYS
    ]
    if len(dominants) != 1:
        return None
    dom = dominants[0]
    for r in with_insp:
        if r.id == dom.id:
            continue
        if r.insp_count > MAX_SHADOW_INSP:
            return None
        # Shadow's range must be fully contained in dom OR fully disjoint.
        contained = dom.first_date <= r.first_date and r.last_date <= dom.last_date
        disjoint = r.last_date < dom.first_date or r.first_date > dom.last_date
        if not (contained or disjoint):
            return None
    # Pass B is only confirmed after verifying no shared inspection days
    # (see confirm_pass_b below). Sub-venues at hotels / stadiums typically
    # get co-inspected on the same day, which is our reject signal.
    return 'B-candidate'


def confirm_pass_b(cluster, insp_dates_by_restaurant: dict[int, list]) -> bool:
    """Reject Pass B candidates that share any inspection day across rows
    in the cluster — that pattern indicates a multi-license co-inspection
    (e.g. a hotel with multiple kitchens inspected in one visit), which
    means these are real sub-venues, not license renewals.

    Exception: when every row in the cluster has the *exact same*
    normalized name AND the same license_type (or license_type isn't
    populated for the region) AND no row shares more than one inspection
    day with the rest of the cluster, treat the shared day as a
    license-rollover crossover (old and new license inspected on the
    transition visit) or an opening-day stub license, not a sub-venue
    co-inspection.

    The "at most one shared day" gate is what separates a one-off
    crossover (Pantanos: 1 shared day between main and renewed license)
    from an ongoing sub-license pattern (Chick-fil-A / Texas Roadhouse
    / NXP: multiple shared days across repeated co-inspections — those
    are distinct operations at one address, not renewals).

    The license_type gate handles Maricopa/Georgia data specifically:
    same name and address but different license_types (e.g. "Eating &
    Drinking" + "Micro Market" for Aramark, "Retail Food" + "Eating &
    Drinking" for gopuff) indicate genuinely distinct operations — a
    cafe and a vending micro market at the same corporate address are
    two businesses, not a renewal. Fuzzy / substring-matched clusters,
    license_type mismatches, and identical-name clusters with >1 shared
    day on any row all fall through to the strict guard."""
    if (len({norm(r.name) for r in cluster}) == 1
            and len({r.license_type or '' for r in cluster}) == 1):
        max_shared = 0
        for r in cluster:
            others = set()
            for o in cluster:
                if o.id != r.id:
                    others.update(insp_dates_by_restaurant.get(o.id, ()))
            shared = sum(1 for d in insp_dates_by_restaurant.get(r.id, ())
                         if d in others)
            if shared > max_shared:
                max_shared = shared
        if max_shared <= 1:
            return True
    seen_days: dict = {}
    for r in cluster:
        for d in insp_dates_by_restaurant.get(r.id, ()):
            if d in seen_days and seen_days[d] != r.id:
                return False
            seen_days[d] = r.id
    return True


def cluster_by_name(rows_at_addr):
    """Union-find over name similarity within a single address group."""
    parent = list(range(len(rows_at_addr)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    keys = [norm(r.name) for r in rows_at_addr]
    n = len(rows_at_addr)
    for i in range(n):
        for j in range(i + 1, n):
            if names_match(keys[i], keys[j]):
                union(i, j)

    clusters: dict[int, list] = defaultdict(list)
    for idx, row in enumerate(rows_at_addr):
        clusters[find(idx)].append(row)
    return [c for c in clusters.values() if len(c) >= 2]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--execute', action='store_true',
                    help='Apply changes (default is dry-run)')
    ap.add_argument('--region', default=None,
                    help='Only process this region')
    ap.add_argument('--verbose', action='store_true',
                    help='Print every eligible group')
    ap.add_argument('--sample', type=int, default=10,
                    help='How many groups to print in non-verbose mode')
    ap.add_argument('--filter', default=None,
                    help='Only show sample groups whose canonical name ILIKE this')
    args = ap.parse_args()

    dry_run = not args.execute

    from sqlalchemy import func, text

    from app import create_app
    from app.db import db
    from app.models.inspection import Inspection
    from app.models.restaurant import Restaurant

    app = create_app()
    with app.app_context():
        q = (
            db.session.query(
                Restaurant.id,
                Restaurant.region,
                Restaurant.name,
                Restaurant.address,
                Restaurant.source_id,
                Restaurant.license_type,
                Restaurant.latest_inspection_date,
                func.min(Inspection.inspection_date).label('first_date'),
                func.max(Inspection.inspection_date).label('last_date'),
                func.count(Inspection.id).label('insp_count'),
            )
            .outerjoin(Inspection, Inspection.restaurant_id == Restaurant.id)
            .filter(Restaurant.address.isnot(None), Restaurant.address != '')
            .group_by(Restaurant.id)
        )
        if args.region:
            q = q.filter(Restaurant.region == args.region)

        # Group by (region, address_key). Name clustering happens inside.
        addr_groups: dict[tuple, list] = defaultdict(list)
        for row in q.all():
            key = (row.region, norm(row.address))
            addr_groups[key].append(row)

        passA_groups = []
        passB_candidates = []
        skipped_groups = 0

        for (_, _), rows_at_addr in addr_groups.items():
            if len(rows_at_addr) < 2:
                continue
            for cluster in cluster_by_name(rows_at_addr):
                cls = classify(cluster)
                if cls == 'A':
                    passA_groups.append(cluster)
                elif cls == 'B-candidate':
                    passB_candidates.append(cluster)
                else:
                    skipped_groups += 1

        # Batch-fetch inspection dates for Pass B candidate restaurants and
        # reject clusters whose rows share any inspection day (sub-venue signal).
        candidate_ids = [r.id for g in passB_candidates for r in g]
        insp_dates_by_rid: dict[int, list] = defaultdict(list)
        if candidate_ids:
            insp_rows = (
                db.session.query(Inspection.restaurant_id, Inspection.inspection_date)
                .filter(Inspection.restaurant_id.in_(candidate_ids))
                .all()
            )
            for rid, d in insp_rows:
                insp_dates_by_rid[rid].append(d)

        passB_groups = []
        passB_rejected = 0
        for cluster in passB_candidates:
            if confirm_pass_b(cluster, insp_dates_by_rid):
                passB_groups.append(cluster)
            else:
                passB_rejected += 1
        skipped_groups += passB_rejected

        all_mergeable = [(g, 'A') for g in passA_groups] + [(g, 'B') for g in passB_groups]
        total_rows_to_delete = sum(len(g) - 1 for g, _ in all_mergeable)
        total_insp_to_move = 0
        for g, _ in all_mergeable:
            sorted_rows = sorted(g, key=_sort_key)
            total_insp_to_move += sum(r.insp_count for r in sorted_rows[1:])

        scope = f"({args.region})" if args.region else "(all regions)"
        print(f"=== Dedup scan {scope} ===")
        print(f"Clusters that look like dupes:   {len(all_mergeable) + skipped_groups:>7,}")
        print(f"  Skipped (ambiguous/sub-venue): {skipped_groups:>7,}")
        print(f"  Mergeable via Pass A (seq):    {len(passA_groups):>7,}")
        print(f"  Mergeable via Pass B (shadow): {len(passB_groups):>7,}")
        print(f"Restaurant rows to delete:       {total_rows_to_delete:>7,}")
        print(f"Inspections to reassign:         {total_insp_to_move:>7,}")

        by_region = defaultdict(lambda: [0, 0, 0, 0, 0])  # a_groups, b_groups, del, insp, _
        for cluster, cls in all_mergeable:
            region = cluster[0].region
            sorted_rows = sorted(cluster, key=_sort_key)
            if cls == 'A':
                by_region[region][0] += 1
            else:
                by_region[region][1] += 1
            by_region[region][2] += len(cluster) - 1
            by_region[region][3] += sum(r.insp_count for r in sorted_rows[1:])
        print()
        print(f"{'region':<15} {'passA':>7} {'passB':>7} {'del':>7} {'insp→':>7}")
        for region, (a, b, d, i, _) in sorted(by_region.items(), key=lambda kv: -kv[1][2]):
            print(f"{region:<15} {a:>7,} {b:>7,} {d:>7,} {i:>7,}")

        # Sample/verbose output
        filtered = all_mergeable
        if args.filter:
            needle = args.filter.lower()
            filtered = [
                (g, c) for (g, c) in all_mergeable
                if any(needle in (r.name or '').lower() for r in g)
            ]
        sample_n = len(filtered) if args.verbose else min(args.sample, len(filtered))
        if sample_n:
            print()
            print(f"=== Sample ({sample_n} groups) ===")
        for cluster, cls in filtered[:sample_n]:
            sorted_rows = sorted(cluster, key=_sort_key)
            canonical = sorted_rows[0]
            print(f"[{canonical.region}] [pass {cls}] {canonical.name} @ {canonical.address}")
            for r in sorted_rows:
                marker = 'KEEP  ' if r.id == canonical.id else 'MERGE→'
                last = r.last_date.isoformat() if r.last_date else '—'
                first = r.first_date.isoformat() if r.first_date else '—'
                nm = r.name if len(r.name) <= 55 else (r.name[:52] + '...')
                print(f"  {marker} id={r.id:<7} license={r.source_id or '—':<10} "
                      f"insp={r.insp_count:>3} range={first}..{last}  {nm}")

        if dry_run:
            print()
            print("DRY RUN — no changes written. Re-run with --execute to apply.")
            return

        print()
        print(f"EXECUTING merges...")
        merged_groups = 0
        for cluster, _cls in all_mergeable:
            sorted_rows = sorted(cluster, key=_sort_key)
            canonical = sorted_rows[0]
            stale_ids = [r.id for r in sorted_rows[1:]]
            if not stale_ids:
                continue
            db.session.execute(
                text("UPDATE inspections SET restaurant_id = :cid "
                     "WHERE restaurant_id = ANY(:stale)"),
                {"cid": canonical.id, "stale": stale_ids},
            )
            db.session.execute(
                text("DELETE FROM restaurants WHERE id = ANY(:stale)"),
                {"stale": stale_ids},
            )
            max_date = (
                db.session.query(func.max(Inspection.inspection_date))
                .filter(Inspection.restaurant_id == canonical.id)
                .scalar()
            )
            db.session.execute(
                text("UPDATE restaurants SET latest_inspection_date = :d, "
                     "ai_summary = NULL WHERE id = :id"),
                {"d": max_date, "id": canonical.id},
            )
            merged_groups += 1
            if merged_groups % 200 == 0:
                db.session.commit()
                print(f"  {merged_groups:,}/{len(all_mergeable):,} groups merged")
        db.session.commit()
        print(f"Done. Merged {merged_groups:,} groups, "
              f"deleted {total_rows_to_delete:,} rows, "
              f"reassigned {total_insp_to_move:,} inspections.")


if __name__ == '__main__':
    main()
