#!/usr/bin/env python3
"""
Generate AI summaries for all restaurants missing one.

Usage:
    GEMINI_API_KEY=your-key nat-health/bin/python3 scripts/generate_summaries.py
    GEMINI_API_KEY=your-key nat-health/bin/python3 scripts/generate_summaries.py --limit 500
    GEMINI_API_KEY=your-key nat-health/bin/python3 scripts/generate_summaries.py --region houston
    GEMINI_API_KEY=your-key nat-health/bin/python3 scripts/generate_summaries.py --workers 80

Parallel: batches of restaurants are fetched from DB, summaries generated concurrently,
then committed together. Safe to interrupt and resume (already-saved summaries are skipped).
"""

import os
import sys
import time
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from google import genai
from google.genai import types
from sqlalchemy.orm import selectinload

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise SystemExit('GEMINI_API_KEY environment variable not set.')

client = genai.Client(api_key=GEMINI_API_KEY)

SYSTEM_PROMPT = """\
You write brief, neutral summaries for a public health inspection website.
Rules:
- 2-3 sentences, factual, based only on the data provided
- Always include the facility name, city, and state naturally in the text
- Do not use words like "pass", "fail", "excellent", "terrible", or make moral judgements
- Do not speculate about causes of violations
- Write in third person
- Use natural prose, not data-report language\
"""

DEFAULT_WORKERS   = 50
DB_BATCH_SIZE     = 500   # restaurants loaded from DB per batch
COMMIT_EVERY      = 100   # commit to DB every N summaries generated


def build_prompt(restaurant, inspections):
    latest = inspections[0] if inspections else None
    total  = len(inspections)

    location = f"{restaurant.city}, {restaurant.state}" if restaurant.city else restaurant.state
    lines = [
        f"Facility: {restaurant.name}",
        f"Location: {location}",
    ]
    if restaurant.cuisine_type:
        lines.append(f"Type: {restaurant.cuisine_type}")
    if restaurant.license_type:
        lines.append(f"License: {restaurant.license_type}")

    lines.append(f"Total inspections on record: {total}")

    if latest:
        tier_label = {'low': 'Low Risk', 'medium': 'Moderate Risk', 'high': 'High Risk'}.get(
            latest.score_tier, 'Unknown'
        )
        lines.append(f"Most recent inspection: {latest.inspection_date} — {tier_label} (score {latest.score})")

        violations = latest.violations
        if violations:
            crit  = sum(1 for v in violations if v.severity == 'critical')
            major = sum(1 for v in violations if v.severity == 'major')
            minor = sum(1 for v in violations if v.severity == 'minor')
            parts = []
            if crit:  parts.append(f"{crit} critical violation{'s' if crit > 1 else ''}")
            if major: parts.append(f"{major} major violation{'s' if major > 1 else ''}")
            if minor: parts.append(f"{minor} minor violation{'s' if minor > 1 else ''}")
            lines.append(f"Violations in latest inspection: {', '.join(parts)}")
            descs = [v.description for v in violations if v.description][:3]
            if descs:
                lines.append("Notable violations: " + "; ".join(descs))
        else:
            lines.append("No violations recorded in latest inspection.")

    if total > 1:
        scored = [i for i in inspections if i.risk_score is not None]
        if scored:
            tier_counts = {'low': 0, 'medium': 0, 'high': 0}
            for i in scored:
                t = i.score_tier
                if t:
                    tier_counts[t] += 1
            dominant = max(tier_counts, key=tier_counts.get)
            lines.append(f"Historical pattern: mostly {dominant}-risk across {len(scored)} scored inspections")

    lines.append("\nWrite a 2-3 sentence summary for this facility's inspection record.")
    return '\n'.join(lines)


def generate_summary(restaurant_id, prompt, retries=4):
    """Thread-safe: only calls Gemini, no DB access."""
    for attempt in range(retries):
        try:
            resp = client.models.generate_content(
                model='gemini-2.0-flash-lite',
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    max_output_tokens=200,
                    temperature=0.4,
                ),
            )
            return restaurant_id, resp.text.strip()
        except Exception as e:
            err = str(e)
            if attempt < retries - 1:
                wait = min(2 ** attempt, 16)
                if '429' in err or 'quota' in err.lower():
                    wait = max(wait, 5)
                time.sleep(wait)
            else:
                return restaurant_id, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit',   type=int,   default=None,  help='Max restaurants to process')
    parser.add_argument('--region',  type=str,   default=None,  help='Only process this region')
    parser.add_argument('--workers', type=int,   default=DEFAULT_WORKERS, help='Parallel API workers')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        q = Restaurant.query.filter(Restaurant.ai_summary.is_(None)).order_by(Restaurant.id)
        if args.region:
            q = q.filter(Restaurant.region == args.region)
        if args.limit:
            q = q.limit(args.limit)

        restaurants = q.all()
        total = len(restaurants)
        print(f"Found {total} restaurants without summaries")
        print(f"Running with {args.workers} parallel workers\n")

        done = skipped = 0
        pending_saves: dict[int, str] = {}  # restaurant_id → summary
        start_time = time.time()

        # Process in DB batches to avoid loading all inspections at once
        for batch_start in range(0, total, DB_BATCH_SIZE):
            batch = restaurants[batch_start:batch_start + DB_BATCH_SIZE]
            batch_ids = [r.id for r in batch]

            # Bulk-load all inspections + violations for this batch in 1 query
            all_inspections = (
                Inspection.query
                .options(selectinload(Inspection.violations))
                .filter(Inspection.restaurant_id.in_(batch_ids))
                .order_by(Inspection.restaurant_id, Inspection.inspection_date.desc())
                .all()
            )
            insp_by_rid: dict[int, list] = defaultdict(list)
            for insp in all_inspections:
                insp_by_rid[insp.restaurant_id].append(insp)

            # Build prompts (main thread, no IO)
            work: list[tuple[int, str]] = []
            for r in batch:
                inspections = insp_by_rid.get(r.id, [])
                if not inspections:
                    skipped += 1
                    continue
                work.append((r.id, build_prompt(r, inspections)))

            # Parallel API calls
            rid_to_restaurant = {r.id: r for r in batch}
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(generate_summary, rid, prompt): rid
                    for rid, prompt in work
                }
                for future in as_completed(futures):
                    rid, summary = future.result()
                    if summary:
                        pending_saves[rid] = summary
                        done += 1
                    else:
                        skipped += 1

                    completed = done + skipped
                    if completed % COMMIT_EVERY == 0 and pending_saves:
                        for save_rid, save_summary in pending_saves.items():
                            rid_to_restaurant[save_rid].ai_summary = save_summary
                        db.session.commit()
                        elapsed = time.time() - start_time
                        rate = done / elapsed * 60 if elapsed > 0 else 0
                        print(f"  [{batch_start + completed}/{total}] "
                              f"{done} generated, {skipped} skipped — "
                              f"{rate:.0f}/min")
                        pending_saves.clear()

            # Commit remaining from this batch
            if pending_saves:
                for save_rid, save_summary in pending_saves.items():
                    rid_to_restaurant[save_rid].ai_summary = save_summary
                db.session.commit()
                pending_saves.clear()

        elapsed = time.time() - start_time
        rate = done / elapsed * 60 if elapsed > 0 else 0
        print(f"\nDone. {done} summaries generated, {skipped} skipped — "
              f"avg {rate:.0f}/min over {elapsed/60:.1f} min.")


if __name__ == '__main__':
    main()
