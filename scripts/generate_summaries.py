#!/usr/bin/env python3
"""
Generate AI summaries for all restaurants missing one.

Usage:
    GEMINI_API_KEY=your-key nat-health/bin/python3 scripts/generate_summaries.py
    GEMINI_API_KEY=your-key nat-health/bin/python3 scripts/generate_summaries.py --limit 50

Checkpoints to DB every 100 restaurants. Safe to interrupt and resume.
"""

import os
import sys
import time
import argparse
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


def build_prompt(restaurant, inspections):
    latest = inspections[0] if inspections else None
    total = len(inspections)

    lines = [
        f"Facility: {restaurant.name}",
        f"Location: {restaurant.city}, {restaurant.state}",
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
            if crit:  parts.append(f"{crit} priority violation{'s' if crit > 1 else ''}")
            if major: parts.append(f"{major} priority foundation violation{'s' if major > 1 else ''}")
            if minor: parts.append(f"{minor} core violation{'s' if minor > 1 else ''}")
            lines.append(f"Violations in latest inspection: {', '.join(parts)}")

            # Include up to 3 violation descriptions for context
            descs = [v.description for v in violations if v.description][:3]
            if descs:
                lines.append("Notable violations: " + "; ".join(descs))
        else:
            lines.append("No violations recorded in latest inspection.")

    # Historical pattern across all inspections
    if total > 1:
        scored = [i for i in inspections if i.risk_score is not None]
        if scored:
            avg_risk = sum(i.risk_score for i in scored) / len(scored)
            tier_counts = {'low': 0, 'medium': 0, 'high': 0}
            for i in scored:
                t = i.score_tier
                if t:
                    tier_counts[t] += 1
            dominant = max(tier_counts, key=tier_counts.get)
            lines.append(f"Historical pattern: mostly {dominant}-risk across {len(scored)} scored inspections")

    lines.append("\nWrite a 2-3 sentence summary for this facility's inspection record.")
    return '\n'.join(lines)


def generate_summary(restaurant, inspections, retries=3):
    prompt = build_prompt(restaurant, inspections)
    for attempt in range(retries):
        try:
            resp = client.models.generate_content(
                model='gemini-3.1-flash-lite-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    max_output_tokens=200,
                    temperature=0.4,
                ),
            )
            return resp.text.strip()
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"    Retry {attempt + 1} after error: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                print(f"    Failed after {retries} attempts: {e}")
                return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None,
                        help='Max number of restaurants to process (default: all)')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        query = (
            Restaurant.query
            .filter(Restaurant.ai_summary.is_(None))
            .order_by(Restaurant.id)
        )
        if args.limit:
            query = query.limit(args.limit)
        restaurants = query.all()

        total = len(restaurants)
        print(f"Found {total} restaurants without summaries\n")

        done = 0
        skipped = 0

        for i, restaurant in enumerate(restaurants, 1):
            inspections = (
                Inspection.query
                .options(selectinload(Inspection.violations))
                .filter_by(restaurant_id=restaurant.id)
                .order_by(Inspection.inspection_date.desc())
                .all()
            )

            if not inspections:
                skipped += 1
                continue

            summary = generate_summary(restaurant, inspections)
            if summary:
                restaurant.ai_summary = summary
                done += 1
            else:
                skipped += 1

            # Small delay to avoid rate limits
            time.sleep(0.1)

            if i % 100 == 0 or i == total:
                db.session.commit()
                print(f"  [{i}/{total}] {done} generated, {skipped} skipped")

        db.session.commit()
        print(f"\nDone. {done} summaries generated, {skipped} skipped.")


if __name__ == '__main__':
    main()
