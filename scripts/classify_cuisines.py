#!/usr/bin/env python3
"""
Classify cuisine_type for restaurants missing it.

Phase 1: Rule-based matching (chain names + keywords) — free, instant.
Phase 2: Gemini batch classification via google.genai SDK + ThreadPoolExecutor.

Usage:
    python3 scripts/classify_cuisines.py --dry-run
    python3 scripts/classify_cuisines.py --rules-only
    GEMINI_API_KEY=... python3 scripts/classify_cuisines.py
    GEMINI_API_KEY=... python3 scripts/classify_cuisines.py --gemini-limit=100
    python3 scripts/classify_cuisines.py --region=houston
"""

import json
import logging
import os
import re
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('google_genai').setLevel(logging.WARNING)

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

from google import genai
from google.genai import types

# ── Cuisine taxonomy (must match existing values in DB) ───────────────────────

CUISINES = [
    'American', 'Café / Breakfast', 'Mexican / Latin', 'Chinese', 'Pizza',
    'Japanese / Sushi', 'Italian', 'Greek / Mediterranean', 'Grocery / Market',
    'Asian / Fusion', 'School / Childcare', 'Indian', 'Korean', 'Bar / Pub',
    'Thai', 'Frozen Desserts', 'Seafood', 'Healthcare Facility', 'Catering',
    'Southeast Asian', 'Steakhouse', 'African', 'French', 'Vegetarian', 'Other',
]
CUISINE_SET = set(CUISINES)

# ── Chain prefix lookup ──────────────────────────────────────────────────────

CHAIN_CUISINES = {
    'mcdonalds': 'American', 'burger king': 'American', 'wendys': 'American',
    'whataburger': 'American', 'subway': 'American', 'jersey mikes': 'American',
    'jimmy johns': 'American', 'firehouse subs': 'American',
    'firehouse sub': 'American', 'popeyes': 'American', 'popeye': 'American',
    'chick-fil-a': 'American', 'chick fil a': 'American',
    'jack in the box': 'American', 'sonic': 'American',
    'raising canes': 'American', 'raising cane': 'American',
    'shake shack': 'American', 'five guys': 'American',
    'wingstop': 'American', 'wing stop': 'American',
    'buffalo wild wings': 'American', 'buffalo wild wing': 'American',
    'applebees': 'American', 'applebee': 'American', 'chilis': 'American',
    'outback steakhouse': 'Steakhouse', 'longhorn steakhouse': 'Steakhouse',
    'texas roadhouse': 'Steakhouse', 'saltgrass': 'Steakhouse',
    'pappas bros': 'Steakhouse', 'pappas steakhouse': 'Steakhouse',
    'pappadeaux': 'Seafood', 'pappas seafood': 'Seafood',
    'pappas burger': 'American', 'pappas bar': 'Bar / Pub',
    'luby': 'American', 'golden corral': 'American',
    'black bear diner': 'American',
    'dennys': 'Café / Breakfast', 'denny': 'Café / Breakfast',
    'ihop': 'Café / Breakfast', 'waffle house': 'Café / Breakfast',
    'first watch': 'Café / Breakfast', 'kolache factory': 'Café / Breakfast',
    'kfc': 'American', 'church': 'American', 'churchs': 'American',
    'el pollo loco': 'Mexican / Latin', 'pollo tropical': 'Mexican / Latin',
    'dominos': 'Pizza', 'domino': 'Pizza', 'pizza hut': 'Pizza',
    'papa johns': 'Pizza', 'papa john': 'Pizza',
    'little caesars': 'Pizza', 'little caesar': 'Pizza',
    'pizza inn': 'Pizza', 'cici': 'Pizza', 'pepperonis': 'Pizza',
    'bertucci': 'Pizza',
    'starbucks': 'Café / Breakfast', 'dunkin': 'Café / Breakfast',
    'panera': 'Café / Breakfast', 'coffee': 'Café / Breakfast',
    'einstein bros': 'Café / Breakfast', 'einstein brother': 'Café / Breakfast',
    'la madeleine': 'Café / Breakfast', 'jamba': 'Café / Breakfast',
    'smoothie king': 'Café / Breakfast',
    'taco bell': 'Mexican / Latin', 'chipotle': 'Mexican / Latin',
    'taco cabana': 'Mexican / Latin', 'torchy': 'Mexican / Latin',
    'torchys': 'Mexican / Latin', 'fuzzy': 'Mexican / Latin',
    'fuzzys': 'Mexican / Latin', 'moe': 'Mexican / Latin',
    'panda express': 'Chinese', 'panda exp': 'Chinese',
    'p.f. chang': 'Chinese', 'pf chang': 'Chinese',
    'kura': 'Japanese / Sushi', 'benihana': 'Japanese / Sushi',
    'nobu': 'Japanese / Sushi',
    'olive garden': 'Italian', 'carrabba': 'Italian',
    'macaroni grill': 'Italian',
    'dairy queen': 'Frozen Desserts', 'baskin robbins': 'Frozen Desserts',
    'baskin-robbins': 'Frozen Desserts', 'marble slab': 'Frozen Desserts',
    'cold stone': 'Frozen Desserts', 'rita': 'Frozen Desserts',
    'dutch bros': 'Café / Breakfast',
    'heb': 'Grocery / Market', 'h-e-b': 'Grocery / Market',
    'kroger': 'Grocery / Market', 'walmart': 'Grocery / Market',
    'wal-mart': 'Grocery / Market', 'target': 'Grocery / Market',
    'costco': 'Grocery / Market', 'sams club': 'Grocery / Market',
    "sam's club": 'Grocery / Market', 'whole foods': 'Grocery / Market',
    'trader joe': 'Grocery / Market', 'aldi': 'Grocery / Market',
    'food lion': 'Grocery / Market', 'fiesta mart': 'Grocery / Market',
    'randalls': 'Grocery / Market', 'spec': 'Grocery / Market',
    'memorial hermann': 'Healthcare Facility',
    'houston methodist': 'Healthcare Facility',
    'hca houston': 'Healthcare Facility', 'harris health': 'Healthcare Facility',
    'md anderson': 'Healthcare Facility',
    'texas childrens': 'Healthcare Facility',
    'texas children': 'Healthcare Facility',
    'ut health': 'Healthcare Facility', 'baylor': 'Healthcare Facility',
    'kindred': 'Healthcare Facility', 'encompass health': 'Healthcare Facility',
}

# ── Keyword rules ─────────────────────────────────────────────────────────────

KEYWORD_RULES = [
    (['elementary', 'middle school', 'high school', 'junior high',
      'early college', 'magnet school', 'isd ', ' isd', 'head start',
      'kindercare', 'kinder care', 'learning center', 'child develop',
      'childcare', 'child care', 'montessori', 'preschool', 'pre-school',
      'daycare', 'day care', 'nursery school', 'after school'],
     'School / Childcare'),
    (['hospital', 'medical center', 'med center', 'health system',
      'nursing home', 'rehabilitation', 'rehab center', 'long term care',
      'ltc facility', 'assisted living', 'dialysis', 'surgery center',
      'urgent care', 'clinic ', ' clinic', 'hospice', 'pharmacy',
      'healthcare facility', 'health care'],
     'Healthcare Facility'),
    (['taqueria', 'taquero', 'tacos', 'taco ', ' taco',
      'mexican', 'mexica', 'enchilada', 'tamale', 'tamales',
      'tortilleria', 'tortilla', 'carnitas', 'pupuseria', 'pupusa',
      'salvadoran', 'honduran', 'guatemalan', 'colombian', 'peruvian',
      'empanada', 'panaderia', 'pollos asados', 'pollo asado',
      'birria', 'barbacoa', 'mariscos', 'ceviche', 'burrito',
      'cantina mex', 'tex-mex', 'texmex'],
     'Mexican / Latin'),
    (['sushi', 'ramen', 'izakaya', 'yakitori', 'teriyaki',
      'hibachi', 'japanese', 'udon', 'tempura', 'tonkatsu',
      'omakase', 'sake house'],
     'Japanese / Sushi'),
    (['chinese', 'china ', ' china', 'hong kong', 'dim sum',
      'szechuan', 'sichuan', 'cantonese', 'peking', 'beijing',
      'shanghai', 'wonton', 'dumpling', 'boba', 'bubble tea',
      'kung fu', 'panda '],
     'Chinese'),
    (['korean', 'korea ', ' korea', 'bulgogi', 'bibimbap',
      'galbi', 'bbq korean', 'korean bbq', 'tofu house'],
     'Korean'),
    (['vietnamese', 'viet ', 'pho ', ' pho', 'banh mi',
      'bahn mi', 'thai ', ' thai', 'pad thai', 'lao ', ' lao',
      'cambodian', 'filipino', 'pilipino', 'singaporean',
      'noodle house', 'noodle bar'],
     'Southeast Asian'),
    (['indian', 'india ', ' india', 'curry ', ' curry',
      'tandoor', 'biryani', 'masala', 'tikka', 'naan',
      'pakistani', 'bengali', 'punjabi', 'halal cart',
      'karahi', 'kebab house'],
     'Indian'),
    (['greek', 'mediterranean', 'gyro', 'gyros', 'falafel',
      'hummus', 'shawarma', 'kebab', 'kabob', 'lebanese',
      'turkish', 'moroccan', 'persian', 'middle eastern',
      'afghan', 'israel', 'jewish'],
     'Greek / Mediterranean'),
    (['italian', 'italia', 'ristorante', 'trattoria', 'osteria',
      'pizzeria', 'pasta ', ' pasta', 'lasagna', 'risotto', 'gelato'],
     'Italian'),
    (['pizza'], 'Pizza'),
    (['seafood', 'sea food', 'oyster', 'crab', 'lobster',
      'shrimp ', ' shrimp', 'fish house', 'fish market',
      'crawfish', 'cajun seafood', 'boiling'],
     'Seafood'),
    (['steakhouse', 'steak house', 'chophouse', 'chop house'], 'Steakhouse'),
    (['brewery', 'brewhouse', 'brew pub', 'brewpub',
      ' pub ', ' pub$', 'tavern', 'saloon', 'sports bar',
      'beer garden', 'wine bar', 'cocktail bar'],
     'Bar / Pub'),
    (['cafe', 'café', 'bakery', 'panaderia', 'donut', 'doughnut',
      'pastry', 'espresso', 'brunch', 'breakfast', 'waffle',
      'pancake', 'crepe', 'beignet', 'tea house', 'teahouse',
      'bubble tea', 'smoothie', 'juice bar', 'kolache'],
     'Café / Breakfast'),
    (['ice cream', 'frozen yogurt', 'froyo', 'gelato',
      'creamery', 'snow cone', 'shaved ice', 'paleta'],
     'Frozen Desserts'),
    (['grocery', 'supermarket', 'supermercado', 'food mart',
      'food store', 'food market', ' market', 'mercado',
      'carniceria', 'butcher', 'deli mart', 'convenience store',
      'gas station', 'fuel station', 'food truck park'],
     'Grocery / Market'),
    (['catering', 'caterer', 'food service', 'commissary'], 'Catering'),
    (['african', 'nigerian', 'ethiopian', 'senegalese',
      'ghanaian', 'kenyan', 'somalian', 'cameroonian', 'west african'],
     'African'),
    (['burger', 'barbecue', 'barbeque', ' bbq', 'smokehouse',
      'smoke house', 'southern kitchen', 'soul food',
      'fried chicken', 'hot dog', 'sandwich shop',
      'american grill', 'american kitchen', 'american bistro'],
     'American'),
]


def _clean(name: str) -> str:
    return re.sub(r"['\u2019\-\.\#]", '', name.lower()).strip()


def rule_classify(name: str) -> str | None:
    cleaned = _clean(name)
    for prefix, cuisine in CHAIN_CUISINES.items():
        p = _clean(prefix)
        if cleaned.startswith(p) or f' {p}' in cleaned:
            return cuisine
    for keywords, cuisine in KEYWORD_RULES:
        for kw in keywords:
            if kw in cleaned:
                return cuisine
    return None


# ── Gemini via google.genai SDK + ThreadPoolExecutor ──────────────────────────

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

GEMINI_SYSTEM = f"""\
You classify restaurant/establishment names into cuisine categories.
Input is a JSON object mapping index numbers to restaurant names.
Return ONLY a JSON object mapping the SAME index numbers to cuisine values.
Use exactly one of these cuisine values, or null if truly uncertain:
{json.dumps(CUISINES, indent=2)}

Rules:
- Schools, daycares, learning centers → "School / Childcare"
- Hospitals, clinics, nursing homes → "Healthcare Facility"
- Gas stations with food → "Grocery / Market"
- Hotel restaurants → classify by their cuisine, not "Other"
- Bars without clear food → "Bar / Pub"
- null only if you genuinely cannot determine the cuisine

Example input: {{"0": "McDonalds", "1": "Tokyo Sushi"}}
Example output: {{"0": "American", "1": "Japanese / Sushi"}}\
"""

GEMINI_BATCH = 50
DEFAULT_WORKERS = 40
COMMIT_EVERY = 50   # commit to DB every N batches


def classify_batch(batch_data: list[tuple[int, str]], retries=4):
    """Thread-safe: calls Gemini to classify a batch of names. No DB access."""
    names = [name for _, name in batch_data]
    # Number the inputs so the model stays aligned
    numbered = {str(i): name for i, name in enumerate(names)}
    prompt = json.dumps(numbered)

    for attempt in range(retries):
        try:
            resp = client.models.generate_content(
                model='gemini-2.5-flash-lite',
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=GEMINI_SYSTEM,
                    max_output_tokens=4096,
                    temperature=0.1,
                ),
            )
            text = resp.text.strip()
            if '```' in text:
                match = re.search(r'```(?:json)?(.*?)```', text, re.DOTALL)
                if match:
                    text = match.group(1).strip()

            result = json.loads(text)

            # Handle dict response (keyed by index)
            if isinstance(result, dict):
                out = []
                for i in range(len(names)):
                    v = result.get(str(i)) or result.get(i)
                    out.append(v if v in CUISINE_SET else None)
                return batch_data, out

            # Handle list response — accept if close enough
            if isinstance(result, list):
                # Pad or truncate to match
                while len(result) < len(names):
                    result.append(None)
                result = result[:len(names)]
                return batch_data, [r if r in CUISINE_SET else None for r in result]

        except Exception as e:
            err = str(e)
            if attempt < retries - 1:
                wait = min(2 ** attempt, 8)
                if '429' in err or 'quota' in err.lower():
                    wait = max(wait, 3)
                time.sleep(wait)

    return batch_data, [None] * len(names)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run',      action='store_true')
    parser.add_argument('--rules-only',   action='store_true')
    parser.add_argument('--region',       default=None)
    parser.add_argument('--gemini-limit', type=int, default=None)
    parser.add_argument('--workers',      type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant

    app = create_app()
    with app.app_context():
        q = Restaurant.query.filter(Restaurant.cuisine_type.is_(None))
        if args.region:
            q = q.filter(Restaurant.region == args.region)
        else:
            q = q.filter(Restaurant.region.in_(['houston', 'maricopa']))
        restaurants = q.all()

        print(f'Restaurants missing cuisine: {len(restaurants)}', flush=True)

        # ── Phase 1: Rule-based ──────────────────────────────────────────────
        rule_hits = 0
        unclassified: list[tuple[int, str]] = []
        updates: dict[int, str] = {}

        for r in restaurants:
            cuisine = rule_classify(r.name)
            if cuisine:
                updates[r.id] = cuisine
                rule_hits += 1
            else:
                unclassified.append((r.id, r.name))

        print(f'\nPhase 1 (rules): {rule_hits} classified, '
              f'{len(unclassified)} remaining', flush=True)

        if not args.dry_run and updates:
            for r in restaurants:
                if r.id in updates:
                    r.cuisine_type = updates[r.id]
            db.session.commit()
            print(f'  Saved {rule_hits} rule-based classifications.', flush=True)

        if args.rules_only or not unclassified:
            print('\nDone.', flush=True)
            return

        # ── Phase 2: Gemini ──────────────────────────────────────────────────
        if not client:
            print('\nNo GEMINI_API_KEY — skipping Gemini phase.', flush=True)
            return

        target_data = unclassified
        if args.gemini_limit:
            target_data = target_data[:args.gemini_limit]

        batches = [
            target_data[i:i + GEMINI_BATCH]
            for i in range(0, len(target_data), GEMINI_BATCH)
        ]

        print(f'\nPhase 2 (Gemini): {len(target_data)} restaurants, '
              f'{len(batches)} batches of {GEMINI_BATCH}, '
              f'{args.workers} workers', flush=True)

        gemini_hits = gemini_null = done_count = 0
        pending: dict[int, str] = {}
        t0 = time.time()

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(classify_batch, bd): bd
                for bd in batches
            }
            for future in as_completed(futures):
                batch_data, results = future.result()
                done_count += 1

                for (rid, _), cuisine in zip(batch_data, results):
                    if cuisine:
                        pending[rid] = cuisine
                        gemini_hits += 1
                    else:
                        gemini_null += 1

                # Periodic commit + progress
                if done_count % COMMIT_EVERY == 0 or done_count == len(batches):
                    if not args.dry_run and pending:
                        for rid, cuisine in pending.items():
                            db.session.execute(
                                db.text('UPDATE restaurants SET cuisine_type = :c WHERE id = :id'),
                                {'c': cuisine, 'id': rid},
                            )
                        db.session.commit()
                        pending.clear()

                    elapsed = time.time() - t0
                    nps = (done_count * GEMINI_BATCH) / elapsed if elapsed > 0 else 0
                    print(f'  [{done_count}/{len(batches)}] '
                          f'{gemini_hits} ok, {gemini_null} null '
                          f'| {nps:.0f} names/s = {nps*60:.0f} names/min '
                          f'| {elapsed:.0f}s', flush=True)

        # Final commit
        if not args.dry_run and pending:
            for rid, cuisine in pending.items():
                db.session.execute(
                    db.text('UPDATE restaurants SET cuisine_type = :c WHERE id = :id'),
                    {'c': cuisine, 'id': rid},
                )
            db.session.commit()

        total = rule_hits + gemini_hits
        elapsed = time.time() - t0
        print(f'\nDone. {total}/{len(restaurants)} classified '
              f'({100*total//max(len(restaurants),1)}% coverage) in {elapsed:.1f}s.',
              flush=True)


if __name__ == '__main__':
    main()
