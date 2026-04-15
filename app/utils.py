"""Shared utility helpers."""

import re

from sqlalchemy import func
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

REGION_INFO = {
    'rhode-island': {
        'display': 'Rhode Island',
        'state_abbr': 'RI',
        'aliases': ['RI'],
    },
    'nyc': {
        'display': 'NYC',
        'state_abbr': 'NY',
        'aliases': ['New York City', 'New York', 'NY'],
    },
    'texas': {
        'display': 'Texas',
        'state_abbr': 'TX',
        'aliases': ['TX', 'Houston', 'San Antonio', 'Houston TX', 'San Antonio TX',
                    'HTX', 'SAT', 'SA'],
    },
    'maricopa': {
        'display': 'Maricopa',
        'state_abbr': 'AZ',
        'aliases': ['Phoenix', 'AZ', 'Arizona', 'PHX', 'Scottsdale', 'Tempe', 'Mesa', 'Chandler', 'Gilbert', 'Glendale'],
    },
    'philadelphia': {
        'display': 'Philadelphia',
        'state_abbr': 'PA',
        'aliases': ['Philly', 'PA', 'Pennsylvania', 'PHL'],
    },
    'florida': {
        'display': 'Florida',
        'state_abbr': 'FL',
        'aliases': ['FL'],
    },
    'chicago': {
        'display': 'Chicago',
        'state_abbr': 'IL',
        'aliases': ['CHI', 'IL', 'Illinois'],
    },
    'boston': {
        'display': 'Boston',
        'state_abbr': 'MA',
        'aliases': ['BOS', 'MA', 'Massachusetts'],
    },
    'georgia': {
        'display': 'Georgia',
        'state_abbr': 'GA',
        'aliases': ['GA', 'Atlanta', 'ATL'],
    },
}


def get_region_display(region: str) -> str:
    """Return a human-readable display name for a region slug."""
    info = REGION_INFO.get(region)
    return info['display'] if info else region.replace('-', ' ').title()


def get_region_aliases(region: str) -> list:
    """Return list of common aliases/abbreviations for a region."""
    info = REGION_INFO.get(region)
    return info['aliases'] if info else []


def get_region_state_abbr(region: str) -> str:
    """Return the state abbreviation for a region (e.g. 'PA' for philadelphia)."""
    info = REGION_INFO.get(region)
    return info.get('state_abbr', '') if info else ''


# Full US state names keyed by abbreviation — used to avoid redundant suffixes
# like "Rhode Island, RI" when the display name already IS the state name.
_FULL_STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming',
}


def region_location(region: str) -> str:
    """Return a natural location string for use in meta descriptions.

    Appends state abbreviation only when it adds information — e.g.
    'Philadelphia, PA' or 'Maricopa County, AZ', but NOT 'Rhode Island, RI'
    (the display name already is the state name) or 'Florida, FL'.
    """
    info = REGION_INFO.get(region)
    if not info:
        return region.replace('-', ' ').title()
    display = info['display']
    abbr = info.get('state_abbr', '')
    if not abbr:
        return display
    # Skip the suffix if the display name already IS the full state name
    if _FULL_STATE_NAMES.get(abbr, '').lower() == display.lower():
        return display
    return f'{display}, {abbr}'


_STOP_WORDS = {'a', 'an', 'and', 'at', 'by', 'for', 'in', 'of', 'or', 'the', 'to'}


def search_restaurants(q, region=None, city=None, sort='date', sort_dir=None, page=1, per_page=25):
    """Return (rows, total) for a name search.

    rows  — list of (Restaurant, Inspection|None) tuples
    total — total matching count (for pagination)

    region: if given, scopes to that region only.
    city:   if given (along with region), scopes to that city. Matched against
            the raw Restaurant.city column, so pass the canonical city name as
            stored in the DB (the same string the city page route resolves
            from the slug).
    sort:   'date', 'score', 'name'
    sort_dir: 'asc' or 'desc' (defaults: date=desc, score=desc, name=asc)
    """
    _defaults = {'date': 'desc', 'score': 'desc', 'name': 'asc'}
    if sort_dir is None:
        sort_dir = _defaults.get(sort, 'desc')
    # Normalize: drop apostrophes (both ASCII and curly U+2019) BEFORE the
    # alphanum split so "domino's" collapses to a single "dominos" token instead
    # of ["domino", "s"]. The bare "s" would otherwise ILIKE-match nearly every
    # restaurant name on the second token, swamping the real result.
    q_norm = q.replace("'", '').replace('\u2019', '')
    tokens = re.sub(r'[^a-zA-Z0-9]+', ' ', q_norm).split()
    tokens = [t for t in tokens if t.lower() not in _STOP_WORDS]
    if not tokens:
        return [], 0

    # Match against the name with apostrophes stripped on the SQL side too —
    # otherwise "dominos" wouldn't find "Domino's Pizza" because the literal
    # ' breaks the ILIKE substring. REPLACE is a cheap deterministic byte
    # substitution (basically memcpy), nothing like the regexp_replace we used
    # to do here. We give up any pg_trgm GIN index on Restaurant.name, but
    # there isn't one in prod and full scans on this column were already the
    # path. Both apostrophe variants get stripped in one nested REPLACE.
    name_col = func.replace(func.replace(Restaurant.name, "'", ''), '\u2019', '')
    name_filters = db.and_(*(name_col.ilike(f'%{t}%') for t in tokens))

    # Count on restaurants only — no outerjoin needed
    count_q = Restaurant.query.filter(name_filters)
    if region:
        count_q = count_q.filter(Restaurant.region == region)
    if city:
        count_q = count_q.filter(Restaurant.city == city)
    total = count_q.count()

    query = (
        db.session.query(Restaurant, Inspection)
        .outerjoin(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
            Inspection.not_future(),
        ))
        .filter(name_filters)
    )

    if region:
        query = query.filter(Restaurant.region == region)
    if city:
        query = query.filter(Restaurant.city == city)

    if sort == 'score':
        score_col = Inspection.score.desc() if sort_dir == 'desc' else Inspection.score.asc()
        query = query.order_by(
            db.case((Inspection.score.is_(None), 1), else_=0),
            score_col,
        )
    elif sort == 'name':
        query = query.order_by(Restaurant.name.asc() if sort_dir == 'asc' else Restaurant.name.desc())
    else:  # date
        date_col = Inspection.inspection_date.desc() if sort_dir == 'desc' else Inspection.inspection_date.asc()
        query = query.order_by(
            db.case((Inspection.inspection_date.is_(None), 1), else_=0),
            date_col,
        )

    rows = query.offset((page - 1) * per_page).limit(per_page).all()
    return rows, total
