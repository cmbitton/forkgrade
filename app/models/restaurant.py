import re
from app.db import db


_SUFFIX_RE = re.compile(
    r',?\s+(?:LLC\.?|INC\.?|CORP\.?|L\.L\.C\.)$',
    re.IGNORECASE
)

# Strip legal entity name before D/B/A — keep only the trade name after it
_DBA_RE = re.compile(r'^.+\bD[/.]?B[/.]?A\b\.?\s+', re.IGNORECASE)


# Philadelphia source data packs names as "owner / legal / DBA / facility type / code",
# joined by slashes. The full chain reads as spam across the page (h1, breadcrumb,
# title, FAQ repeated 5+ times), so extract the DBA for display.

# Vendor/licence codes: V00959, #V05193, V-03000, ID# V00762, #55 & 56, 03655, -03001
_PHILLY_CODE_RE = re.compile(
    r'^(?:ID\s*#?:?\s*)?[-#]?V[\s\-#]?\d+[A-Z0-9\-]*$'
    r'|^#\s*\d+(?:\s*(?:&|and)\s*#?\s*\d+)?$'
    r'|^-?\d+$',
    re.IGNORECASE,
)
# Philly facility-type tags (not real business names).
_PHILLY_FACILITY_RE = re.compile(
    r'^(?:Permanent\s+Special\s+Event(?:\s+(?:Food\s+)?Vendor)?'
    r'|Special\s+Events?\s+Permanent\s+Vendor'
    r'|Mobile\s+Food(?:\s+(?:Unit|Vendor))?'
    r'|Commissary(?:\s*#?\s*\d+)?'
    r'|Tent\s*#?\s*\d+'
    r'|Roamer'
    r'|Curbstand(?:\s*#?\s*\d+(?:\s*(?:&|and)\s*#?\s*\d+)?)?'
    r')$',
    re.IGNORECASE,
)
# "Lastname, Firstname" owner pattern. Permissive on both sides so compound
# surnames ("Jimenez Ramirez, Griselda") and "&"/middle-name firstnames match.
_PHILLY_OWNER_COMMA_RE = re.compile(
    r"^[A-Z][A-Za-z.\-' ]*,\s*[A-Z][A-Za-z.\-' &]+$"
)
# 2- or 3-word "FirstName LastName" shape — weaker owner signal used only to tie-break
# between two plain-trade-looking segments (no digits, no &, each word title-case).
_PHILLY_BARE_NAME_RE = re.compile(
    r"^[A-Z][a-z'.\-]+(?:\s+[A-Z][a-z'.\-]+){1,2}$"
)
# Tokens that, when present in a segment, flag it as a trade/business name
# rather than a person name. Used only to break ties when the first segment
# looks like a bare person name (e.g. 'Clifton White/Seafood Soul/-V07203').
_PHILLY_TRADE_WORD_RE = re.compile(
    r'\b(?:Food|Foods|Cart|Truck|Kitchen|Cafe|Caf\xe9|Grill|Restaurant|Bakery'
    r'|Bistro|Pizza|Gyro|Halal|Taste|Fruit|Salad|Smoothie|Smoothies|Market'
    r'|Shop|Store|BBQ|Deli|Bagel|Bagels|Juice|Cream|Donuts?|Burger|Burgers'
    r'|Seafood|Chicken|Catering|Brewing|Brewery|Vineyards|Cider|Treats'
    r'|Concessions|Dishes|Cakes|Cupcakes|Tacos|Burritos|Sushi|Ramen|Noodles?'
    r'|Coffee|Wings|Steaks|Lunch|Express|Soul|Eats|Cuisine|Pretzel'
    r'|Cheesesteak|Hoagies?|Pops)\b',
    re.IGNORECASE,
)


def _philly_classify(seg: str) -> str:
    if _PHILLY_CODE_RE.match(seg):
        return 'code'
    if _PHILLY_FACILITY_RE.match(seg):
        return 'facility'
    if _PHILLY_OWNER_COMMA_RE.match(seg):
        return 'owner'
    if _SUFFIX_RE.search(seg):
        return 'legal'
    return 'other'


def _philly_split_outside_parens(s: str) -> list[str]:
    """Split on '/' but ignore slashes inside parentheses. Keeps names like
    'Vie (Event Space w/ Bars)' and 'Nanee's Kitchen (Indian/Pakistani)' intact."""
    out, cur, depth = [], [], 0
    for ch in s:
        if ch == '(':
            depth += 1
            cur.append(ch)
        elif ch == ')':
            depth = max(0, depth - 1)
            cur.append(ch)
        elif ch == '/' and depth == 0:
            out.append(''.join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append(''.join(cur))
    return out


def _parse_philly_name(name: str) -> str:
    """Extract the DBA from Philly's slash-packed legal-chain names.

    Philadelphia inspection data joins segments like
    'Gibson, Shelia / Pastry by Her Llc / Pastry by Her / Permanent Special Event Vendor'.
    The chain gets repeated in the h1, title, breadcrumb, AI summary, and every
    FAQ question — unreadable at ~80 chars. Pull out just the trade name.

    Only collapses when the chain carries recognizable metadata (owner-comma
    segment, legal suffix, code, or facility-type tag). Dual-brand names like
    'KFC / Taco Bell' or 'Mochinut/Ebisu' have no such markers and pass through
    untouched.
    """
    if '/' not in name:
        return name
    segments = [s.strip() for s in _philly_split_outside_parens(name)]
    segments = [s for s in segments if s]
    if len(segments) < 2:
        return name

    kinds = [_philly_classify(s) for s in segments]
    if not any(k in ('owner', 'code', 'facility', 'legal') for k in kinds):
        return name  # no chain markers — likely a dual-brand shared location

    # Canonical pattern is [owner?] / [legal] / [dba] / [facility/code*].
    # Prefer the 'other' segment immediately following a legal segment.
    for i, k in enumerate(kinds):
        if k == 'legal':
            for j in range(i + 1, len(kinds)):
                if kinds[j] == 'other':
                    return segments[j]
            return _SUFFIX_RE.sub('', segments[i]).strip().rstrip(',').strip()

    # No legal segment — take the first 'other'. If the first 'other' looks
    # like a bare person name (e.g. 'Ron Bzdewka') and the next 'other' clearly
    # doesn't (contains a lowercase connector like 'of'/'by', a digit, '&',
    # parens, or has 3+ words), the first is likely owner-as-trade-name.
    others = [i for i, k in enumerate(kinds) if k == 'other']
    if not others:
        # All segments are owner/code/facility — nothing to show, return first.
        return segments[0]
    first = others[0]
    if len(others) >= 2 and _PHILLY_BARE_NAME_RE.match(segments[first]):
        nxt = segments[others[1]]
        looks_like_trade = (
            len(nxt.split()) >= 3
            or any(c.isdigit() for c in nxt)
            or '&' in nxt
            or '(' in nxt
            or re.search(r'\b(?:of|by|the|and|on|with|\'?s)\b', nxt, re.IGNORECASE)
            or _PHILLY_TRADE_WORD_RE.search(nxt) is not None
        )
        if looks_like_trade:
            return nxt
    return segments[first]


def _aka_key(s: str) -> str:
    """Normalize a name for redundancy comparison: lowercase, strip legal
    suffixes, collapse whitespace, drop non-alphanumerics. Lets us decide
    whether two halves of `X — Y` carry the same content."""
    s = _SUFFIX_RE.sub('', s).strip().rstrip(',').strip()
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '', s)
    return s


def _collapse_redundant_aka(name: str) -> str:
    """Drop the trailing ` — aka` when it duplicates the dba half.

    Chicago imports build names as `{dba} — {aka}` when the two differ. In
    practice most pairs differ only by LLC/INC suffix, a trailing license
    number, or whitespace, and the doubled form reads as spam across a
    page that mentions the name 9 times. Keep the aka only when it adds
    substantive information.
    """
    if ' — ' not in name:
        return name
    left, _, right = name.partition(' — ')
    lkey = _aka_key(left)
    rkey = _aka_key(right)
    if not lkey or not rkey:
        return name
    # Same normalized content, or one is a substring of the other and the
    # longer half carries only a license number / store code tacked on.
    if lkey == rkey or lkey.startswith(rkey) or rkey.startswith(lkey):
        return left if len(lkey) >= len(rkey) else right
    return name


_SMALL_WORDS = frozenset([
    'a', 'an', 'the', 'and', 'but', 'or', 'nor', 'for',
    'of', 'on', 'in', 'at', 'to', 'by', 'up', 'as',
])

# Short acronyms that arrive in source data as all-caps and should stay that
# way through title-casing. Without this, Florida's "YH SEAFOOD CLUBHOUSE"
# became "Yh Seafood Clubhouse" and Philadelphia's "CVS PHARMACY" became
# "Cvs Pharmacy". Limited to short (<=4 char) tokens where the source is
# unambiguous — longer strings like "STOP" or "MART" aren't acronyms and
# should title-case normally.
_UPPER_ACRONYMS = frozenset([
    'BBQ', 'BB', 'BJ', 'CVS', 'DQ', 'IHOP', 'KFC', 'MCO',
    'TCBY', 'USA', 'YH',
    'II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX',
])


def _title_word(word: str) -> str:
    """Title-case one word without capitalizing after an apostrophe.

    Digits don't trigger capitalization of the following letter (so "7th"
    stays "7th", not "7Th"). Whitelisted short acronyms are preserved as-is.
    """
    # Preserve known short acronyms when the source sends them all-caps.
    if word.upper() in _UPPER_ACRONYMS:
        return word.upper()
    result = []
    cap_next = True
    for ch in word:
        if ch == "'":
            result.append(ch)
            cap_next = False
        elif ch.isalpha():
            result.append(ch.upper() if cap_next else ch.lower())
            cap_next = False
        elif ch.isdigit():
            # Digits inside a word (e.g. "7th", "B100") are not word
            # boundaries — don't capitalize the letter that follows.
            result.append(ch)
            cap_next = False
        else:
            result.append(ch)
            cap_next = True  # capitalize after hyphens, periods, etc.
    return ''.join(result)


def _smart_title(name: str) -> str:
    """Title-case with small-word lowercasing and no capitalize-after-apostrophe."""
    words = name.split()
    out = []
    for i, word in enumerate(words):
        if i > 0 and word.lower() in _SMALL_WORDS:
            out.append(word.lower())
        else:
            out.append(_title_word(word))
    return ' '.join(out)


class Restaurant(db.Model):
    __tablename__ = 'restaurants'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), nullable=False)
    address = db.Column(db.String(255))
    city = db.Column(db.String(100))
    state = db.Column(db.String(50))
    zip = db.Column(db.String(20))
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    source_id = db.Column(db.String(50), index=True)   # RI facility ID from the API
    cuisine_type = db.Column(db.String(100))
    license_type = db.Column(db.String(200))
    region = db.Column(db.String(100), nullable=False)
    latest_inspection_date = db.Column(db.Date, nullable=True)  # denormalized; updated on every import
    ai_summary = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    __table_args__ = (
        db.UniqueConstraint('region', 'slug', name='uq_restaurant_region_slug'),
        db.Index('ix_restaurants_region', 'region'),
        db.Index('ix_restaurants_city', 'city'),
        db.Index('ix_restaurants_cuisine_type', 'cuisine_type'),
        db.Index('ix_restaurants_region_city', 'region', 'city'),
        db.Index('ix_restaurants_region_state', 'region', 'state'),
        db.Index('ix_restaurants_region_state_city', 'region', 'state', 'city'),
        db.Index('ix_restaurants_region_latest_date', 'region', 'latest_inspection_date'),
        db.Index('ix_restaurants_region_lat_lng', 'region', 'latitude', 'longitude'),
        db.Index('ix_restaurants_name_trgm', 'name',
                 postgresql_using='gin',
                 postgresql_ops={'name': 'gin_trgm_ops'}),
    )

    inspections = db.relationship(
        'Inspection',
        backref='restaurant',
        lazy='select',
        cascade='all, delete-orphan',
        order_by='Inspection.inspection_date.desc()'
    )

    @property
    def latest_inspection(self):
        return self.inspections[0] if self.inspections else None

    @property
    def latest_score(self):
        insp = self.latest_inspection
        if insp:
            return insp.score
        return None

    @property
    def score_tier(self):
        insp = self.latest_inspection
        if insp is None:
            return None
        return insp.score_tier

    @property
    def display_name(self):
        """Name cleaned: D/B/A entity stripped, legal suffixes removed, title-cased.

        Chicago imports join `dba_name — aka_name` so hotel kitchens read as
        "Trump International Hotel — Sixteen". For most Chicago rows the two
        halves are near-duplicates ("Pantanos Restaurant Chicago Llc —
        Pantanos Restaurant Chicago", "Kaiser Tiger — Kaisertiger") and the
        suffix adds noise rather than information. Drop the aka half when a
        normalized comparison shows it carries the same content.

        Philadelphia packs names as 'owner / LLC / DBA / facility-type / code'
        joined by slashes — extract the DBA so pages don't repeat the 80-char
        chain across h1, title, breadcrumb, and FAQ.
        """
        name = self.name
        if self.region == 'philadelphia':
            name = _parse_philly_name(name)
        name = _DBA_RE.sub('', name).strip()
        name = _collapse_redundant_aka(name)
        name = _SUFFIX_RE.sub('', name).strip().rstrip(',').strip()
        return _smart_title(name)

    @property
    def score_display_tier(self):
        """Visual tier based on normalized 0-100 score: low ≥75, medium ≥55, high <55."""
        score = self.latest_score
        if score is None:
            return None
        if score >= 75:
            return 'low'
        elif score >= 55:
            return 'medium'
        return 'high'

    @property
    def city_slug(self):
        city = self.city or ''
        city = city.lower()
        city = city.replace("'", '')
        city = re.sub(r'\s+', '-', city)
        city = re.sub(r'[^a-z0-9-]', '', city)
        return city

    def __repr__(self):
        return f'<Restaurant {self.name} ({self.region})>'
