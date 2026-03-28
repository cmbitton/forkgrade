import re
from app.db import db


_SUFFIX_RE = re.compile(
    r',?\s+(?:LLC\.?|INC\.?|CORP\.?|L\.L\.C\.)$',
    re.IGNORECASE
)

# Strip legal entity name before D/B/A — keep only the trade name after it
_DBA_RE = re.compile(r'^.+\bD[/.]?B[/.]?A\b\.?\s+', re.IGNORECASE)

_SMALL_WORDS = frozenset([
    'a', 'an', 'the', 'and', 'but', 'or', 'nor', 'for',
    'of', 'on', 'in', 'at', 'to', 'by', 'up', 'as',
])


def _title_word(word: str) -> str:
    """Title-case one word without capitalizing after an apostrophe."""
    result = []
    cap_next = True
    for ch in word:
        if ch == "'":
            result.append(ch)
            cap_next = False
        elif ch.isalpha():
            result.append(ch.upper() if cap_next else ch.lower())
            cap_next = False
        else:
            result.append(ch)
            cap_next = True  # capitalize after hyphens in compound words
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
    ai_summary = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    __table_args__ = (
        db.UniqueConstraint('region', 'slug', name='uq_restaurant_region_slug'),
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
        """Name cleaned: D/B/A entity stripped, legal suffixes removed, title-cased."""
        name = _DBA_RE.sub('', self.name).strip()
        name = _SUFFIX_RE.sub('', name).strip().rstrip(',').strip()
        return _smart_title(name)

    @property
    def score_display_tier(self):
        """Visual tier based on normalized 0-100 score: low ≥70, medium ≥50, high <50."""
        score = self.latest_score
        if score is None:
            return None
        if score >= 70:
            return 'low'
        elif score >= 50:
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
