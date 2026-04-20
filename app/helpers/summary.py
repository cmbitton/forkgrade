"""
Data-driven facility summary builder.

Produces 3-4 paragraphs of analytical narrative + an FAQ block, framed around
patterns in a single facility's inspection data. Reads from phrase_bank for
deterministic-but-varied wording. Pulls city comparison stats from the
precomputed RegionStats dict when available.

No LLM, no external calls. Pure derivation from data already in the DB.
"""
import re
from datetime import date
from collections import Counter

from sqlalchemy.orm import selectinload

from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.models.region_stats import RegionStats
from app.utils import get_region_display, get_region_state_abbr
from app.helpers.phrase_bank import pick


# ── Number / text utilities ──────────────────────────────────────────────────

_NUMBER_WORDS = {0: 'zero', 1: 'one', 2: 'two', 3: 'three', 4: 'four',
                 5: 'five', 6: 'six', 7: 'seven', 8: 'eight', 9: 'nine'}


def _n(value: int) -> str:
    """Spell numbers under 10, digits for 10+. Per project style guide."""
    return _NUMBER_WORDS[value] if 0 <= value < 10 else str(value)


def _plural(n: int) -> str:
    return '' if n == 1 else 's'


def _fmt_date(d) -> str:
    if d is None:
        return ''
    try:
        return d.strftime('%b %-d, %Y')
    except Exception:
        return str(d)


def _fmt_year(d) -> str:
    if d is None:
        return ''
    try:
        return d.strftime('%Y')
    except Exception:
        return str(d)


# Acronyms NYC's source data leaves in lowercase ("hot tcs food item",
# "(fpc) not held", "above 140 °f"). Word-bounded uppercase fixes the casing
# before the label hits the page. Extend if other lowercase abbreviations
# show up during spot checks.
_ACRONYMS = {'tcs', 'fpc', 'rop', 'frsa', 'haccp', 'fda', 'doh', 'dohmh',
             'cdc', 'rte', 'phf'}
_ACRONYM_RE = re.compile(r'\b(' + '|'.join(_ACRONYMS) + r')\b', re.IGNORECASE)
_DEGREE_RE = re.compile(r'°([fc])\b', re.IGNORECASE)


def _polish(s: str) -> str:
    """Restore casing on acronyms and degree symbols that source data lowercased."""
    s = _ACRONYM_RE.sub(lambda m: m.group(0).upper(), s)
    s = _DEGREE_RE.sub(lambda m: '°' + m.group(1).upper(), s)
    return s


# Connectors used as natural truncation points when a description has no
# punctuation in the right place. Cutting just before one of these reads as
# a complete (if clipped) thought rather than a sentence cut off mid-flow.
_CONNECTOR_RE = re.compile(
    r'\s(?:in|near|or|with|by|at|to|from|of|for|on|and|that|which|'
    r'when|where|during|after|before|except|including|under|over)\s'
)

# Words that should never be the LAST word of a label — they require a
# complement that the truncation chopped off. Renders prose like
# "X comes up most often" → "Y of comes up most often", which reads broken.
# Sentence-end and comma-clause splits don't filter for these on their own,
# so we apply this gate after every truncation path.
#
# Three groups:
#   - prepositions and conjunctions (the bulk of the failure cases)
#   - determiners (a/an/the can't end a phrase)
#   - adjectives that essentially never stand alone in inspection text
#     ("conditions conducive [to X]" → cutting at "to" leaves a dangling
#     "conducive" — not technically a connector, but reads broken).
_TRAILING_BAD = frozenset({
    'to', 'of', 'from', 'with', 'for', 'in', 'on', 'at', 'by',
    'that', 'which', 'or', 'and',
    'a', 'an', 'the',
    'conducive', 'attributable', 'prone',
})


def _strip_trailing_bad(s: str) -> str | None:
    """Back up one word at a time while the last word is a dangling
    preposition or conjunction. Returns None if backing up consumed too
    much of the phrase to still be useful — caller will drop the paragraph
    rather than ship a stub like 'food not'.

    A few iterations is enough; descriptions don't stack 5 prepositions in
    a row. Cap at 4 to avoid pathological loops on weird input.
    """
    for _ in range(4):
        s = s.rstrip(',;: ').strip()
        words = s.split()
        if not words:
            return None
        if words[-1].lower() not in _TRAILING_BAD:
            break
        s = ' '.join(words[:-1])
    else:
        return None
    s = s.rstrip(',;: ').strip()
    if len(s) < 20:
        return None
    # Final safety net: if we still end on a bad word after 4 passes, drop.
    if s.split()[-1].lower() in _TRAILING_BAD:
        return None
    return s


def _short_label(desc: str | None) -> str | None:
    """Trim a violation description to a tight inline phrase.

    NYC descriptions can run 100+ chars. The order matters here:
      1. Drop everything after the first sentence-end / colon / semicolon.
      2. If still too long, try the first comma-clause when it's 20-70 chars.
         (Looser min than before — was 25 — to catch shorter clean phrases.)
      3. If still too long, find the latest preposition/connector in
         [30, 70] and cut there. Reads as a clipped phrase, not a chopped
         sentence with "..." trailing — that ellipsis was the "bot wrote
         this" signal that tripped on Tacos Azteca.
      4. EVERY candidate runs through _strip_trailing_bad to prune dangling
         prepositions/conjunctions. If nothing survives that gate, return
         None — caller omits P3 entirely rather than ship broken prose.
      5. Polish acronym casing on the way out.
    """
    if not desc:
        return None
    s = re.split(r'[.;:]', desc, 1)[0].strip()
    s = s.rstrip('.,;:').strip()
    if len(s) <= 70:
        cleaned = _strip_trailing_bad(s) if s else None
        return _polish(cleaned) if cleaned else None

    first_clause = s.split(',', 1)[0].strip()
    if 20 <= len(first_clause) <= 70:
        cleaned = _strip_trailing_bad(first_clause)
        if cleaned:
            return _polish(cleaned)

    # Find the latest connector whose prefix lands in the readable range.
    # Iterating finditer with the last match preserves the most meaning.
    best_cut = None
    for m in _CONNECTOR_RE.finditer(s):
        if 30 <= m.start() <= 70:
            best_cut = m.start()
    if best_cut:
        cleaned = _strip_trailing_bad(s[:best_cut])
        if cleaned:
            return _polish(cleaned)

    # Nothing clean to ship. Caller will skip P3.
    return None


def _times_phrase(n: int) -> str:
    """Pre-composed plural-aware count phrase for inline use: 'one time', '12 times'."""
    return f'{_n(n)} time{_plural(n)}'


def _inspections_phrase(n: int) -> str:
    """Pre-composed plural-aware count phrase: 'one inspection', '12 inspections'."""
    return f'{_n(n)} inspection{_plural(n)}'


def _violations_phrase(n: int) -> str:
    """Pre-composed plural-aware count phrase: 'one violation', '12 violations'."""
    return f'{_n(n)} violation{_plural(n)}'


def _lower_first(s: str) -> str:
    return s[:1].lower() + s[1:] if s else s


def _cap_first(s: str) -> str:
    return s[:1].upper() + s[1:] if s else s


# ── Aggregation helpers ──────────────────────────────────────────────────────

def _violation_count(inspection) -> int:
    return len(inspection.violations or [])


def _trend(inspections: list) -> dict | None:
    """Determine inspection trend per the rolling-window rules.

    Returns a dict with the direction and the integer display values to use,
    or None if there isn't enough history.

    Display values: when stable, both numbers should round to the same int
    (otherwise prose says "around six" while FAQ says "around five" for
    the same trend). For stable, average curr+prev and round once.

    Rules:
      6+ inspections: avg of last 3 vs avg of previous 3
      3-5 inspections: latest vs avg of previous 2
      2 inspections: latest vs previous
      <2 inspections: None (caller skips P2)
    """
    n = len(inspections)
    if n < 2:
        return None

    counts = [_violation_count(i) for i in inspections]
    if n >= 6:
        curr = sum(counts[:3]) / 3
        prev = sum(counts[3:6]) / 3
    elif n >= 3:
        curr = counts[0]
        prev = sum(counts[1:3]) / 2
    else:
        curr = counts[0]
        prev = counts[1]

    delta = curr - prev
    # Threshold of 1 full violation absorbs single-inspection noise. Anything
    # smaller is "stable"; larger swings get a directional call.
    if abs(delta) < 1.0:
        direction = 'stable'
        baseline = int(round((curr + prev) / 2))
        return {'direction': direction, 'curr_disp': baseline, 'prev_disp': baseline}
    direction = 'improving' if delta < 0 else 'worsening'
    return {'direction': direction,
            'curr_disp': int(round(curr)),
            'prev_disp': int(round(prev))}


def _top_violation(inspections: list) -> tuple[str, str, int] | None:
    """Find the most common violation across the full history.

    Group by violation_code (consistent within a facility's region). Return
    (code, short_label, occurrences) for the top code, or None if there are
    no violations or none of them have a usable description.
    """
    code_counts: Counter = Counter()
    code_descs: dict[str, str] = {}
    for insp in inspections:
        for v in (insp.violations or []):
            code = v.violation_code or ''
            if not code:
                continue
            code_counts[code] += 1
            # Keep one description per code; first one we see wins (inspections
            # are most-recent-first, so this prefers the latest wording).
            if code not in code_descs and v.description:
                code_descs[code] = v.description
    if not code_counts:
        return None
    code, occurrences = code_counts.most_common(1)[0]
    label = _short_label(code_descs.get(code))
    if not label:
        return None
    return code, label, occurrences


def _city_avg_score(region: str, city: str | None) -> float | None:
    """Look up a city's avg_score from precomputed region_stats.

    Returns None if region_stats is missing, the city isn't tracked
    (precompute requires >=10 restaurants per city), or the slug doesn't
    match. Caller decides how to fall back.
    """
    if not city:
        return None
    rs = db.session.get(RegionStats, region)
    if rs is None or not rs.data:
        return None
    cities = rs.data.get('city_stats') or {}
    city_slug = re.sub(r'[^a-z0-9-]', '',
                       re.sub(r'\s+', '-', city.lower().replace("'", '')))
    entry = cities.get(city_slug)
    if not entry:
        return None
    return entry.get('avg_score')


def _region_avg_score(region: str) -> float | None:
    rs = db.session.get(RegionStats, region)
    if rs is None or not rs.data:
        return None
    return rs.data.get('avg_score')


# ── Tier label helpers ───────────────────────────────────────────────────────

def _tier_for(score: int | None) -> str | None:
    if score is None:
        return None
    if score >= 75:
        return 'low'
    if score >= 55:
        return 'medium'
    return 'high'


_TIER_LABEL = {'low': 'low risk', 'medium': 'medium risk', 'high': 'high risk'}


# ── Paragraph builders ──────────────────────────────────────────────────────

def _build_p1(restaurant, inspections, latest, fid: int) -> str:
    """Overview: total inspections, span, most recent date, tier explainer.

    Appends a stale-record notice when the most recent inspection is more
    than 2 years old. Without it, pages can read "low risk, steady
    performance" while the underlying data is years out of date.
    """
    name = restaurant.display_name
    total = len(inspections)
    earliest = inspections[-1].inspection_date
    latest_date = latest.inspection_date

    # Thin-record framing kicks in for sparse history. Frames the *record* as
    # thin, not the facility as new — a long-running business may simply have
    # a short public history.
    if total < 3:
        intro = pick('thin_record', fid,
                     name=name, count=_n(total), plural=_plural(total))
    else:
        intro = pick('intro_inspection_count', fid,
                     name=name, count=_n(total), date_first=_fmt_year(earliest))

    recent = pick('recent_inspection_opener', fid,
                  name=name, date=_fmt_date(latest_date))

    tier = _tier_for(latest.score)
    if tier:
        base = f'{intro} {recent} {pick(f"risk_tier_{tier}", fid)}'
    else:
        base = f'{intro} {recent}'

    # Stale record gate: 2 years = 730 days. Append a notice so the rest of
    # the page reads as historical commentary rather than a current snapshot.
    if (date.today() - latest_date).days > 730:
        stale = pick('stale_record', fid, name=name,
                     date=_fmt_date(latest_date))
        base = f'{base} {stale}'

    return base


def _build_p2(inspections, fid: int) -> str | None:
    """Trend: rolling-window comparison of recent violation counts.

    Skipped when:
      - fewer than 2 inspections (no comparison possible)
      - zero violations across the entire history (the "stable at zero" line
        is dead weight next to the clean_record paragraph in P3)
    """
    if sum(_violation_count(i) for i in inspections) == 0:
        return None
    trend = _trend(inspections)
    if trend is None:
        return None
    curr_disp = trend['curr_disp']
    prev_disp = trend['prev_disp']
    return pick(f"trend_{trend['direction']}", fid,
                curr=_n(curr_disp),
                prev=_n(prev_disp),
                curr_v=_violations_phrase(curr_disp),
                prev_v=_violations_phrase(prev_disp))


def _build_p3(restaurant, inspections, fid: int) -> str | None:
    """Pattern: most common violation type across full history."""
    total_violations = sum(_violation_count(i) for i in inspections)
    if total_violations == 0:
        return pick('clean_record', fid,
                    name=restaurant.display_name, count=_n(len(inspections)))

    top = _top_violation(inspections)
    if top is None:
        return None
    _code, label, occurrences = top
    return pick('violation_pattern_opener', fid,
                category=_lower_first(label),
                category_cap=_cap_first(label),
                times=_times_phrase(occurrences))


def _build_p4(restaurant, latest, fid: int) -> str | None:
    """Comparative: city or region average score vs this facility."""
    if latest.score is None:
        return None
    city_avg = _city_avg_score(restaurant.region, restaurant.city)
    if city_avg is not None:
        compare_unit = restaurant.city
        avg = city_avg
    else:
        # Per the spec: if the city isn't in the precomputed stats, frame the
        # comparison honestly as state/region-wide rather than pretending it's
        # a city comparison.
        avg = _region_avg_score(restaurant.region)
        if avg is None:
            return None
        compare_unit = get_region_display(restaurant.region)

    score = latest.score
    delta = score - avg
    if abs(delta) < 3.0:
        slot = 'comparison_average'
    elif delta > 0:
        slot = 'comparison_better_than_city'
    else:
        slot = 'comparison_worse_than_city'

    return pick(slot, fid,
                name=restaurant.display_name,
                score=str(score),
                city=compare_unit,
                city_avg=f'{avg:.0f}')


def _build_conclusion(restaurant, latest, inspections, fid: int) -> str:
    """One closing sentence appended to whichever paragraph fits best.

    Read off the latest tier and the sum of recent violations to pick a
    positive/neutral/caution closer.
    """
    tier = _tier_for(latest.score)
    if tier == 'low':
        slot = 'conclusion_positive'
    elif tier == 'high':
        slot = 'conclusion_caution'
    else:
        slot = 'conclusion_neutral'
    return pick(slot, fid)


# ── FAQ ──────────────────────────────────────────────────────────────────────

def _avg_inspections_per_year(inspections) -> float | None:
    if len(inspections) < 2:
        return None
    earliest = inspections[-1].inspection_date
    latest = inspections[0].inspection_date
    span_days = (latest - earliest).days
    if span_days < 30:
        return None
    years = span_days / 365.25
    return len(inspections) / years if years > 0 else None


def _build_faq(restaurant, inspections, latest, fid: int) -> list[dict]:
    name = restaurant.display_name
    city = restaurant.city or get_region_display(restaurant.region)
    faq: list[dict] = []

    # When was X last inspected
    faq.append({
        'question': f'When was {name} last inspected?',
        'answer': (f'The most recent health inspection at {name} on file is '
                   f'from {_fmt_date(latest.inspection_date)}. '
                   f'The public record contains '
                   f'{_inspections_phrase(len(inspections))} in total.'),
    })

    # Most common violation
    top = _top_violation(inspections)
    if top is not None:
        _code, label, occurrences = top
        faq.append({
            'question': f'What is the most common violation at {name}?',
            'answer': (f'Across the inspection record, {_lower_first(label)} '
                       f'has been cited {_times_phrase(occurrences)}, more '
                       f'than any other issue at {name}.'),
        })
    else:
        total_v = sum(_violation_count(i) for i in inspections)
        if total_v == 0:
            faq.append({
                'question': f'What is the most common violation at {name}?',
                'answer': (f'No violations have been recorded at {name} '
                           f'across the {_inspections_phrase(len(inspections))} '
                           f'on file.'),
            })

    # City comparison
    if latest.score is not None:
        city_avg = _city_avg_score(restaurant.region, restaurant.city)
        if city_avg is not None and restaurant.city:
            delta = latest.score - city_avg
            if abs(delta) < 3.0:
                framing = (f'about the same as the {restaurant.city} '
                           f'average of {city_avg:.0f}')
            elif delta > 0:
                framing = (f'higher than the {restaurant.city} average '
                           f'of {city_avg:.0f}')
            else:
                framing = (f'lower than the {restaurant.city} average '
                           f'of {city_avg:.0f}')
            faq.append({
                'question': (f'How does {name} compare to other restaurants '
                             f'in {city}?'),
                'answer': (f'{name} most recently scored {latest.score} out '
                           f'of 100, which is {framing}.'),
            })

    # Trend question (skip when there are no violations to discuss)
    if sum(_violation_count(i) for i in inspections) > 0:
        trend = _trend(inspections)
        if trend is not None:
            curr_disp = trend['curr_disp']
            prev_disp = trend['prev_disp']
            if trend['direction'] == 'improving':
                ans = (f'Yes. Recent inspections at {name} have turned up '
                       f'fewer violations than earlier ones, with the latest '
                       f'finding around {_n(curr_disp)} '
                       f'violation{_plural(curr_disp)} compared to about '
                       f'{_n(prev_disp)} previously.')
            elif trend['direction'] == 'worsening':
                ans = (f'No. Recent inspections at {name} have flagged more '
                       f'violations than earlier ones, ticking from about '
                       f'{_n(prev_disp)} per visit to around '
                       f'{_n(curr_disp)} more recently.')
            else:
                ans = (f'Results have been roughly steady. Recent '
                       f'inspections at {name} have produced about the same '
                       f'number of violations as earlier ones, holding '
                       f'around {_n(curr_disp)} per visit.')
            faq.append({
                'question': (f"Has {name}'s inspection record improved over time?"),
                'answer': ans,
            })

    # Tier meaning
    tier = _tier_for(latest.score)
    if tier:
        label = _TIER_LABEL[tier]
        if tier == 'low':
            meaning = ('inspectors found minimal or no significant issues at '
                       'the most recent visit. Most facilities at this tier '
                       'have a clean recent inspection report.')
        elif tier == 'medium':
            meaning = ('the most recent inspection turned up a handful of '
                       'issues that the health department wrote up but did '
                       'not classify as critical.')
        else:
            meaning = ('the most recent inspection flagged either critical '
                       'violations or a substantial number of smaller ones. '
                       'Diners may want to read the violation details before '
                       'deciding to visit.')
        faq.append({
            'question': f'What does a {label} rating mean?',
            'answer': f'A {label} rating at {name} means {meaning}',
        })

    # Inspection frequency.
    # Round per-year to an integer for display: the float precision is noise
    # from variable date spans, and dividing 12/freq to get "every N months"
    # breaks at the edges (12/year → "every 1 months", 24/year → "every 0
    # months"). Bucket the cadence directly off the integer count instead.
    freq = _avg_inspections_per_year(inspections)
    if freq is not None:
        per_year = max(1, round(freq))
        if per_year >= 12:
            cadence = 'about once a month or more'
        elif per_year >= 2:
            cadence = f'around {_n(per_year)} times per year on average'
        elif freq >= 0.8:
            cadence = 'roughly once per year on average'
        else:
            years_between = max(2, round(1 / freq))
            cadence = f'about once every {_n(years_between)} years on average'
        faq.append({
            'question': f'How often is {name} inspected?',
            'answer': (f'Based on the inspection history on file, {name} is '
                       f'inspected {cadence}.'),
        })

    return faq


# ── Public API ───────────────────────────────────────────────────────────────

def build_summary(facility_id: int) -> dict | None:
    """Return {'paragraphs': [...], 'faq': [...]} or None if no inspections."""
    restaurant = db.session.get(Restaurant, facility_id)
    if restaurant is None:
        return None

    inspections = (
        Inspection.query
        .options(selectinload(Inspection.violations))
        .filter_by(restaurant_id=facility_id)
        .filter(Inspection.not_future())
        .order_by(Inspection.inspection_date.desc())
        .all()
    )
    if not inspections:
        return None

    latest = inspections[0]
    fid = facility_id

    paragraphs: list[str] = []

    # P1: always
    p1 = _build_p1(restaurant, inspections, latest, fid)

    # P2: trend (None if <2 inspections)
    p2 = _build_p2(inspections, fid)

    # P3: pattern or clean record
    p3 = _build_p3(restaurant, inspections, fid)

    # P4: comparison (city or region fallback)
    p4 = _build_p4(restaurant, latest, fid)

    # Conclusion: append to last available paragraph so the closer always lands
    conclusion = _build_conclusion(restaurant, latest, inspections, fid)

    paragraphs.append(p1)
    if p2:
        paragraphs.append(p2)
    if p3:
        paragraphs.append(p3)
    if p4:
        paragraphs.append(p4 + ' ' + conclusion)
    else:
        # Tack the closer onto whichever paragraph ended up last.
        paragraphs[-1] = paragraphs[-1] + ' ' + conclusion

    faq = _build_faq(restaurant, inspections, latest, fid)

    return {'paragraphs': paragraphs, 'faq': faq}
