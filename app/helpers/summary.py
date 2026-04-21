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
from app.helpers.inspection_collapse import collapse_inspections


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

# Source-data prefixes and code markers that leak into violation descriptions
# across regions. Stripping these turns "17C - physical facilities installed,
# maintained, and clean" into "physical facilities installed, maintained, and
# clean" and "Basic - Food stored on floor" into "food stored on floor".
#
# Prefixes (left-anchored):
#   "17C - ", "15C - "                      Georgia FDA-code prefix
#   "Basic - ", "Intermediate - ",
#   "High Priority - "                      Florida priority-class prefix
#   "(a)word", "(b)word", "(c)word"         Texas Houston section-code prefix
#
# Suffix markers (right-anchored):
#   " (c)", " (p)", " (pf)", " (a)",
#   " (b)", " (pa)"                         Boston / FDA code-class tag
_PREFIX_RE = re.compile(
    r'^\s*(?:'
    r'\d{1,3}[A-Za-z]?\s*-\s*'            # "17C - ", "5 - "
    r'|(?:basic|intermediate|high\s+priority)\s*-+\s*'
    r'|\([a-z]\)(?=[a-z])'                # "(a)word" with no space
    r')',
    re.IGNORECASE,
)
_SUFFIX_CODE_RE = re.compile(
    r'\s*\(\s*(?:p|pf|pa|a|b|c|p[fs]?)\s*\)\s*$',
    re.IGNORECASE,
)


def _polish(s: str) -> str:
    """Normalize label casing and strip source-data code markers.

    Order matters: strip prefixes/suffixes before case normalization so the
    resulting first letter (not the code prefix) is what gets sentence-cased.
    Then re-uppercase known acronyms (`TCS`, `PHF`, etc.) that live inside
    the label.
    """
    s = _PREFIX_RE.sub('', s).strip()
    s = _SUFFIX_CODE_RE.sub('', s).strip()
    # Collapse any run of repeated dashes or whitespace introduced by the
    # strips (e.g. "Intermediate - - From initial inspection" → "From
    # initial inspection") and trim stray punctuation at the edges.
    s = re.sub(r'\s*-\s*-\s*', ' - ', s)
    s = re.sub(r'\s+', ' ', s).strip(' -,;:')
    # Normalize to sentence case: lowercase everything except the first
    # character. Fixes mid-phrase title case from Maricopa/Texas sources
    # ("preventing Contamination from Hands" → "preventing contamination
    # from hands"). Acronyms get re-uppercased below.
    if s:
        s = s[0] + s[1:].lower()
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

# Words that should never be the FIRST word of a label — a real violation
# description doesn't start with a preposition or conjunction. These only
# appear as leading words when an earlier prefix-strip removed the real
# subject (e.g. "Intermediate - - From initial inspection" → "From initial
# inspection" after stripping). Rejecting these keeps garbage descriptions
# out of the "most common violation" slot.
_LEADING_BAD = frozenset({
    'from', 'to', 'of', 'with', 'for', 'in', 'on', 'at', 'by',
    'that', 'which', 'or', 'and', 'but',
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

    def _finalize(text: str | None) -> str | None:
        if not text:
            return None
        polished = _polish(text)
        if not polished:
            return None
        # Reject labels that start with a preposition or conjunction — the
        # real subject was stripped away by prefix cleanup and what's left
        # isn't a meaningful category name ("from initial inspection",
        # "for proper storage"). Caller skips P3 rather than ship garbage.
        first = polished.split(None, 1)[0].lower()
        if first in _LEADING_BAD:
            return None
        return polished

    if len(s) <= 70:
        cleaned = _strip_trailing_bad(s) if s else None
        return _finalize(cleaned)

    first_clause = s.split(',', 1)[0].strip()
    if 20 <= len(first_clause) <= 70:
        cleaned = _strip_trailing_bad(first_clause)
        result = _finalize(cleaned)
        if result:
            return result

    # Find the latest connector whose prefix lands in the readable range.
    # Iterating finditer with the last match preserves the most meaning.
    best_cut = None
    for m in _CONNECTOR_RE.finditer(s):
        if 30 <= m.start() <= 70:
            best_cut = m.start()
    if best_cut:
        cleaned = _strip_trailing_bad(s[:best_cut])
        result = _finalize(cleaned)
        if result:
            return result

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


def _possessive(name: str) -> str:
    """English possessive for a facility name.

    Rules, in priority order:
      - Name already ends in "'s" (e.g. "Johnny's", "Wendy's") — return as-is.
        These are named after a possessor, and "Johnny's food" / "Johnny's
        inspection record" is how English speakers frame them.
      - Name ends in plural "s" (e.g. "Erik's Fit Meals") — add apostrophe.
      - Otherwise — add "'s".

    Matters for FAQ questions and P4 templates that frame the facility as
    a possessor; the raw "{name}'s" produces "Johnny's's" / "Meals's".
    """
    if not name:
        return name
    if name.endswith(("'s", "'S", "\u2019s", "\u2019S")):
        return name
    if name[-1] in ('s', 'S'):
        return name + "'"
    return name + "'s"


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
    # Threshold of 2 full violations: a 1-violation swing is within normal
    # inspection variance — one inspector flags something a second doesn't,
    # or a borderline item gets written up. At higher counts especially
    # (e.g. 8→7) calling that "improving" overstates what changed. Caller
    # uses the "stable" phrasing below the threshold, directional above.
    if abs(delta) < 2.0:
        direction = 'stable'
        baseline = int(round((curr + prev) / 2))
        return {'direction': direction, 'curr_disp': baseline, 'prev_disp': baseline}
    direction = 'improving' if delta < 0 else 'worsening'
    return {'direction': direction,
            'curr_disp': int(round(curr)),
            'prev_disp': int(round(prev))}


def _top_violation(inspections: list) -> tuple[str, str, int] | None:
    """Find the most common violation across the full history.

    Group by violation_code (consistent within a facility's region). Walk
    down the ranked list of codes (by frequency) and return the first one
    whose description survives `_short_label` cleanup. Returns None if:
      - there are no violations at all
      - every code's description is too garbled to produce a label
      - no code has at least 2 citations (a one-off isn't a pattern and
        reads broken — "has been cited one time, more than any other
        issue")

    Why the walk-down: some source feeds (Florida especially) leave
    placeholder descriptions like "Intermediate - - From initial inspection"
    on their top code. Rather than skip P3 entirely, fall through to the
    next-most-common code that has a real description.
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
    for code, occurrences in code_counts.most_common():
        if occurrences < 2:
            return None  # ranked list; nothing below will clear the threshold
        label = _short_label(code_descs.get(code))
        if label:
            return code, label, occurrences
    return None


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
    # a short public history. Threshold lives in _THIN_RECORD_THRESHOLD so
    # the cadence FAQ skip uses the same definition.
    if total < _THIN_RECORD_THRESHOLD:
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
    # Wrap the category in curly quotes so it reads as a callout in prose
    # and survives intact through the FAQ JSON-LD (where HTML styling
    # would render as text). Mid-sentence form stays lower-cased; the
    # capitalized variant keeps its leading capital so templates that open
    # with {category_cap} still scan as sentence starts.
    return pick('violation_pattern_opener', fid,
                category=f'\u201c{_lower_first(label)}\u201d',
                category_cap=f'\u201c{_cap_first(label)}\u201d',
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
                name_poss=_possessive(restaurant.display_name),
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

# Inspection counts at or above this threshold get full statistical framing
# (intro_inspection_count opener, cadence FAQ, etc.). Below it, the record
# is "thin" — the prose acknowledges sparse history and the cadence FAQ is
# omitted. Defined once so build_p1 and build_faq can never disagree about
# whether a record is thick enough to extrapolate from.
_THIN_RECORD_THRESHOLD = 3

# Minimum window before the per-year cadence calc is meaningful, even when
# inspection count clears _THIN_RECORD_THRESHOLD. 3 visits over 4 months
# extrapolates to "9 per year" but the next visit could land 18 months later
# just as easily. At 225k pages, "missing FAQ entry" is far better than
# "confidently wrong number".
_CADENCE_MIN_SPAN_DAYS = 547  # ~18 months


def _avg_inspections_per_year(inspections) -> float | None:
    if len(inspections) < _THIN_RECORD_THRESHOLD:
        return None
    earliest = inspections[-1].inspection_date
    latest = inspections[0].inspection_date
    span_days = (latest - earliest).days
    if span_days < _CADENCE_MIN_SPAN_DAYS:
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
            'answer': (f'Across the inspection record, '
                       f'\u201c{_lower_first(label)}\u201d '
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
            # Framing matches the P2 prose: the numbers are rolling averages
            # over 2–3 visits, so the answer talks about the average rather
            # than claiming "the latest visit found X".
            if trend['direction'] == 'improving':
                ans = (f'Yes. Recent inspections at {name} have averaged '
                       f'around {_n(curr_disp)} '
                       f'violation{_plural(curr_disp)} per visit, down from '
                       f'roughly {_n(prev_disp)} earlier in the record.')
            elif trend['direction'] == 'worsening':
                ans = (f'No. Recent inspections at {name} have averaged '
                       f'around {_n(curr_disp)} '
                       f'violation{_plural(curr_disp)} per visit, up from '
                       f'roughly {_n(prev_disp)} earlier in the record.')
            else:
                ans = (f'Results have been roughly steady. Inspections at '
                       f'{name} have averaged around {_n(curr_disp)} '
                       f'violation{_plural(curr_disp)} per visit across '
                       f'the recent record.')
            faq.append({
                'question': (f'Has {_possessive(name)} inspection record '
                             f'improved over time?'),
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
    #
    # Skip this entire FAQ for thin records — by definition, a record too
    # sparse for prose framing ("only N inspections so far") is too sparse
    # for rate calculations. _avg_inspections_per_year already enforces the
    # count gate, but the explicit check here keeps the invariant readable
    # at the call site so a future change to the calc can't accidentally
    # produce a cadence FAQ on a thin-record page.
    is_thin = len(inspections) < _THIN_RECORD_THRESHOLD
    # Skip the present-tense cadence answer when the latest inspection is
    # more than 2 years old. The stale_record notice in P1 has already told
    # the reader the file is out of date; "is inspected around 3 times per
    # year" would directly contradict that. 2y = 730d matches the P1 gate.
    is_stale = (date.today() - latest.inspection_date).days > 730
    freq = None if (is_thin or is_stale) else _avg_inspections_per_year(inspections)
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

    inspections_raw = (
        Inspection.query
        .options(selectinload(Inspection.violations))
        .filter_by(restaurant_id=facility_id)
        .filter(Inspection.not_future())
        .order_by(Inspection.inspection_date.desc())
        .all()
    )
    if not inspections_raw:
        return None

    # Collapse same-date rows and Boston Fail+closeout pairs. The detail
    # page uses the same helper (see restaurant.render_restaurant) so the
    # visible inspection list and the summary math describe the same events.
    inspections = collapse_inspections(inspections_raw)
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
