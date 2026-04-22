"""Street-address normalization.

Expands Texas-source abbreviations to user-friendly form so pages read
cleanly and match what people actually search for:

  "Fredsbg Rd"        → "Fredericksburg Rd"
  "Ih 10 W"           → "IH-10 W"
  "New Brnfls N"      → "New Braunfels N"
  "Nw Military Hy"    → "NW Military Hwy"
  "Gen Mcmullen S"    → "Gen McMullen S"

Only transforms we can make unambiguously from context are applied —
when a token could legitimately be either a street name or a random
business-name fragment, it's left alone. All substitutions are
word-boundary-anchored so mid-word matches (e.g. "Ih" inside "Ihaven")
never trigger.
"""

import re


# Full-word expansions for Texas street-name abbreviations the public data
# feeds ship in truncated form. Each entry is unambiguous in the context of
# an address string — "Fredsbg" only ever means Fredericksburg in the SA
# data, not a business name.
_STREET_EXPANSIONS = {
    'fredsbg':  'Fredericksburg',
    'brnfls':   'Braunfels',
    'jdtn':     'Jourdanton',
    'maltsbrg': 'Maltsberger',
    # Road-type abbreviations the source over-abbreviates even for common
    # concepts. "Fy" and "Hy" aren't standard US postal abbreviations —
    # normalize to the common "Fwy" / "Hwy" form.
    'fy':  'Fwy',
    'hy':  'Hwy',
}

# Acronyms the source ships lower-cased ("Ih 10", "Us 281", "Nw Military").
# Restored to their canonical uppercase. Anchored with word boundaries so
# they never touch the middle of a longer word.
_ACRONYMS = {
    'ih', 'us', 'fm',
    'nw', 'ne', 'sw', 'se',
}

_WORD_RE = re.compile(r"[A-Za-z]+")

# Join "IH 35" / "IH  35" → "IH-35"  (interstate naming convention).
# Does NOT touch US-##, FM-##, or State Hwy ## — those conventionally use
# a space, not a hyphen.
_IH_NUMBER_RE = re.compile(r'\bIH\s+(\d+)\b')

# Fix "Mc" casing — source data ships "Mcmullen" and .title() leaves it
# as "Mcmullen" (capitalizes first letter only). The proper form uppercases
# the first letter of the root ("McMullen", "McCullough").
_MC_CASE_RE = re.compile(r'\bMc([a-z])')


def normalize_street(s: str | None) -> str | None:
    """Apply all normalizations in sequence. None in → None out.

    Idempotent: running it on already-clean input is a no-op.
    """
    if not s:
        return s

    def _sub_word(m: re.Match) -> str:
        word = m.group(0)
        key = word.lower()
        if key in _STREET_EXPANSIONS:
            return _STREET_EXPANSIONS[key]
        if key in _ACRONYMS:
            return word.upper()
        return word

    out = _WORD_RE.sub(_sub_word, s)
    out = _MC_CASE_RE.sub(lambda m: 'Mc' + m.group(1).upper(), out)
    out = _IH_NUMBER_RE.sub(r'IH-\1', out)
    # Collapse any double spaces introduced by transforms (none of ours
    # should, but belt-and-suspenders for future edits).
    out = re.sub(r'\s{2,}', ' ', out).strip()
    return out
