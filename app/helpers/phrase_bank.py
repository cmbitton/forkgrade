"""
Phrase bank for the data-driven facility summary.

Each slot is a list of complete-sentence templates. Templates take named
format() params; summary.py is responsible for passing pre-formatted strings
(e.g. counts already converted to words/digits per the under-ten rule).

Selection is deterministic: hashed by slot+facility_id so the same facility
always reads the same way, but adjacent facilities pick different variants.
The slot name is included in the salt so picks across slots don't correlate
(otherwise every "facility 7" would hit index 7 of every list).

No em dashes anywhere. Tone: conversational, written for diners.
"""
import hashlib

PHRASE_BANK: dict[str, list[str]] = {

    # ── P1 openers: total inspections + earliest date ────────────────────────
    "intro_inspection_count": [
        "{name} has been inspected {count} times since {date_first}.",
        "Inspectors have visited {name} {count} times, with records going back to {date_first}.",
        "The health department has logged {count} inspections at {name}, the earliest from {date_first}.",
        "Public records show {count} inspections at {name} stretching back to {date_first}.",
        "{name} appears in inspection records {count} times, starting in {date_first}.",
        "Across the available record, {name} has {count} inspections on file, the first dated {date_first}.",
        "Going back to {date_first}, {name} has {count} inspections in the public record.",
    ],

    # ── P1 fragment: when was the most recent visit ──────────────────────────
    "recent_inspection_opener": [
        "The most recent visit was on {date}.",
        "Inspectors last stopped by on {date}.",
        "{name} was last inspected on {date}.",
        "The latest inspection on file is from {date}.",
        "On {date}, the health department conducted the most recent visit.",
        "The newest entry in the record is dated {date}.",
        "The most recent report on file is from {date}.",
    ],

    # ── P1 explainer fragments: what the risk tier means for a diner ─────────
    "risk_tier_low": [
        "A low risk rating suggests inspectors haven't found much to be concerned about lately.",
        "Low risk means the most recent visit produced few or no significant findings.",
        "A low risk tier reflects an inspection that turned up minimal issues.",
        "Diners can read the low risk label as a sign that recent inspections have gone well.",
        "Low risk indicates the latest report didn't flag anything that would worry the average customer.",
        "When a facility lands in the low risk tier, it usually means nothing alarming showed up at the most recent visit.",
    ],
    "risk_tier_medium": [
        "A medium risk rating points to a few notable findings at the last inspection, though nothing severe.",
        "Medium risk typically reflects a handful of issues that inspectors wrote up but didn't deem critical.",
        "The medium risk tier sits in the middle: not spotless, but not alarming either.",
        "Diners should read medium risk as a signal that some issues exist but aren't extreme.",
        "A medium risk score generally means inspectors found things to fix, though most weren't urgent.",
        "When a facility lands in medium risk territory, it usually means a mixed inspection result.",
    ],
    "risk_tier_high": [
        "A high risk rating points to multiple serious findings at the most recent inspection.",
        "High risk usually means inspectors flagged either critical violations or a stack of smaller ones.",
        "The high risk label is a heads-up that the most recent visit didn't go well.",
        "Diners may want to take note: high risk reflects issues that a health inspector considered important.",
        "High risk indicates the latest inspection turned up problems worth knowing about.",
        "A high risk tier suggests the facility had a rough recent inspection, with multiple violations on record.",
    ],

    # ── P2 trend sentences: comparing recent inspections ─────────────────────
    # Tokens precomposed by summary.py:
    #   {curr_v}/{prev_v} = "five violations" / "one violation" / "zero violations"
    #   {curr}/{prev}     = bare numerals: "five" / "one" / "zero"
    # Variants pick whichever fits cleanly so phrasing reads naturally at small
    # counts ("down from around one violation" not "down from around one violations").
    #
    # IMPORTANT: these numbers are rolling averages over 2–3 visits (see
    # summary._trend). Templates MUST NOT frame them as a single visit's
    # count — readers scan the inspection list and notice the mismatch.
    # "Around X" / "roughly Y per visit" / "averaging" phrasing is honest
    # in both the averaged case and the degenerate 2-visit case.
    "trend_improving": [
        "Violation counts have been trending down, averaging around {curr_v} across recent inspections versus roughly {prev_v} before.",
        "Things have been moving in the right direction, with the rolling count dropping from around {prev_v} to closer to {curr_v} per visit.",
        "Recent inspections have found fewer violations than earlier ones, averaging around {curr_v} lately and about {prev_v} before that.",
        "The picture has improved over the last few visits: recent inspections have averaged around {curr_v}, down from roughly {prev_v} earlier in the record.",
        "The trend has been favorable: violation counts have eased from around {prev_v} to closer to {curr_v} per visit over the last few inspections.",
        "Things are looking better lately, with recent visits averaging around {curr_v} compared to roughly {prev_v} earlier on.",
    ],
    "trend_worsening": [
        "Violation counts have ticked up lately, averaging around {curr_v} per visit versus roughly {prev_v} earlier in the record.",
        "The trend has not been favorable: recent inspections average around {curr_v} each, up from closer to {prev_v} before.",
        "Things have been moving the wrong way, with the rolling count rising from around {prev_v} to closer to {curr_v} per visit.",
        "Recent inspections have turned up more issues than earlier ones, averaging around {curr_v} lately compared to roughly {prev_v} before.",
        "The picture has gotten worse over the last few visits, with the average climbing from around {prev_v} to closer to {curr_v}.",
        "Recent visits have flagged more than earlier ones: around {curr_v} per visit lately, up from roughly {prev_v} before.",
    ],
    "trend_stable": [
        "Violation counts have held steady across recent visits, averaging around {prev_v} each.",
        "Recent inspections have turned up roughly the same number of issues each time, hovering near {prev_v} per visit.",
        "There hasn't been much movement either way: counts have stayed near {prev_v} per visit across recent inspections.",
        "Inspection results have stayed in a similar range over the last few visits, averaging around {prev_v} each.",
        "Recent visits have produced comparable findings, with counts hovering near {prev_v} per visit.",
        "Performance has remained roughly level over recent inspections, averaging near {prev_v} each time.",
    ],

    # ── P3 openers: most common violation pattern ────────────────────────────
    # {times} is precomposed by summary.py: "five times" / "one time" — keeps
    # singular/plural agreement out of the template strings.
    "violation_pattern_opener": [
        "The most common issue across all inspections has been {category}, showing up {times}.",
        "{category_cap} comes up most often, recorded {times} in the inspection record.",
        "Looking across the full record, {category} is the recurring theme, flagged {times}.",
        "The pattern that stands out is {category}, which has been cited {times}.",
        "{category_cap} accounts for the largest share of issues, appearing {times} across the record.",
        "When inspectors have written things up, {category} has been the most frequent reason, cited {times}.",
        "Across the inspection history, {category} is the issue that surfaces most often, recorded {times}.",
    ],

    # ── P4 city comparison sentences ─────────────────────────────────────────
    # {name_poss} is the English possessive of the name; summary.py
    # pre-computes it so trailing-s names read as "Erik's Fit Meals'"
    # rather than "Erik's Fit Meals's".
    "comparison_better_than_city": [
        "{name_poss} latest score of {score} sits above the {city} average of {city_avg}.",
        "That puts the facility ahead of the local pack: the average {city} restaurant scores {city_avg}.",
        "Restaurants in {city} average {city_avg}, so {name} is doing better than most peers.",
        "Compared to the broader {city} restaurant scene, where the average is {city_avg}, this is a stronger showing.",
        "The city-wide average for {city} sits at {city_avg}, putting {name} on the better side of that line.",
        "Among {city} restaurants, the typical score is {city_avg}; {name} is comfortably above that bar.",
    ],
    "comparison_worse_than_city": [
        "{name_poss} latest score of {score} falls below the {city} average of {city_avg}.",
        "That's lower than the typical {city} restaurant, which scores around {city_avg}.",
        "By comparison, the average {city} facility scores {city_avg}, putting {name} on the weaker side.",
        "Restaurants in {city} average {city_avg}, so {name} trails the local norm.",
        "The city-wide average sits at {city_avg}, which {name_poss} {score} doesn't quite reach.",
        "Compared to other {city} restaurants (averaging {city_avg}), there's room to close the gap.",
    ],
    "comparison_average": [
        "{name_poss} latest score is in line with the {city} average of {city_avg}.",
        "That falls roughly in the middle of the pack for {city} restaurants.",
        "Compared to the broader {city} restaurant scene, this is about average.",
        "{name} scores about where you'd expect for a {city} restaurant.",
        "The city-wide average is {city_avg}, putting {name} squarely in typical territory.",
        "Among {city} restaurants, this is a fairly standard result.",
    ],

    # ── Closing sentences (final line of P3 or P4) ───────────────────────────
    "conclusion_positive": [
        "Overall, the inspection record reads well.",
        "The file should reassure diners considering a visit.",
        "The full picture is one of consistent compliance.",
        "The record reflects steady performance over time.",
        "Taken together, the history is a positive one.",
        "There isn't much in the file that would give a customer pause.",
    ],
    "conclusion_neutral": [
        "On the whole, the file is mixed but not concerning.",
        "The full record sits in fairly typical territory for a working restaurant.",
        "Taken together, the history looks like that of a busy facility working through the usual inspection cycle.",
        "Nothing in the record is alarming, but there's room to improve.",
        "The record is unremarkable in either direction.",
        "The inspection history reads as standard for a restaurant of this size.",
    ],
    "conclusion_caution": [
        "Diners may want to weigh the inspection history when deciding to visit.",
        "The pattern in the record is worth a careful look.",
        "This restaurant has more on its record than most do.",
        "Taken together, the history suggests a facility that has struggled with consistency.",
        "There are enough flags in the record to merit a second thought.",
        "Diners may want to scan the inspection details before deciding to visit.",
    ],

    # ── Edge case: facility has fewer than 3 inspections ─────────────────────
    # Frames the record as thin, not the facility as new — a long-running
    # business may simply have a short public history (data freshness, region
    # added recently, infrequent inspection cadence).
    "thin_record": [
        "{name} has a thin inspection record, with only {count} visit{plural} on file so far.",
        "There aren't many inspections to draw from yet: just {count} so far.",
        "The inspection history at {name} is still short, with only {count} visit{plural} logged.",
        "With just {count} inspection{plural} in the record, there isn't enough history to establish a clear pattern.",
        "Public records currently show only {count} inspection{plural} for {name}.",
        "{name} has only {count} inspection{plural} on file, a thin record to work from.",
    ],

    # ── Edge case: most recent inspection is more than 2 years old ───────────
    # Appended to P1 to put readers on alert that the rest of the page reflects
    # stale data, not current conditions. At scale, plenty of facilities have
    # gaps in their public record (closed, transferred ownership, agency lag).
    "stale_record": [
        "Public records show no inspections at {name} since {date}, so this file may not reflect current conditions.",
        "The most recent inspection at {name} is from {date}, and nothing newer appears in the public record.",
        "No fresh inspection data is available: the latest entry for {name} dates to {date}.",
        "The file hasn't been updated since {date}, so take the current picture with that in mind.",
        "{name}'s record stops at {date}, more than two years back, so current conditions may differ.",
        "Note that {name}'s inspection history hasn't been updated since {date}; current conditions may have shifted from what the file shows.",
    ],

    # ── Edge case: zero violations across full history ───────────────────────
    "clean_record": [
        "No violations have appeared in any of the inspections on file.",
        "Across {count_v}, no violations have been recorded.",
        "{name} has a clean inspection sheet: zero violations across the full record.",
        "Inspectors haven't written up any violations across the available history.",
        "The record is empty on the violation front, which is unusual.",
        "Every inspection in the file has come back without a recorded violation.",
    ],
}


def pick(slot: str, facility_id: int, **fields) -> str:
    """Deterministically choose and format a phrase variant.

    Salting with the slot name keeps picks across slots uncorrelated: without
    it, a facility that hashes to index 0 would always get the first variant
    of every slot, producing a recognizable pattern across pages.
    """
    variants = PHRASE_BANK[slot]
    salt = f"{slot}:{facility_id}".encode()
    idx = int(hashlib.md5(salt).hexdigest(), 16) % len(variants)
    return variants[idx].format(**fields)
