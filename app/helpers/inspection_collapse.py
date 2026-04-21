"""
Collapse logically-duplicate inspection rows so the detail page and summary
agree on what counts as "one visit."

Two patterns in the source data need collapsing:

1. **Same-date rows** — Chattanooga / Florida / Georgia / Chicago occasionally
   emit multiple inspection records for one facility on one day, typically
   representing parallel sub-kitchen visits. Treating them as four separate
   events inflates visit counts and cadence.

2. **Boston Fail + closeout** — Boston's Open Data feed records each
   violation TWICE: once stamped with the original inspection date and
   `result='Fail'`, then again stamped with the closeout date (7–14 days
   later) and `result='Pass'` or `'Conditional Pass'` when the inspector
   returns to confirm fixes. Same violations, same codes, same severities,
   same risk_score. The closeout is a bureaucratic re-stamp, not a new
   inspection event.

The collapse keeps the Fail as canonical so the page surfaces the initial
finding — a diner cares about "this place failed with 8 violations," not
"they passed the closeout visit." For records without a matching Fail, the
original row is preserved untouched.
"""
from datetime import date


_BOSTON_PAIR_MAX_DAYS = 30


def _violation_count(inspection) -> int:
    return len(inspection.violations or [])


def collapse_inspections(inspections: list) -> list:
    """Collapse same-date duplicates and Boston Fail+closeout pairs.

    Args:
        inspections: inspections for a single facility, sorted by
            `inspection_date DESC` (newest first). Must include the
            `violations` relationship if any caller relies on counts.

    Returns:
        Filtered list in the same order. For unaffected facilities the
        returned list is identical; for affected ones, closeout/dup rows
        are removed and the underlying Fail (or worst-score same-date row)
        remains as canonical.
    """
    if not inspections:
        return inspections

    # Step 1 — same-date collapse. Multiple rows on one date collapse to the
    # row with the worst score (violations the reader would actually see).
    # Ties break toward the row with more violations, then source-list order
    # (deterministic).
    by_date: dict = {}
    order: list = []
    for insp in inspections:
        d = insp.inspection_date
        if d not in by_date:
            by_date[d] = insp
            order.append(d)
            continue
        existing = by_date[d]
        ex_score = existing.score if existing.score is not None else 101
        in_score = insp.score if insp.score is not None else 101
        if in_score < ex_score or (
            in_score == ex_score
            and _violation_count(insp) > _violation_count(existing)
        ):
            by_date[d] = insp
    collapsed = [by_date[d] for d in order]

    # Step 2 — Boston Fail + closeout pair collapse. For each Pass / Conditional
    # Pass row, if the immediately older row is a Fail within 30 days AND has
    # the same risk_score, drop the Pass — it's a closeout stamp of the same
    # violations. Risk-score match is the honest dup signal; matching
    # violation counts alone can happen legitimately.
    result = []
    for i, insp in enumerate(collapsed):
        if i + 1 < len(collapsed):
            older = collapsed[i + 1]
            result_str = (insp.result or '').strip()
            older_result = (older.result or '').strip()
            if (
                older_result == 'Fail'
                and result_str in ('Pass', 'Conditional Pass')
                and 0 < (insp.inspection_date - older.inspection_date).days
                <= _BOSTON_PAIR_MAX_DAYS
                and insp.risk_score is not None
                and older.risk_score is not None
                and insp.risk_score == older.risk_score
            ):
                continue
        result.append(insp)
    return result
