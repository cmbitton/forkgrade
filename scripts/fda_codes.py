"""
FDA Food Code 2022 — violation severity by code section and inspection item number.

Shared across all import scripts that use FDA Food Code violation numbering
(Rhode Island, Houston, Maricopa, Philadelphia, etc.).

Values:
  P  = Priority       → critical (weight 3)
  Pf = Priority Foundation → major (weight 2)
  C  = Core            → minor   (weight 1)

Subsection-specific entries (e.g. "3-304.15(B)") take precedence over the
base code ("3-304.15") when an exact match exists.

PRIORITY_ITEMS / PRIORITY_FOUNDATION_ITEMS
  Standard FDA inspection form item numbers (used on forms that don't embed
  P/Pf/C labels directly, e.g. Rhode Island, Philadelphia).
  Items not in either set → Core (minor).
"""

# FDA Food Code 2022 standard inspection form — item number → severity
# Source: FDA Form 3-A (Retail Food Establishment Inspection Report)
PRIORITY_ITEMS            = {4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19,
                              20, 21, 22, 23, 24, 26, 27, 28}
PRIORITY_FOUNDATION_ITEMS = {1, 3, 5, 10, 14, 25, 29}


def item_severity(item_number: int) -> str:
    """Return 'critical', 'major', or 'minor' for an FDA inspection item number."""
    if item_number in PRIORITY_ITEMS:
        return 'critical'
    if item_number in PRIORITY_FOUNDATION_ITEMS:
        return 'major'
    return 'minor'

CODE_SEVERITY = {
    "2-101.11": "Pf", "2-102.11": "Pf", "2-102.12": "C",  "2-103.11": "Pf",
    "2-201.11": "P",  "2-201.12": "P",  "2-201.13": "P",
    "2-301.11": "P",  "2-301.12": "P",  "2-301.14": "P",  "2-301.15": "Pf",
    "2-301.16": "Pf", "2-302.11": "Pf", "2-303.11": "C",  "2-304.11": "C",
    "2-401.11": "C",  "2-401.12": "C",  "2-401.13": "C",  "2-402.11": "C",
    "2-403.11": "Pf", "2-501.11": "Pf",
    "3-101.11": "P",
    "3-201.11": "P",  "3-201.12": "P",  "3-201.13": "P",  "3-201.14": "P",
    "3-201.15": "P",  "3-201.16": "P",  "3-201.17": "P",
    "3-202.11": "P",  "3-202.110": "P", "3-202.12": "P",  "3-202.13": "P",
    "3-202.14": "P",  "3-202.15": "Pf", "3-202.16": "P",  "3-202.17": "C",
    "3-202.18": "Pf", "3-203.11": "C",  "3-203.12": "Pf",
    "3-301.11": "P",  "3-301.12": "P",
    "3-302.11": "P",  "3-302.12": "C",  "3-302.13": "P",  "3-302.14": "P",
    "3-302.15": "Pf", "3-303.11": "P",  "3-303.12": "C",
    "3-304.11": "P",  "3-304.12": "C",  "3-304.13": "C",  "3-304.14": "C",
    "3-304.15": "P",  "3-304.16": "C",  "3-304.17": "P",
    "3-305.11": "C",  "3-305.12": "Pf", "3-305.13": "C",  "3-305.14": "C",
    "3-306.11": "P",  "3-306.12": "C",  "3-306.13": "P",  "3-306.14": "P",
    "3-307.11": "C",
    "3-401.11": "P",  "3-401.12": "P",  "3-401.13": "Pf", "3-401.14": "P",
    "3-401.15": "P",  "3-402.11": "P",  "3-402.12": "Pf", "3-403.11": "P",
    "3-404.11": "P",
    "3-501.11": "C",  "3-501.12": "C",  "3-501.13": "Pf", "3-501.14": "P",
    "3-501.15": "Pf", "3-501.16": "P",  "3-501.17": "Pf", "3-501.18": "P",
    "3-501.19": "P",  "3-502.11": "Pf", "3-502.12": "P",
    "3-601.11": "C",  "3-601.12": "C",  "3-602.11": "Pf", "3-602.12": "C",  "3-603.11": "Pf",
    "3-701.11": "P",  "3-801.11": "P",
    "4-101.11": "P",  "4-101.12": "C",  "4-101.13": "P",  "4-101.14": "P",
    "4-101.15": "P",  "4-101.16": "C",  "4-101.17": "C",  "4-101.18": "C",
    "4-101.19": "C",  "4-102.11": "P",  "4-201.11": "C",  "4-201.12": "P",
    "4-202.11": "Pf", "4-202.12": "Pf", "4-202.13": "C",  "4-202.14": "C",
    "4-202.15": "C",  "4-202.16": "C",  "4-202.17": "C",  "4-202.18": "C",
    "4-203.11": "Pf", "4-203.12": "Pf", "4-203.13": "C",
    "4-204.11": "C",  "4-204.110": "P", "4-204.111": "P", "4-204.112": "Pf","4-204.113": "C",
    "4-204.114": "C", "4-204.115": "Pf","4-204.116": "Pf","4-204.117": "Pf",
    "4-204.118": "C", "4-204.119": "C", "4-204.12": "C",  "4-204.120": "C",
    "4-204.121": "C", "4-204.122": "C", "4-204.123": "C", "4-204.13": "P",
    "4-204.14": "C",  "4-204.15": "C",  "4-204.16": "C",  "4-204.17": "C",
    "4-204.18": "C",  "4-204.19": "C",
    "4-301.11": "Pf", "4-301.12": "Pf", "4-301.13": "C",  "4-301.14": "C",
    "4-301.15": "C",  "4-302.11": "Pf", "4-302.12": "Pf", "4-302.13": "Pf",
    "4-302.14": "Pf", "4-303.11": "Pf", "4-401.11": "Pf", "4-402.11": "C",  "4-402.12": "C",
    "4-501.11": "C",  "4-501.110": "Pf","4-501.111": "P", "4-501.112": "Pf",
    "4-501.113": "C", "4-501.114": "P", "4-501.115": "C", "4-501.116": "Pf","4-501.12": "C",
    "4-501.13": "C",  "4-501.14": "C",  "4-501.15": "C",  "4-501.16": "C",
    "4-501.17": "Pf", "4-501.18": "C",  "4-501.19": "Pf",
    "4-502.11": "Pf", "4-502.12": "P",  "4-502.13": "C",  "4-502.14": "C",
    "4-601.11": "Pf", "4-602.11": "P",  "4-602.12": "C",  "4-602.13": "C",
    "4-603.11": "C",  "4-603.12": "C",  "4-603.13": "C",  "4-603.14": "C",
    "4-603.15": "C",  "4-603.16": "C",  "4-702.11": "P",  "4-703.11": "P",
    "4-801.11": "C",  "4-802.11": "C",  "4-803.11": "C",  "4-803.12": "C",
    "4-803.13": "C",  "4-901.11": "C",  "4-901.12": "C",  "4-902.11": "C",
    "4-902.12": "C",  "4-903.11": "C",  "4-903.12": "Pf", "4-904.11": "C",  "4-904.12": "C",
    "4-904.13": "C",  "4-904.14": "C",
    "5-101.11": "P",  "5-101.12": "P",  "5-101.13": "P",
    "5-102.11": "P",  "5-102.12": "P",  "5-102.13": "Pf", "5-102.14": "C",
    "5-103.11": "Pf", "5-103.12": "Pf", "5-104.11": "Pf", "5-104.12": "Pf",
    "5-201.11": "P",
    "5-202.11": "P",  "5-202.12": "Pf", "5-202.13": "P",  "5-202.14": "P",
    "5-202.15": "C",  "5-203.11": "Pf", "5-203.12": "C",  "5-203.13": "C",
    "5-203.14": "P",  "5-203.15": "P",  "5-204.11": "Pf", "5-204.12": "C",
    "5-204.13": "C",  "5-205.11": "Pf", "5-205.12": "P",  "5-205.13": "Pf",
    "5-205.14": "P",  "5-205.15": "P",
    "5-301.11": "P",  "5-302.11": "C",  "5-302.12": "C",  "5-302.13": "C",
    "5-302.14": "C",  "5-302.15": "C",  "5-302.16": "P",  "5-303.11": "P",
    "5-303.12": "C",  "5-303.13": "C",  "5-304.11": "P",  "5-304.12": "C",
    "5-304.13": "C",  "5-304.14": "P",
    "5-401.11": "C",  "5-402.11": "P",  "5-402.12": "C",  "5-402.13": "P",
    "5-402.14": "Pf", "5-402.15": "C",  "5-403.11": "P",  "5-403.12": "C",
    "5-501.11": "C",  "5-501.110": "C", "5-501.111": "C", "5-501.112": "C",
    "5-501.113": "C", "5-501.114": "C", "5-501.115": "C", "5-501.116": "C",
    "5-501.12": "C",  "5-501.13": "C",  "5-501.14": "C",  "5-501.15": "C",
    "5-501.16": "C",  "5-501.17": "C",  "5-501.18": "C",  "5-501.19": "C",
    "5-502.11": "C",  "5-502.12": "C",  "5-503.11": "C",
    "6-101.11": "C",  "6-102.11": "C",
    "6-201.11": "C",  "6-201.12": "C",  "6-201.13": "C",  "6-201.14": "C",
    "6-201.15": "C",  "6-201.16": "C",  "6-201.17": "C",  "6-201.18": "C",
    "6-202.11": "C",  "6-202.110": "C", "6-202.111": "P", "6-202.112": "C",
    "6-202.12": "C",  "6-202.13": "C",  "6-202.14": "C",  "6-202.15": "C",
    "6-202.16": "C",  "6-202.17": "C",  "6-202.18": "C",  "6-202.19": "C",
    "6-301.11": "Pf", "6-301.12": "Pf", "6-301.13": "C",  "6-301.14": "C",
    "6-302.11": "Pf", "6-303.11": "C",  "6-304.11": "C",  "6-305.11": "C",
    "6-402.11": "C",  "6-403.11": "C",  "6-404.11": "Pf",
    "6-501.11": "C",  "6-501.110": "C", "6-501.111": "Pf","6-501.112": "C",
    "6-501.113": "C", "6-501.114": "C", "6-501.115": "Pf","6-501.12": "C",
    "6-501.13": "C",  "6-501.14": "C",  "6-501.15": "Pf", "6-501.16": "C",
    "6-501.17": "C",  "6-501.18": "C",  "6-501.19": "C",
    "7-101.11": "Pf", "7-102.11": "Pf", "7-201.11": "P",  "7-202.11": "Pf",
    "7-202.12": "P",  "7-203.11": "P",  "7-204.11": "P",  "7-204.12": "P",  "7-204.13": "P",
    "7-204.14": "P",  "7-205.11": "P",  "7-206.11": "P",  "7-206.12": "P",
    "7-206.13": "P",  "7-207.11": "P",  "7-207.12": "P",
    "7-208.11": "P",  "7-209.11": "C",  "7-301.11": "P",
    "8-103.11": "Pf", "8-103.12": "P",  "8-201.13": "C",  "8-201.14": "Pf",
    "8-301.11": "P",  "8-302.11": "Pf", "8-304.11": "Pf",
    # Subsection overrides — where lettered sub-paragraphs differ from the base section severity
    "2-201.11(B)": "Pf", "2-201.11(E)": "Pf",
    "3-202.11(E)": "Pf", "3-202.11(F)": "Pf",
    "3-202.110(A)": "Pf",
    "3-203.11(B)": "Pf",
    "3-301.11(C)": "Pf",
    "3-304.15(A)": "P",  "3-304.15(B)": "C",  "3-304.15(C)": "C",  "3-304.15(D)": "C",
    "3-306.13(A)": "P",  "3-306.13(B)": "Pf", "3-306.13(C)": "Pf",
    "3-401.12(A)": "C",  "3-401.12(B)": "C",  "3-401.12(C)": "P",  "3-401.12(D)": "C",
    "3-401.14(F)": "Pf",
    "3-404.11(A)": "P",  "3-404.11(B)": "Pf",
    "3-501.19(A)": "Pf",
    "3-502.12(B)": "Pf",
    "3-801.11(G)": "C",
    "4-204.110(A)": "P", "4-204.110(B)": "Pf",
    "4-401.11(C)": "C",
    "4-502.11(A)": "C",  "4-502.11(B)": "Pf", "4-502.11(C)": "C",
    "4-601.11(A)": "Pf", "4-601.11(B)": "C",  "4-601.11(C)": "C",
    "5-205.12(B)": "Pf",
    "6-501.111(A)": "C", "6-501.111(B)": "C", "6-501.111(D)": "C",
    "7-202.12(C)": "Pf",
    "7-207.11(A)": "Pf",
    "7-208.11(A)": "Pf",
    "8-103.12(A)": "Pf",
}


# Plain-language descriptions for common FDA Food Code sections.
# Used as a fallback when the source portal only stores the terse section title.
CODE_DESCRIPTION: dict[str, str] = {
    # Employee health / hygiene
    "2-101.11": "Person in charge not present or does not demonstrate food safety knowledge",
    "2-102.11": "Person in charge lacks required food safety knowledge",
    "2-102.12": "No certified food protection manager on duty",
    "2-103.11": "Person in charge not ensuring employee compliance with food safety duties",
    "2-201.11": "Employee with illness or symptoms not restricted or excluded",
    "2-201.12": "Ill employee not removed from food handling",
    "2-201.13": "Employee not reporting illness to person in charge",
    "2-301.11": "Employee did not wash hands before handling food",
    "2-301.12": "Improper handwashing technique",
    "2-301.14": "Employee failed to wash hands at a required time",
    "2-301.15": "Employee washed hands in unapproved sink",
    "2-301.16": "Employee did not wash hands between handling raw and ready-to-eat food",
    "2-302.11": "Employee fingernails not trimmed or maintained",
    "2-303.11": "Employee wearing prohibited jewelry while preparing food",
    "2-304.11": "Employee outer clothing not clean",
    "2-401.11": "Employee eating, drinking, or using tobacco in unapproved area",
    "2-402.11": "Employee not wearing required hair restraint",
    "2-403.11": "Employee not using utensils or dispensing equipment to minimize bare hand contact",
    "2-501.11": "No written procedures for responding to vomiting or diarrheal events",
    # Food source / condition
    "3-101.11": "Food from unapproved or unsafe source",
    "3-201.11": "Food not from an approved source",
    "3-202.11": "Food received at improper temperature",
    "3-202.15": "Food packages not in good condition upon receipt",
    "3-301.11": "Bare hand contact with ready-to-eat food",
    "3-302.11": "Raw animal food not properly separated from ready-to-eat food",
    "3-302.12": "Food not covered or protected from contamination",
    "3-304.11": "Food contact with unclean equipment or utensils",
    "3-304.12": "In-use utensils not properly stored between uses",
    "3-304.14": "Wiping cloths not stored in sanitizing solution between uses",
    "3-304.15": "Gloves not changed or used properly",
    "3-305.11": "Food stored on floor or in unapproved location",
    "3-305.14": "Food not stored in clean, dry location away from contamination",
    "3-306.11": "Food on display not protected from contamination",
    "3-307.11": "Food not protected from contamination during storage or preparation",
    # Cooking and temperature control
    "3-401.11": "Raw animal food not cooked to required internal temperature",
    "3-401.12": "Microwave cooking does not meet required temperature",
    "3-402.11": "Raw fish not frozen before serving raw or undercooked",
    "3-403.11": "Reheated food did not reach required temperature",
    "3-501.11": "Food not maintained at proper temperature",
    "3-501.12": "Food not stored under proper time/temperature conditions",
    "3-501.13": "Food held using time only, not properly documented or discarded",
    "3-501.14": "Food not cooled from 135°F to 70°F within 2 hours or to 41°F within 6 hours",
    "3-501.15": "Improper cooling method used",
    "3-501.16": "Potentially hazardous food not held at safe temperature (41°F or below / 135°F or above)",
    "3-501.17": "Ready-to-eat food not date-marked or date mark exceeded",
    "3-501.18": "Food past use-by date or date mark not followed",
    "3-501.19": "Time as a public health control not properly documented",
    "3-502.11": "Food processing without required HACCP plan or variance",
    "3-502.12": "Specialized processing without required variance from regulatory authority",
    "3-601.12": "Food offered for human consumption not honestly presented or labeled",
    "3-602.11": "No consumer advisory for raw or undercooked animal food on menu",
    "3-603.11": "Consumer advisory not posted for raw or undercooked animal foods",
    "3-701.11": "Recalled or unsafe food not removed from sale",
    # Equipment and utensils
    "4-101.11": "Unapproved materials used for food-contact surfaces",
    "4-202.11": "Equipment not designed for easy cleaning",
    "4-301.11": "Inadequate equipment for cooling, heating, or holding food at proper temperature",
    "4-301.12": "Inadequate manual or mechanical warewashing facilities",
    "4-302.12": "No suitable thermometer provided to measure food temperature",
    "4-302.14": "No chemical test kit for measuring sanitizer concentration",
    "4-501.11": "Equipment not in good repair",
    "4-501.114": "Warewashing sanitizer concentration too low or too high",
    "4-601.11": "Food-contact surfaces not clean and sanitized",
    "4-602.11": "Food-contact surfaces not cleaned at required frequency",
    "4-602.13": "Non-food contact surfaces not cleaned at required frequency",
    "4-702.11": "Equipment not sanitized before use",
    "4-703.11": "Improper sanitization method used",
    "4-903.11": "Equipment and utensils not air-dried before storage",
    # Water, plumbing, and handwashing facilities
    "5-101.11": "Water from unapproved source",
    "5-202.12": "Handwashing sink not properly installed",
    "5-204.11": "No handwashing sink in required location",
    "5-205.11": "Handwashing sink blocked or not accessible",
    "5-205.15": "Plumbing system not maintained in good repair",
    "5-402.11": "Sewage not disposed of properly",
    # Handwashing supplies
    "6-301.11": "No soap available at handwashing sink",
    "6-301.12": "No paper towels or hand dryer at handwashing sink",
    "6-302.11": "Toilet facilities not properly constructed or maintained",
    # Physical facilities
    "6-201.11": "Floors not smooth, durable, and easily cleanable in food areas",
    "6-202.15": "Outer openings not protected against pest entry",
    "6-501.11": "Physical facility not maintained in good repair",
    "6-501.12": "Physical facility not cleaned as often as necessary",
    "6-501.111": "Evidence of insects, rodents, or other pests",
    "6-501.114": "Premises not free of unnecessary items or litter",
    "6-501.115": "Unnecessary items or dead animals present",
    # Chemicals and toxic substances
    "7-101.11": "Toxic substances improperly labeled",
    "7-201.11": "Toxic substances not properly stored away from food",
    "7-202.11": "Pesticide not approved for use in food establishment",
    "7-204.11": "Chemical sanitizer used at improper concentration",
    # Permit and compliance
    "8-103.11": "Operating without valid permit or license",
    "8-103.12": "Required permit or license not available for inspection",
    "8-304.11": "Required permit information not displayed",
    "8-302.11": "Operating without a valid permit",
}


def code_weight(code: str) -> int:
    """Severity weight from FDA code string. Falls back to base code (no subsection)."""
    import re
    sev = CODE_SEVERITY.get(code)
    if sev is None:
        base = re.sub(r'\([A-Za-z]\)$', '', code)
        sev = CODE_SEVERITY.get(base)
    if sev == "P":
        return 3
    if sev == "Pf":
        return 2
    return 1


# ── FDA Food Code 2022 — short section titles ─────────────────────────────────
# Extracted from FDA-FoodCode2022.pdf, Chapters 2–8.
# Used as display-layer overrides for verbose regulatory text stored in the DB.
# code_short_title() strips subitems and does prefix matching.

FDA_SHORT_TITLES = {
    # Chapter 2 — Management and Personnel
    '2-101.11': 'Person in charge not present',
    '2-102.11': 'Person in charge lacks food safety knowledge',
    '2-102.12': 'No certified food protection manager',
    '2-103.11': 'Person in charge not controlling unsafe operations',
    '2-201.11': 'Employees not reporting illness or health conditions',
    '2-201.12': 'Sick employee not excluded or restricted from work',
    '2-201.13': 'Excluded or restricted employee not properly managed',
    '2-301.11': 'Employee hands not clean',
    '2-301.12': 'Improper handwashing procedure',
    '2-301.14': 'Hands not washed when required',
    '2-301.15': 'Handwashing done in improper location',
    '2-301.16': 'Hand antiseptic used improperly',
    '2-302.11': 'Fingernails not trimmed or maintained',
    '2-303.11': 'Employee wearing prohibited jewelry while handling food',
    '2-304.11': 'Employee wearing dirty outer clothing',
    '2-401.11': 'Employee eating, drinking, or using tobacco in food area',
    '2-401.12': 'Employee with open wound or draining lesion working with food',
    '2-402.11': 'Employee not wearing required hair restraint',
    '2-403.11': 'Employee handling animals improperly',
    '2-501.11': 'No written procedures for vomiting or diarrheal events',
    # Chapter 3 — Food
    '3-101.11': 'Food is unsafe, adulterated, or misrepresented',
    '3-201.11': 'Food obtained from unapproved source',
    '3-201.14': 'Fish not properly identified or sourced',
    '3-201.15': 'Molluscan shellfish from unapproved source',
    '3-202.11': 'Food received at improper temperature',
    '3-202.13': 'Eggs received cracked, dirty, or from unapproved source',
    '3-202.15': 'Food package damaged or integrity compromised',
    '3-301.11': 'Bare hand contact with ready-to-eat food',
    '3-302.11': 'Raw and ready-to-eat foods not properly separated',
    '3-302.12': 'Food storage containers not labeled with contents',
    '3-302.15': 'Fruits or vegetables not washed before use',
    '3-303.12': 'Packaged food in direct contact with water or ice',
    '3-304.11': 'Food contacted unclean equipment or utensils',
    '3-304.12': 'In-use utensils stored improperly between uses',
    '3-304.14': 'Wiping cloths used or stored improperly',
    '3-304.15': 'Gloves used incorrectly or not changed when required',
    '3-305.11': 'Food stored improperly or exposed to contamination',
    '3-306.11': 'Food on display not protected from contamination',
    '3-306.13': 'Raw animal food offered for unassisted consumer self-service',
    '3-306.14': 'Previously served food offered to another consumer',
    '3-307.11': 'Food contaminated by miscellaneous source',
    '3-401.11': 'Raw animal food not cooked to required temperature',
    '3-402.11': 'Fish not frozen to destroy parasites',
    '3-403.11': 'Food not reheated to required temperature for hot holding',
    '3-501.11': 'Frozen food not maintained frozen',
    '3-501.13': 'Food thawed using improper method',
    '3-501.14': 'Food not cooled to safe temperature within required time',
    '3-501.15': 'Improper cooling methods used',
    '3-501.16': 'Hot or cold food held at improper temperature',
    '3-501.17': 'Ready-to-eat food not date marked',
    '3-501.18': 'Expired or improperly marked ready-to-eat food not discarded',
    '3-501.19': 'Time used as public health control not properly documented',
    '3-502.11': 'Specialized processing method used without required variance',
    '3-502.12': 'Reduced oxygen packaging criteria not met',
    '3-601.11': 'Food does not meet standards of identity',
    '3-602.11': 'Food label missing or inaccurate',
    '3-603.11': 'Consumer advisory not provided for raw or undercooked food',
    '3-701.11': 'Unsafe or contaminated food not properly discarded',
    '3-801.11': 'Prohibited food served or offered',
    # Chapter 4 — Equipment, Utensils, and Linens
    '4-101.11': 'Equipment material allows contamination or unsafe migration',
    '4-101.19': 'Nonfood-contact surfaces not corrosion-resistant or cleanable',
    '4-201.11': 'Equipment not durable or properly constructed',
    '4-202.11': 'Food-contact surface not smooth, sealed, or easily cleanable',
    '4-202.16': 'Nonfood-contact surfaces have cracks, ledges, or crevices',
    '4-301.11': 'Equipment lacks capacity to maintain safe food temperatures',
    '4-301.12': 'Warewashing sink compartments insufficient or improperly set up',
    '4-302.12': 'Food thermometer not available or accessible',
    '4-302.14': 'Sanitizer test kit not available',
    '4-402.11': 'Fixed equipment not properly spaced or sealed',
    '4-501.11': 'Equipment not in good repair or proper adjustment',
    '4-501.12': 'Cutting surfaces scratched, scored, or no longer cleanable',
    '4-501.14': 'Warewashing equipment not cleaned at required frequency',
    '4-502.11': 'Utensils or thermometers not in good repair or calibrated',
    '4-502.13': 'Single-use articles reused',
    '4-601.11': 'Equipment or utensils not clean',
    '4-602.11': 'Food-contact surfaces not cleaned and sanitized at required frequency',
    '4-602.12': 'Cooking or baking equipment not cleaned every 24 hours',
    '4-602.13': 'Nonfood-contact surfaces not cleaned at required frequency',
    '4-702.11': 'Equipment or utensils not sanitized before use after cleaning',
    '4-703.11': 'Sanitization method does not meet required temperatures or concentration',
    '4-901.11': 'Equipment or utensils not air-dried before storage',
    '4-903.11': 'Cleaned equipment or utensils stored improperly',
    '4-904.11': 'Single-service or single-use items not protected from contamination',
    # Chapter 5 — Water, Plumbing, and Waste
    '5-101.11': 'Water supply from unapproved source',
    '5-202.11': 'Plumbing system not approved or fixtures not cleanable',
    '5-202.12': 'Handwashing sink water not at required temperature',
    '5-202.13': 'Backflow prevention air gap not adequate',
    '5-202.14': 'Backflow prevention device does not meet design standard',
    '5-203.11': 'Insufficient number of handwashing sinks',
    '5-203.14': 'Plumbing not installed to prevent backflow',
    '5-205.11': 'Handwashing sink blocked, inaccessible, or used improperly',
    '5-205.12': 'Cross connection present or not corrected',
    '5-205.15': 'Plumbing system not maintained in good repair',
    '5-402.11': 'Sewage backflow not prevented',
    '5-501.13': 'Refuse receptacles not suitable or in poor condition',
    '5-501.15': 'Outdoor refuse receptacles lack tight-fitting lids',
    '5-501.16': 'Insufficient or improperly located refuse receptacles',
    '5-502.11': 'Refuse not removed at sufficient frequency',
    # Chapter 6 — Physical Facilities
    '6-101.11': 'Floor, wall, or ceiling surface not smooth or cleanable',
    '6-201.11': 'Floors, walls, or ceilings not easily cleanable',
    '6-201.13': 'Floor and wall junctures not properly coved or sealed',
    '6-202.11': 'Light bulbs not shatter-resistant or protected',
    '6-202.13': 'Insect control devices improperly placed or designed',
    '6-202.15': 'Outer openings not protected against pests',
    '6-301.10': 'Insufficient number of handwashing sinks provided',
    '6-301.11': 'No soap available at handwashing sink',
    '6-301.12': 'No hand drying provision at handwashing sink',
    '6-301.14': 'Handwashing sign not posted',
    '6-302.11': 'Toilet tissue not available in restroom',
    '6-303.11': 'Lighting not sufficient for food or utensil handling areas',
    '6-304.11': 'Mechanical ventilation inadequate or absent',
    '6-402.11': 'Toilet facilities not conveniently accessible',
    '6-403.11': 'No designated area for employee eating or drinking',
    '6-501.11': 'Physical facilities not in good repair',
    '6-501.12': 'Physical facilities not cleaned at required frequency',
    '6-501.14': 'Ventilation system not cleaned properly',
    '6-501.16': 'Mops not properly air-dried after use',
    '6-501.111': 'Evidence of pests or pest control inadequate',
    '6-501.112': 'Dead or trapped pests not removed promptly',
    '6-501.113': 'Refuse or recyclables not stored properly',
    '6-501.114': 'Unnecessary items or litter present on premises',
    '6-501.115': 'Live animals present in food establishment',
    # Chapter 7 — Poisonous or Toxic Materials
    '7-101.11': 'Toxic materials not properly labeled',
    '7-102.11': 'Working containers of toxic materials not identified',
    '7-201.11': 'Toxic materials not stored separately from food',
    '7-202.11': 'Prohibited toxic material present in facility',
    '7-202.12': 'Toxic materials not used according to law or manufacturer instructions',
    '7-203.11': 'Toxic material container reused for food',
    '7-204.11': 'Chemical sanitizer does not meet required criteria',
    '7-206.11': 'Restricted-use pesticide applied improperly',
    '7-206.12': 'Rodent bait not in tamper-resistant station',
    '7-207.11': 'Medicines stored or used improperly',
    '7-208.11': 'First aid supplies not properly stored',
    '7-301.11': 'Toxic retail products not separated from food items',
    # Chapter 8 — Compliance and Enforcement
    '8-301.11': 'Operating without a valid permit',
    '8-302.11': 'Permit application not submitted on time',
    '8-304.11': 'Permit holder responsibilities not met',
    '8-401.10': 'Inspection not conducted at required frequency',
    '8-404.11': 'Operations not ceased when imminent health hazard present',
    '8-404.12': 'Operations resumed without regulatory approval',
}


# ── Houston City Ordinance codes ──────────────────────────────────────────────
HOUSTON_CODE_TITLES = {
    'COH-20-20(b)': 'Operating without meeting city food establishment requirements',
    'COH-20-20(d)': 'Operations resumed before reinspection clearance',
    'COH-20-25(a)': 'Construction or remodeling without approved plans',
    'COH-20-37(b)': 'Mobile food unit operating without valid medallion',
    'COH-21-244(a)': 'No smoking signs not posted in public place',
    'COH-21-244(b)': 'No smoking sign missing at entrance',
    'COH-21-247(b)': 'Smoking not enforced in prohibited area',
    'COH-47-512(b)': 'Grease interceptor not evacuated at required frequency',
    'COH-47-522':    'Waste manifest records not properly maintained',
}


FL_ITEM_TITLES: dict[str, str] = {
    'FL-01': 'Supervisor/Person in Charge Present',
    'FL-02': 'Certified Food Manager Identification',
    'FL-03': 'Employee Health Policies Present',
    'FL-04': 'Proper Use of Restriction and Exclusion',
    'FL-05': 'Procedures for Responding to Vomiting/Diarrheal Events',
    'FL-06': 'Proper Eating, Tasting, Drinking, or Tobacco Use',
    'FL-07': 'No Discharge from Eyes, Nose, and Mouth',
    'FL-08': 'Hands Clean and Properly Washed',
    'FL-09': 'No Bare Hand Contact with Ready-to-Eat Food',
    'FL-10': 'Adequate Handwashing Facilities Supplied and Accessible',
    'FL-11': 'Food Obtained from Approved Sources',
    'FL-12': 'Food Received at Proper Temperature',
    'FL-13': 'Food in Good Condition, Safe, and Unadulterated',
    'FL-14': 'Required Records Available; Shellstock Tags/Labels',
    'FL-15': 'Proper Date Marking and Disposition',
    'FL-16': 'Potentially Hazardous Food Held at Proper Temperature',
    'FL-17': 'Proper Cooling Time and Temperature',
    'FL-18': 'Proper Cooking Temperatures for PHF',
    'FL-19': 'Consumer Advisory Posted for Raw/Undercooked Animal Foods',
    'FL-20': 'Variance/Specialized Process/HACCP Plan in Place',
    'FL-21': 'Proper Hot and Cold Holding Temperatures',
    'FL-22': 'Food Contact Surfaces Clean and Sanitized',
    'FL-23': 'Non-Food Contact Surfaces Clean',
    'FL-24': 'Warewashing Facilities Maintained and Used',
    'FL-25': 'Hot and Cold Water Available; Adequate Pressure',
    'FL-26': 'Toilet Facilities Constructed, Supplied, Cleaned',
    'FL-27': 'Sewage and Wastewater Properly Disposed',
    'FL-28': 'Plumbing Installed; Proper Backflow Devices',
    'FL-29': 'Equipment Adequate to Maintain Product Temperature',
    'FL-30': 'Food Storage Containers Properly Identified',
    'FL-31': 'Approved Thawing Methods Used',
    'FL-32': 'Toxic Substances Properly Identified, Stored, Used',
    'FL-33': 'Proper Sanitizer Contact Time and Concentration',
    'FL-34': 'Equipment and Utensils Maintained in Good Repair',
    'FL-35': 'Equipment, Food and Non-Food Contact Surfaces Approved',
    'FL-36': 'Lighting Adequate; Required Shields in Place',
    'FL-37': 'Ventilation and Filters Clean and Operable',
    'FL-38': 'Insects, Rodents, and Animals Not Present',
    'FL-39': 'Contamination Prevented During Preparation/Storage',
    'FL-40': 'Personal Cleanliness',
    'FL-41': 'Wiping Cloths Properly Used and Stored',
    'FL-42': 'Washing Fruits and Vegetables',
    'FL-43': 'In-Use Utensils Properly Stored',
    'FL-44': 'Utensils, Equipment, and Linens Properly Stored and Dried',
    'FL-45': 'Single-Use/Single-Service Articles Properly Stored and Used',
    'FL-46': 'Food Temperature Measuring Devices Provided and Accurate',
    'FL-47': 'Raw Animal Food Properly Separated from Each Other and RTE Food',
    'FL-48': 'Food Properly Stored/Protected',
    'FL-49': 'Non-Food Contact Surfaces Cleanable and Properly Maintained',
    'FL-50': 'Food and Non-Food Contact Surfaces Designed and Constructed',
    'FL-51': 'Plumbing Maintained; Sewage Disposal',
    'FL-52': 'Floors, Walls, and Ceilings Clean and in Good Repair',
    'FL-53': 'Toilet Rooms Maintained',
    'FL-54': 'Garbage and Refuse Properly Disposed',
    'FL-55': 'Physical Facilities Installed, Maintained, and Clean',
    'FL-56': 'Unnecessary Items and Litter Not Present',
    'FL-57': 'All Food Employees Have Food Handler Certificate',
    'FL-58': 'Allergen Labeling/Menu Disclosure',
}


def code_short_title(code: str) -> str | None:
    """
    Return a plain-English violation description for an FDA Food Code section number.

    Strips subitems from right to left until a match is found, e.g.:
      '3-501.14(A)(1)' → '3-501.14(A)' → '3-501.14' → 'Cooked food not cooled to safe temperature in time'
    Returns None if no match (caller should fall back to full description).
    """
    if not code:
        return None
    current = code.strip()
    if current in HOUSTON_CODE_TITLES:
        return HOUSTON_CODE_TITLES[current]
    if current in FL_ITEM_TITLES:
        return FL_ITEM_TITLES[current]
    import re as _re
    for _ in range(6):
        title = FDA_SHORT_TITLES.get(current)
        if title:
            return title
        stripped = _re.sub(r'\s*\([^)]*\)\s*$', '', current).strip()
        if stripped == current:
            break
        current = stripped
    return None
