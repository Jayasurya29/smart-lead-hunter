# -*- coding: utf-8 -*-
"""Organization-name normalization for grouping inbox contacts by property.

Single source of truth, used by:
  - contact_dedup.upsert_contact   (stamps `organization_normalized` on write)
  - alembic 029                    (backfills the column for existing rows)

Design rule (product call): UNDER-merging is safe, OVER-merging is dangerous.
We only collapse *formatting* differences — case, punctuation, surrounding
whitespace, word order, and trailing legal suffixes (Inc / LLC / Ltd / Co /
Corp / ...). We deliberately do NOT strip meaningful words like "hotel",
"resort", "residences", "group", or "international", because those distinguish
genuinely different entities (e.g. "The Setai Hotel" vs "The Setai Residences").
A different city is a different word, so it stays a different group —
"Hilton Orlando" never merges with "Hilton Miami".

Contacts already matched to a real lead/hotel are grouped by that id instead
(see contact_dedup.list_contact_groups) and never touch this function — so this
only governs the unmatched fallback.
"""

import re

# Legal / corporate suffixes that carry no identity — safe to drop.
_SUFFIX_STOPWORDS = {
    "the",
    "inc",
    "incorporated",
    "llc",
    "llp",
    "lp",
    "ltd",
    "limited",
    "co",
    "corp",
    "corporation",
    "company",
    "plc",
    "gmbh",
    "sa",
    "sas",
    "bv",
    "nv",
}

_NON_ALNUM = re.compile(r"[^a-z0-9 ]+")  # punctuation/symbols -> space
_MULTISPACE = re.compile(r"\s+")


def normalize_organization(name):
    """Return a stable grouping key for an org name, or None if empty.

    The key is word-order independent (tokens are sorted + de-duped), so two
    spellings that differ only in formatting/order collapse together, while two
    different properties — which differ by at least one real word — never do.
    """
    if not name or not str(name).strip():
        return None

    s = str(name).lower()
    s = _NON_ALNUM.sub(" ", s)
    s = _MULTISPACE.sub(" ", s).strip()
    if not s:
        return None

    tokens = [t for t in s.split(" ") if t and t not in _SUFFIX_STOPWORDS]
    # If every token was a stopword (e.g. "The Co."), fall back to the cleaned
    # string so we still emit a stable key rather than None.
    if not tokens:
        tokens = [t for t in s.split(" ") if t]

    tokens = sorted(set(tokens))
    return " ".join(tokens) or None
