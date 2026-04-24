"""
Brand Tier Audit — finds tier conflicts across the THREE sources of truth:

  1. app/config/brand_registry.py     (canonical brand metadata)
  2. app/services/lead_data_enrichment.py TIER RULES  (Smart Fill prompt)
  3. app/services/scorer.py TIER1/2/3/4 lists        (lead scoring)

All three drive different behaviors:
  - Registry  → contact enrichment picks who to call (brand_managed vs franchised)
  - Prompt    → Gemini's brand_tier extraction from snippets
  - Scorer    → 0-25 points on the lead's 100-point score + revenue calc tier

When they disagree, you get weird results:
  - Smart Fill writes "tier2_luxury" (from prompt) but registry says tier3
  - Scorer gives 15 pts (tier3 in its list) but displayed tier is tier2
  - Revenue formula uses saved DB tier (tier2) — higher staff count than registry implies

Run:
    python -m scripts.audit_brand_tiers           # pretty report
    python -m scripts.audit_brand_tiers --csv     # CSV for spreadsheet review
    python -m scripts.audit_brand_tiers --fix-plan  # generate SQL/patch suggestions
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ══════════════════════════════════════════════════════════════════════
# INDUSTRY STANDARD (STR CHAIN SCALE) — May 2024 publication
# Curated from STR's North America + Caribbean chain scale, cross-checked
# against CBRE / MMCG / HSMAI sources (April 2026). Use this as the
# "industry" baseline when flagging conflicts.
# ══════════════════════════════════════════════════════════════════════

STR_LUXURY = {  # = JA tier1 OR tier2 (JA splits STR Luxury into two)
    # Ultra-luxury flagships
    "aman", "four seasons", "ritz-carlton", "ritz carlton", "rosewood",
    "mandarin oriental", "peninsula", "st. regis", "st regis",
    "waldorf astoria", "waldorf-astoria", "park hyatt", "montage",
    "auberge", "one & only", "one&only", "pendry", "capella",
    "raffles", "six senses", "cheval blanc", "como hotels",
    "corinthia", "dorchester collection", "kempinski", "regent",
    "oetker collection", "rocco forte", "belmond", "bulgari", "bvlgari",
    "faena", "lxr", "edition", "jw marriott", "w hotels", "w hotel",
    "conrad", "fairmont", "sofitel", "luxury collection",
    "the luxury collection", "ritz-carlton reserve", "langham",
    "nobu hotel", "viceroy", "thompson", "andaz", "alila",
    "taj", "shangri-la", "armani hotel", "oberoi",
    # STR 2024 explicitly confirmed Luxury (verified from STR spreadsheet):
    "grand hyatt", "intercontinental", "miraval", "design hotels",
    "delano", "mondrian", "sls", "banyan tree", "signia by hilton",
    "vignette collection", "virgin hotels", "unbound collection",
    "the unbound collection by hyatt", "1 hotel", "1 hotels", "1hotel",
    # STR 2024 Luxury all-inclusives (verified from STR spreadsheet):
    "sandals", "beaches", "royalton", "breathless resorts",
    "dreams resorts", "secrets resorts", "now resorts", "zoetry",
    "hyatt zilara", "hyatt ziva", "palace resorts", "moon palace",
    "grand palladium", "iberostar grand", "iberostar grand collection",
    "paradisus", "hyatt vivid hotels", "hyatt vivid hotels & resorts",
    # Caribbean/US luxury independents
    "acqualina", "baha mar", "baker's bay", "fontainebleau",
    "grand wailea", "greenbrier", "hamilton princess",
    "kamalame cay", "musha cay", "nemacolin", "newbury boston",
    "sea island", "the biltmore", "the breakers", "the broadmoor",
    "the setai", "the surf club", "grace bay club",
    "sanctuary cap cana", "tucker's point", "vidanta", "wynn",
    "zemi beach", "dorado beach", "hualuxe", "boca raton resort",
    "canyon ranch", "equinox hotel", "mr. c", "cap juluca",
}

STR_UPPER_UPSCALE = {  # = JA tier3
    "marriott", "marriott hotels", "marriott marquis", "sheraton",
    "westin", "hyatt regency", "hyatt centric", "hilton hotels", "hilton",
    "hard rock hotel", "hard rock", "renaissance hotels", "renaissance",
    "loews", "le meridien", "delta hotels",  # Delta is Upscale in STR 2023 - flagged separately
    "embassy suites", "embassy suites by hilton",
    "autograph collection", "tribute portfolio", "curio collection",
    "curio collection by hilton", "canopy by hilton", "kimpton",
    "hotel indigo", "gaylord", "omni",
    "hilton tapestry collection", "tapestry collection by hilton",
    "mgallery", "pullman", "movenpick", "swissotel", "radisson",
    "hoxton", "ace hotel", "graduate hotels", "graduate",
    "wyndham grand", "margaritaville",  # STR 2023 confirmed Upper Upscale
    "outrigger", "sonesta", "caribe hilton",
    "jdv by hyatt", "destination by hyatt", "bunkhouse",
    "unscripted by hyatt", "dream hotel", "life house",
}

STR_UPSCALE = {  # = JA tier4
    "ac hotels", "ac hotel", "aloft", "courtyard", "hilton garden inn",
    "hyatt place", "hyatt house", "four points", "element",
    "residence inn", "springhill suites", "home2", "homewood suites",
    "moxy", "holiday inn", "caption by hyatt", "motto by hilton",
    "spark by hilton", "tempo by hilton", "mercure", "novotel",
    "garner", "cambria", "cambria hotels", "voco",
    # STR 2023 explicitly Upscale (moved down from Upper Upscale):
    "crowne plaza", "doubletree", "doubletree by hilton",
    "delta hotels", "delta hotel", "citizenm",
    "iberostar", "riu", "barcelo", "karisma", "club med",
    "playa resorts", "divi resorts", "compass by margaritaville",
}


def str_tier_for(brand: str) -> Optional[str]:
    """Return STR canonical tier for a brand, or None if not classified."""
    b = brand.lower().strip()
    if b in STR_LUXURY:
        return "Luxury (JA tier1 or tier2)"
    if b in STR_UPPER_UPSCALE:
        return "Upper Upscale (JA tier3)"
    if b in STR_UPSCALE:
        return "Upscale (JA tier4)"
    return None


# ══════════════════════════════════════════════════════════════════════
# LOADERS — extract tier info from each of the three sources
# ══════════════════════════════════════════════════════════════════════

def load_registry() -> dict[str, str]:
    """brand → tier from app/config/brand_registry.py"""
    import types
    import importlib.util

    # Mock dependencies the registry might import
    sys.modules.setdefault("app.services.utils", types.ModuleType("app.services.utils"))

    spec = importlib.util.spec_from_file_location(
        "br", str(_PROJECT_ROOT / "app/config/brand_registry.py")
    )
    br = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(br)

    # Find the big dict (has 'marriott' in it)
    for name in dir(br):
        val = getattr(br, name)
        if isinstance(val, dict) and val and "marriott" in str(val)[:2000].lower():
            registry = val
            break
    else:
        raise RuntimeError("Could not find brand registry dict")

    out = {}
    for brand, info in registry.items():
        tier = getattr(info, "tier", None)
        if tier:
            out[brand.lower().strip()] = tier
    return out


def load_prompt_tier_rules() -> dict[str, str]:
    """brand → tier from Smart Fill prompt TIER RULES block"""
    text = (
        _PROJECT_ROOT / "app/services/lead_data_enrichment.py"
    ).read_text(encoding="utf-8")

    # Match the TIER RULES block — each line looks like:
    #   tier2_luxury: Sandals, Royalton, Nickelodeon, ...
    out = {}
    for tier_key in (
        "tier1_ultra_luxury",
        "tier2_luxury",
        "tier3_upper_upscale",
        "tier4_upscale",
        "tier5_upper_midscale",
        "tier6_midscale",
        "tier7_economy",
    ):
        m = re.search(rf"{tier_key}:\s*(.+)", text)
        if not m:
            continue
        brands_str = m.group(1).strip()
        for token in brands_str.split(","):
            token = token.strip().lower()
            # Skip descriptive tokens like "themed/experiential resorts"
            if not token or token.endswith(" resorts") or "experiential" in token or "all-inclusive" in token:
                continue
            # Drop trailing parentheticals
            token = re.sub(r"\s*\(.*\)\s*$", "", token)
            if token:
                out[token] = tier_key
    return out


def load_scorer_tiers() -> dict[str, str]:
    """brand → tier from scorer.py TIER1/2/3/4/5 lists"""
    text = (
        _PROJECT_ROOT / "app/services/scorer.py"
    ).read_text(encoding="utf-8")

    out = {}
    for tier_key, list_name in (
        ("tier1_ultra_luxury", "TIER1_ULTRA_LUXURY"),
        ("tier2_luxury", "TIER2_LUXURY"),
        ("tier3_upper_upscale", "TIER3_UPPER_UPSCALE"),
        ("tier4_upscale", "TIER4_UPSCALE"),
        ("tier5_skip", "TIER5_SKIP"),
    ):
        # Find the list declaration
        m = re.search(
            rf"^{list_name}\s*=\s*\[(.*?)^\]",
            text,
            re.MULTILINE | re.DOTALL,
        )
        if not m:
            continue
        block = m.group(1)
        # Extract string literals
        for token in re.findall(r'"([^"]+)"', block):
            t = token.strip().lower()
            if t and t not in out:  # first-match-wins (matches scorer behavior)
                out[t] = tier_key
    return out


# ══════════════════════════════════════════════════════════════════════
# CONFLICT DETECTION
# ══════════════════════════════════════════════════════════════════════

def tier_compatible_with_str(tier: str, str_label: Optional[str]) -> bool:
    """Check if a JA tier matches the STR chain scale bucket."""
    if str_label is None:
        return True  # Can't verify — don't flag
    if "tier1_ultra_luxury" in tier or "tier2_luxury" in tier:
        return "Luxury" in str_label
    if "tier3_upper_upscale" in tier:
        return "Upper Upscale" in str_label
    if "tier4_upscale" in tier:
        return "Upscale" in str_label
    return True


def audit():
    registry = load_registry()
    prompt = load_prompt_tier_rules()
    scorer = load_scorer_tiers()

    # Load canonical as well — it's the new source of truth that scorer and
    # prompt derive from. Brands in canonical are considered "covered
    # everywhere" even if scorer/prompt no longer have their own lists.
    canonical: dict[str, str] = {}
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_canonical_audit",
            str(_PROJECT_ROOT / "app/config/canonical_tiers.py"),
        )
        ct = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ct)
        canonical = ct.CANONICAL_TIERS
    except Exception as e:
        print(f"  ⚠️  Could not load canonical_tiers.py: {e}")

    print("═" * 90)
    print("  BRAND TIER AUDIT — 3-way consistency check")
    print("═" * 90)
    print()
    print(f"  Registry:  {len(registry):>4} brands")
    print(f"  Prompt:    {len(prompt):>4} brands (TIER RULES in Smart Fill)")
    print(f"  Scorer:    {len(scorer):>4} brands (TIER1/2/3/4/5 lists)")
    if canonical:
        print(f"  Canonical: {len(canonical):>4} brands (single source of truth)")
    print()

    # Gather ALL brands mentioned anywhere (including canonical)
    all_brands = (
        set(registry.keys())
        | set(prompt.keys())
        | set(scorer.keys())
        | set(canonical.keys())
    )

    # Find conflicts
    hard_conflicts = []   # HIGH: registry vs prompt OR registry vs scorer OR vs canonical disagree
    str_conflicts = []    # HIGH: any source disagrees with STR industry standard
    coverage_gaps = []    # LOW: brand only in one source AND not in canonical

    for brand in sorted(all_brands):
        r = registry.get(brand)
        p = prompt.get(brand)
        s = scorer.get(brand)
        c = canonical.get(brand)
        str_label = str_tier_for(brand)

        # Internal conflict — any two declared tiers disagree
        tiers_declared = {t for t in (r, p, s, c) if t}
        if len(tiers_declared) > 1:
            hard_conflicts.append({
                "brand": brand,
                "registry": r or "—",
                "prompt": p or "—",
                "scorer": s or "—",
                "canonical": c or "—",
                "str": str_label or "—",
            })
            continue

        # STR conflict (a declared tier contradicts STR industry standard)
        for source, tier in (
            ("registry", r), ("prompt", p), ("scorer", s), ("canonical", c),
        ):
            if tier and not tier_compatible_with_str(tier, str_label):
                str_conflicts.append({
                    "brand": brand,
                    "source": source,
                    "declared": tier,
                    "str_says": str_label,
                })

        # Coverage gap — brand declared in only ONE source, excluding canonical.
        # If it's in canonical, scorer+prompt reach it transitively, so
        # not-being-in-scorer-or-prompt is NOT a gap — that's the whole
        # point of the single-source-of-truth refactor.
        sources_declaring = sum(
            1 for t in (r, p, s, c) if t
        )
        if sources_declaring == 1 and r and not c:
            # Brand is in registry but not in canonical → real gap
            # (scorer/prompt won't know about it)
            coverage_gaps.append(
                {"brand": brand, "only_in": "registry", "tier": r}
            )

    # ═══ REPORT ═══
    print("─" * 90)
    print(f"  HARD CONFLICTS — our 3 sources disagree  ({len(hard_conflicts)})")
    print("─" * 90)
    if hard_conflicts:
        print(f"  {'BRAND':<30} {'REGISTRY':<22} {'PROMPT':<22} {'SCORER':<22}")
        print("  " + "─" * 88)
        for c in hard_conflicts:
            print(f"  {c['brand']:<30} {c['registry']:<22} {c['prompt']:<22} {c['scorer']:<22}")
    else:
        print("  (none)")
    print()

    print("─" * 90)
    print(f"  STR CONFLICTS — declared tier disagrees with industry standard  ({len(str_conflicts)})")
    print("─" * 90)
    if str_conflicts:
        print(f"  {'BRAND':<30} {'SOURCE':<12} {'DECLARED':<22} STR SAYS")
        print("  " + "─" * 88)
        for c in str_conflicts:
            print(f"  {c['brand']:<30} {c['source']:<12} {c['declared']:<22} {c['str_says']}")
    else:
        print("  (none)")
    print()

    print("─" * 90)
    print(f"  COVERAGE GAPS — brand only declared in one source  ({len(coverage_gaps)})")
    print("─" * 90)
    print("  Only first 20 shown. These don't cause bugs but risk future drift.")
    for c in coverage_gaps[:20]:
        print(f"  {c['brand']:<35} only in {c['only_in']:<12} ({c['tier']})")
    if len(coverage_gaps) > 20:
        print(f"  ... and {len(coverage_gaps) - 20} more")
    print()

    print("═" * 90)
    print("  SUMMARY")
    print("═" * 90)
    print(f"  Hard conflicts (fix these first):  {len(hard_conflicts)}")
    print(f"  STR-disagreement conflicts:        {len(str_conflicts)}")
    print(f"  Coverage gaps:                     {len(coverage_gaps)}")
    print()

    return hard_conflicts, str_conflicts, coverage_gaps


def print_csv(hard, str_conflicts, gaps):
    print("type,brand,registry,prompt,scorer,str_industry,notes")
    for c in hard:
        print(f"hard_conflict,{c['brand']},{c['registry']},{c['prompt']},{c['scorer']},{c['str']},")
    for c in str_conflicts:
        print(f"str_conflict,{c['brand']},,,,{c['str_says']},{c['source']}={c['declared']}")
    for c in gaps:
        print(f"coverage_gap,{c['brand']},,,,,only in {c['only_in']} ({c['tier']})")


def main():
    parser = argparse.ArgumentParser(
        description="Audit brand tiers across registry + prompt + scorer"
    )
    parser.add_argument(
        "--csv", action="store_true", help="Output CSV format for spreadsheet review"
    )
    args = parser.parse_args()

    hard, str_conflicts, gaps = audit()

    if args.csv:
        print()
        print("══════════ CSV OUTPUT ══════════")
        print_csv(hard, str_conflicts, gaps)


if __name__ == "__main__":
    main()
