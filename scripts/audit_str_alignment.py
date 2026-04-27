"""
STR 2023 Chain Scales → JA canonical_tiers.py audit script.

Run anytime to verify that brand tier assignments still match STR's
official chain scale list. Catches drift if anyone adds a brand at the
wrong tier or if STR re-classifies a brand in a future release.

Usage:
  python -m scripts.audit_str_alignment

Source data: STR 2023 Chain Scales (gov-archive PDF, McKinney TX
attachment). When STR releases a 2024+ update, replace the STR_DATA
block below with the new tier assignments.

Success criterion: 0 discrepancies. Anything else means JA's tier list
drifted from STR truth.
"""
import re

STR_DATA = '''
1 Luxury Chains:1 Hotel,21c Museum Hotel,AKA,Alila,Aman,Andaz,Auberge Resorts Collection,Belmond Hotels,Bulgari,Conrad,Delano,Destination by Hyatt,Dorchester Collection,Edition,Faena,Fairmont,Firmdale,Four Seasons,Grand Hyatt,InterContinental,JW Marriott,Kirkwood Collection,Langham,Loews,Lotte Hotel,Luxury Collection,LXR Hotels & Resorts,Mandarin Oriental,Mantis Collection,Miraval,Mokara,Mondrian,Montage,Nobu Hotels,Oetker Collection,One & Only,Park Hyatt,Pendry,Raffles,Regent,Ritz-Carlton,RockResorts,Rosewood,Six Senses,Sixty Hotels,SLS,Sofitel,St. Regis,Taj,The Doyle Collection,The Peninsula,The Unbound Collection,Thompson Hotels,Trump International,Under Canvas,Viceroy,Vignette Collection,Virgin Hotels,W Hotel,Waldorf Astoria
2 Upper Upscale Chains:Ace Hotel,Autograph Collection,Canopy by Hilton,Club Quarters,Curio Collection by Hilton,Disney's Deluxe Resorts,Dolce Hotels & Resorts,Dream Hotels,Embassy Suites by Hilton,Fireside Inn & Suites,Gaylord,Graduate Hotel,Great Wolf Lodge,Hard Rock,Hilton,Hilton Grand Vacations,Hotel Indigo,Hotel Nikko,Hyatt,Hyatt Centric,Hyatt Regency,Hyde,Instinct Hotels,JdV by Hyatt,Kasa,Kimpton,Le Meridien,Life House,Magnolia,Margaritaville,Marriott,Marriott Conference Center,Marriott Vacation Club,MGallery by Sofitel,Mint House,NH Collection,Omni,Outrigger Resorts,Pan Pacific,Pullman,Radisson Blu by Choice,Radisson RED by Choice,Renaissance,Royal Sonesta,Sandman Signature,Sheraton Hotel,Sheraton Vacation Club,Signia by Hilton,Silver Cloud,Starhotels,Swissotel,Tapestry Collection by Hilton,The Guild,The Hoxton,The Marmara,The Standard,Tribute Portfolio,Unscripted,Valencia Hotel Group,Warwick Hotel,Westin,Westin Vacation Club,Wyndham Grand
3 Upscale Chains:AC Hotels by Marriott,aloft Hotel,APA Hotel,Ascend Collection,Aston Hotel,Axel Hotel,Ayres,Best Western Premier,BW Premier Collection,Cambria Hotels,Canad Inn,Caption by Hyatt,Citadines,citizenM,Coast Hotels,Compass by Margaritaville,Courtyard,Crowne Plaza,Delta Hotel,Disney's Moderate Resorts,DoubleTree by Hilton,Eaton,element,Eurostars,EVEN Hotels,Four Points by Sheraton,Grand America,Hilton Garden Inn,Holiday Inn Club Vacations,Homewood Suites by Hilton,Hotel RL,Hyatt House,Hyatt Place,Iberostar Hotels & Resorts,Innside by Melia,Larkspur Landing,Legacy Vacation Club,Mantra,Melia,Millennium,Miyako,Mysk by Shaza,Novotel,Oakwood Residence,Park Plaza by Choice,Pestana,Pestana CR7,Radisson by Choice,Radisson Individuals by Choice,Residence Inn,RIU Plaza,Sonesta Hotel,Sonesta Select,SpringHill Suites,Staybridge Suites,Stoney Creek,Tempo by Hilton,Vacation Condos by Outrigger,Vib,voco,Westmark,Wyndham,Wyndham Vacation Resort,YOTEL
4 Upper Midscale Chains:Aiden by Best Western,Aqua Hotels & Resorts,Atwell Suites,Best Western Executive Residency,Best Western Plus,Boarders Inn & Suites,BW Signature Collection,Centerstone Hotels,Chase Suites,Clarion,Clarion Pointe,Cobblestone,Comfort,Comfort Inn,Comfort Suites,Country Inn & Suites by Choice,Disney's Value Resorts,DoubleTree Club,Drury Inn & Suites,Drury Plaza Hotel,Fairfield Inn,GLo Best Western,GrandStay Hotels,Hampton by Hilton,Holiday Inn,Holiday Inn Express,Home2 Suites by Hilton,Isle of Capri,La Quinta Inns & Suites,Magnuson Grand,Mama Shelter,Motto by Hilton,MOXY,OHANA,Oxford Suites,Park Inn by Choice,Quality,Red Lion Hotel,Shilo Inn,Sonesta ES Suites,Sonesta Essential,The Red Collection,TownePlace Suites,Trademark Collection by Wyndham,Tryp by Wyndham,Universal,WaterWalk,Westgate,Wyndham Garden
5 Midscale Chains:A Victory,AmericInn,Avid,Baymont,Best Western,Candlewood Suites,Coratel Inn & Suites,Crystal Inn,Days Inn,Everhome Suites,Extend-a-Suites,Extended Stay America Premier Suites,Extended Stay America Suites,FairBridge Inn,FairBridge Inn Express,Generator Hostel,GreenTree Inn,GuestHouse,Hawthorn Suites by Wyndham,InnSuites Hotel,Loyalty Inn,Magnuson,MainStay Suites,Motel One,My Place,Palace Inn,Quality Inn,Ramada,Red Lion Inn & Suites,Rode Inn,Selina,Signature Inn,Sleep Inn,Sonesta Simply Suites,Spark by Hilton,Stayable Suites,stayAPT Suites,Tru by Hilton,Uptown Suites,Vista,Wingate by Wyndham
6 Economy Chains:Affordable Suites of America,America's Best Inn,Americas Best Value Inn,AmeriVu Inn & Suites,Budget Host,Budget Suites of America,Budgetel,Country Hearth Inn,Days Inn,Downtowner Inn,ECHO Suites Extended Stay by Wyndham,Econo Lodge,Efficiency Lodge,Extended Stay America Premier Suites,Extended Stay America Select Suites,E-Z 8,Good Nite Inn,Great Western,Henn na Hotel,HomeTowne Studios by Red Roof,Howard Johnson,InTown Suites,Jameson Inn,Key West Inn,Knights Inn,Lite Hotels,LivAway Suites,M Star,Master Hosts Inns,Microtel Inn & Suites by Wyndham,Motel 6,National 9,OYO,OYO Townhouse,Passport Inn,Pear Tree Inn,Red Carpet Inn,Red Roof Inn,Red Roof PLUS+,Rodeway Inn,Scottish Inn,Select Inn,Siegel Select,Siegel Suite,Studio 6,Suburban Studios,Super 8,SureStay,SureStay Collection,SureStay Plus,SureStay Studio,Travelodge,Vagabond Inn,WoodSpring Suites
'''.strip()

STR_BRAND_TIER = {}
for line in STR_DATA.split('\n'):
    tier_label, brands = line.split(':', 1)
    tier_num = int(tier_label.split()[0])
    for b in brands.split(','):
        STR_BRAND_TIER[b.strip().lower()] = tier_num


def ja_for_str(str_tier):
    if str_tier == 1:
        return 'tier1_or_2_luxury'
    if str_tier == 2:
        return 'tier3_upper_upscale'
    if str_tier == 3:
        return 'tier4_upscale'
    return 'tier5_skip'


def main():
    # Explicit utf-8 encoding — canonical_tiers.py contains Curaçao, Mövenpick,
    # São Paulo etc. which break Windows' default cp1252 codec.
    with open('app/config/canonical_tiers.py', encoding='utf-8') as f:
        content = f.read()
    matches = re.findall(r'"([^"]+)":\s*"(tier\d_\w+)"', content)
    ja = {b: t for b, t in matches}

    discrepancies = []
    for str_brand, str_tier in STR_BRAND_TIER.items():
        expected = ja_for_str(str_tier)
        candidates = [
            str_brand, str_brand.replace('&', 'and'),
            str_brand.replace(' & ', ' '),
            str_brand.replace("'s", '').strip(),
            str_brand.replace('-', ' '), str_brand.replace('.', ''),
        ]
        actual = None
        for c in candidates:
            if c in ja:
                actual = ja[c]
                break
        if actual is None:
            discrepancies.append((str_brand, expected, 'NOT_IN_JA', str_tier))
        else:
            if expected == 'tier1_or_2_luxury':
                ok = actual in ('tier1_ultra_luxury', 'tier2_luxury')
            else:
                ok = actual == expected
            if not ok:
                discrepancies.append((str_brand, expected, actual, str_tier))

    print("\nSTR 2023 → JA AUDIT")
    print(f"STR brands: {len(STR_BRAND_TIER)}, JA brands: {len(ja)}, "
          f"Discrepancies: {len(discrepancies)}")

    str_label = {1: 'Luxury', 2: 'Up.Up', 3: 'Upscale', 4: 'Up.Mid',
                 5: 'Midscale', 6: 'Economy'}
    mistier = [d for d in discrepancies if d[2] != 'NOT_IN_JA']
    not_in_ja = [d for d in discrepancies if d[2] == 'NOT_IN_JA']

    if mistier:
        print(f"\n--- WRONG TIER ({len(mistier)}) ---")
        for brand, expected, actual, st in sorted(mistier, key=lambda x: (x[3], x[0])):
            print(f"  {brand:<40} STR:{str_label[st]:<8} expected:{expected:<22} got:{actual}")
    if not_in_ja:
        print(f"\n--- MISSING FROM JA ({len(not_in_ja)}) ---")
        for brand, expected, _, st in sorted(not_in_ja, key=lambda x: (x[3], x[0])):
            print(f"  {brand:<40} STR:{str_label[st]:<8} → add as {expected}")

    return 0 if not discrepancies else 1


def audit_registry_vs_canonical() -> int:
    """Audit brand_registry.py tier field vs canonical_tiers.py.

    Returns the number of conflicts. Should be 0 because brand_registry
    auto-syncs at import — but this catches the case where a developer
    adds a brand to brand_registry that's MISSING from canonical_tiers
    (auto-sync only overrides matches, doesn't add new entries).
    """
    try:
        from app.config.canonical_tiers import CANONICAL_TIERS
        from app.config.brand_registry import BRAND_REGISTRY, BrandRegistry
    except ImportError as e:
        print(f"\n[REGISTRY AUDIT] Skipped — imports failed: {e}")
        return 0

    print("\nbrand_registry.py → canonical_tiers.py AUDIT")

    not_in_canonical: list[tuple[str, str]] = []
    for reg_key, info in BRAND_REGISTRY.items():
        if reg_key == "unknown":
            continue
        if reg_key in CANONICAL_TIERS:
            continue
        # Try aliases — this brand might be reachable via an alias key
        aliased_to_canonical = False
        for alias_src, alias_dst in BrandRegistry.ALIASES.items():
            if alias_dst == reg_key and alias_src in CANONICAL_TIERS:
                aliased_to_canonical = True
                break
        if not aliased_to_canonical:
            not_in_canonical.append((reg_key, info.tier))

    print(f"Registry entries not findable in canonical_tiers: {len(not_in_canonical)}")
    if not_in_canonical:
        print("These brands have hardcoded tiers that auto-sync can't verify:")
        for reg_key, reg_tier in sorted(not_in_canonical)[:30]:
            print(f"  {reg_key:<40} (registry tier: {reg_tier})")
        if len(not_in_canonical) > 30:
            print(f"  ... + {len(not_in_canonical) - 30} more")
        print(
            "\nFix: add these brands to canonical_tiers.py with the correct STR tier, "
            "or alias them to an existing canonical_tiers entry via "
            "BrandRegistry.ALIASES."
        )
    return len(not_in_canonical)


if __name__ == "__main__":
    str_audit = main()
    registry_audit = audit_registry_vs_canonical()
    raise SystemExit(0 if (str_audit == 0 and registry_audit == 0) else 1)
