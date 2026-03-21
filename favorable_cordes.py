"""
Favorable starting posts (cordes) by hippodrome and distance.
"""

import re

# Favorable cordes data: {hippodrome: {distance_category: [corde1, corde2, corde3]}}
FAVORABLE_CORDES = {
    'Chantilly': {
        'short': [2, 5, 3],
        'medium': [3, 1, 4],
        'long': [4, 2, 5]
    },
    'Bordeaux Le Bouscat': {
        'short': [1, 2, 5],
        'medium': [6, 4, 8],
        'long': []
    },
    'Cagnes Sur Mer': {
        'short': [5, 6, 4],
        'medium': [3, 5, 2],
        'long': [7, 3, 4]
    },
    'Clairefontaine': {
        'short': [9, 4, 10],
        'medium': [4, 2, 6],
        'long': [7, 2, 3]
    },
    'Compiegne': {
        'short': [2, 5, 4],
        'medium': [5, 1, 4],
        'long': [6, 8, 12]
    },
    'Deauville': {
        'short': [5, 6, 4],
        'medium': [4, 3, 5],
        'long': [5, 6, 9]
    },
    'Dieppe': {
        'short': [7, 5, 3],
        'medium': [5, 2, 13],
        'long': [5, 8, 6]
    },
    'Fontainebleau': {
        'short': [17, 1, 4],
        'medium': [5, 3, 2],
        'long': [6, 4, 7]
    },
    'La Teste De Buch': {
        'short': [4, 5, 6],
        'medium': [6, 5, 7],
        'long': []
    },
    'Le Lion D Angers': {
        'short': [5, 8, 7],
        'medium': [6, 3, 5],
        'long': [2, 5, 3]
    },
    'Longchamp': {
        'short': [6, 1, 4],
        'medium': [1, 2, 3],
        'long': [2, 4, 5]
    },
    'Lyon La Soie': {
        'short': [],
        'medium': [4, 5, 10],
        'long': []
    },
    'Marseille Borely': {
        'short': [5, 4, 3],
        'medium': [2, 3, 1],
        'long': [7, 1, 5]
    },
    'Marseille Vivaux': {
        'short': [5, 7, 4],
        'medium': [7, 9, 3],
        'long': [5, 7, 3]
    },
    'Nantes': {
        'short': [6, 4, 2],
        'medium': [6, 1, 5],
        'long': [2, 1, 4]
    },
    'Pau': {
        'short': [7, 8, 5],
        'medium': [4, 6, 5],
        'long': []
    },
    'Saint Cloud': {
        'short': [2, 4, 5],
        'medium': [3, 1, 4],
        'long': [3, 4, 7]
    },
    'Strasbourg': {
        'short': [10, 9, 7],
        'medium': [6, 3, 9],
        'long': []
    },
    'Toulouse': {
        'short': [7, 9, 5],
        'medium': [2, 4, 5],
        'long': []
    },
    'Vichy': {
        'short': [2, 5, 1],
        'medium': [8, 7, 4],
        'long': []
    }
}


def _parse_distance(distance_meters):
    if distance_meters is None:
        return None
    try:
        return float(distance_meters)
    except (ValueError, TypeError):
        pass
    try:
        s = str(distance_meters).replace(",", ".")
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return None


def _normalize_hippo_name(name):
    if not name:
        return ""
    s = str(name).strip().replace("’", "'")
    s = re.sub(r"\s+", " ", s)
    return re.sub(r"[^a-zA-Z0-9 ]+", "", s).strip().lower()


def _detect_horse_num_col(df):
    for c in ["N°", "NÂ°", "NÃ‚Â°", "N", "Numero", "NUM", "Num"]:
        if c in df.columns:
            return c
    return None


def get_distance_category(distance_meters):
    """
    Classify distance into short/medium/long.
    Short: < 1600m
    Medium: 1600-2400m
    Long: >= 2400m
    """
    dist = _parse_distance(distance_meters)
    if dist is None:
        return None
    if dist < 1600:
        return 'short'
    if dist < 2400:
        return 'medium'
    return 'long'


def get_favorable_cordes(hippodrome, distance_meters):
    """
    Get favorable cordes for a given hippodrome and distance.
    Handles uppercase, lowercase, and mixed case hippodrome names.

    Args:
        hippodrome: Hippodrome name (str) - can be any case
        distance_meters: Distance in meters (int or float)

    Returns:
        List of favorable corde numbers, or empty list if not found
    """
    if not hippodrome or not distance_meters:
        print(f"[DEBUG get_favorable] hippodrome={hippodrome}, distance_meters={distance_meters}")
        return []

    hippo_str = str(hippodrome).strip()
    hippo_normalized = hippo_str.title()
    hippo_norm_key = _normalize_hippo_name(hippo_str)

    print(f"[DEBUG get_favorable] Original: '{hippodrome}' -> Normalized: '{hippo_normalized}' | key='{hippo_norm_key}'")

    dist_cat = get_distance_category(distance_meters)
    print(f"[DEBUG get_favorable] Distance {distance_meters}m -> category: {dist_cat}")

    if not dist_cat:
        return []

    if hippo_normalized in FAVORABLE_CORDES:
        result = FAVORABLE_CORDES[hippo_normalized].get(dist_cat, [])
        print(f"[DEBUG get_favorable] Found in dict (exact): {result}")
        return result

    for key in FAVORABLE_CORDES.keys():
        if _normalize_hippo_name(key) == hippo_norm_key:
            result = FAVORABLE_CORDES[key].get(dist_cat, [])
            print(f"[DEBUG get_favorable] Found in dict (normalized): {result}")
            return result

    print(f"[DEBUG get_favorable] NOT FOUND in FAVORABLE_CORDES")
    return []


def compute_favorable_corde_horses(race_df, hippodrome, distance_meters, top_n=3):
    """
    Find horses with the most favorable starting posts (corde) for a race.
    Handles uppercase hippodrome names automatically.

    Args:
        race_df: DataFrame with race entries (must include horse number and CORDE columns)
        hippodrome: Hippodrome name (str) - can be any case
        distance_meters: Distance in meters (int or float)
        top_n: Number of horses to return (default 3)

    Returns:
        List of horse numbers (str) with favorable cordes, or empty list if none found
    """
    if race_df is None or race_df.empty:
        print(f"[DEBUG compute_favorable] race_df is None or empty")
        return []

    print(f"[DEBUG compute_favorable] Starting with hippodrome={hippodrome}, distance={distance_meters}")

    favorable_cordes = get_favorable_cordes(hippodrome, distance_meters)
    print(f"[DEBUG compute_favorable] get_favorable_cordes returned: {favorable_cordes}")
    if not favorable_cordes:
        print(f"[DEBUG compute_favorable] No favorable cordes found, returning empty")
        return []

    try:
        num_col = _detect_horse_num_col(race_df)
        if not num_col:
            print(f"[DEBUG compute_favorable] Horse number column not found. Available: {race_df.columns.tolist()}")
            return []
        corde_col = "CORDE" if "CORDE" in race_df.columns else ("Corde" if "Corde" in race_df.columns else None)
        if not corde_col:
            print(f"[DEBUG compute_favorable] \"CORDE\" column not found. Available: {race_df.columns.tolist()}")
            return []

        print(f"[DEBUG compute_favorable] Columns OK, filtering CORDE values in {favorable_cordes}")
        print(f"[DEBUG compute_favorable] CORDE column type: {race_df[corde_col].dtype}")
        print(f"[DEBUG compute_favorable] Sample CORDE values: {race_df[corde_col].head().tolist()}")
        print(f"[DEBUG compute_favorable] Sample CORDE types: {[type(x).__name__ for x in race_df[corde_col].head().tolist()]}")

        def _parse_corde(v):
            try:
                if v is None:
                    return None
                s = str(v).replace(",", ".")
                m = re.search(r"\d+", s)
                return int(m.group(0)) if m else None
            except Exception:
                return None

        race_df = race_df.copy()
        race_df["_CORDE_NUM"] = race_df[corde_col].apply(_parse_corde)
        favorable_set = set(int(x) for x in favorable_cordes if str(x).isdigit())
        df_favorable = race_df[race_df["_CORDE_NUM"].isin(favorable_set)].copy()
        print(f"[DEBUG compute_favorable] After filter: {len(df_favorable)} horses match favorable cordes")

        if df_favorable.empty:
            print(f"[DEBUG compute_favorable] No horses with favorable cordes found")
            return []

        def corde_rank(corde_val):
            try:
                corde_int = int(corde_val) if corde_val is not None else None
                if corde_int in favorable_cordes:
                    return favorable_cordes.index(corde_int)
                return float("inf")
            except (ValueError, TypeError):
                return float("inf")

        df_favorable["corde_rank"] = df_favorable["_CORDE_NUM"].apply(corde_rank)
        df_favorable = df_favorable.sort_values("corde_rank")

        top_horses = df_favorable[num_col].astype(str).head(top_n).tolist()
        print(f"[DEBUG compute_favorable] Returning top horses: {top_horses}")
        return top_horses

    except Exception as e:
        print(f"Error computing favorable corde horses: {e}")
        import traceback
        traceback.print_exc()
        return []
