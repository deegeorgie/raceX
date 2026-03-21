import pandas as pd

def compute_race_statistics(race_df, composite_df=None):
    """
    Compute race statistics for a single race: average odds, median odds, mean composite, median composite.
    Returns a dictionary with statistics and horse numbers for each category.
    """
    try:
        if race_df is None or race_df.empty:
            return {}
        
        stats = {}
        
        # Get odds column (prefer COTE)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        if cote_col:
            odds = pd.to_numeric(race_df[cote_col], errors='coerce').dropna()
            if not odds.empty:
                stats['avg_odds'] = odds.mean()
                stats['median_odds'] = odds.median()
                
                # Find horses with average odds (within 0.5 of average)
                avg_mask = abs(odds - stats['avg_odds']) <= 0.5
                avg_indices = odds[avg_mask].index
                stats['avg_odds_horses'] = race_df.loc[avg_indices, 'N°'].astype(str).str.replace('.0', '').tolist() if 'N°' in race_df.columns else []
                
                # Find horses with median odds (within 0.5 of median)
                median_mask = abs(odds - stats['median_odds']) <= 0.5
                median_indices = odds[median_mask].index
                stats['median_odds_horses'] = race_df.loc[median_indices, 'N°'].astype(str).str.replace('.0', '').tolist() if 'N°' in race_df.columns else []
        
        # Get composite scores if available
        if composite_df is not None and not composite_df.empty and 'Composite' in composite_df.columns:
            comp = pd.to_numeric(composite_df['Composite'], errors='coerce').dropna()
            if not comp.empty:
                stats['mean_composite'] = comp.mean()
                stats['median_composite'] = comp.median()
                
                # Find horses with mean composite (within 0.05 of mean)
                mean_mask = abs(comp - stats['mean_composite']) <= 0.05
                mean_indices = comp[mean_mask].index
                stats['mean_composite_horses'] = composite_df.loc[mean_indices, 'N°'].astype(str).str.replace('.0', '').tolist() if 'N°' in composite_df.columns else []
                
                # Find horses with median composite (within 0.05 of median)
                median_mask = abs(comp - stats['median_composite']) <= 0.05
                median_indices = comp[median_mask].index
                stats['median_composite_horses'] = composite_df.loc[median_indices, 'N°'].astype(str).str.replace('.0', '').tolist() if 'N°' in composite_df.columns else []
        
        return stats
    except Exception as e:
        print(f"Error computing race statistics: {e}")
        return {}