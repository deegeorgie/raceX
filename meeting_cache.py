"""
Meeting Cache Manager for zone-turf.fr programmes.
Provides persistent caching of meeting data to avoid re-downloading on every app launch.
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta, date


def get_cache_dir():
    """Get the cache directory path. Creates it if it doesn't exist."""
    if os.name == 'nt':  # Windows
        cache_dir = Path(os.getenv('APPDATA')) / 'zone_turf' / 'cache'
    else:  # Linux/Mac
        cache_dir = Path.home() / '.cache' / 'zone_turf'
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_cache_file():
    """Get the path to the meetings cache file."""
    return get_cache_dir() / 'meetings_cache.json'


def get_cache_date_key(date_input):
    """Convert date/datetime/str to cache key format (YYYY-MM-DD)."""
    if isinstance(date_input, datetime):
        return date_input.date().isoformat()
    if isinstance(date_input, date):
        return date_input.isoformat()
    return str(date_input)


def load_cache():
    """Load meetings cache from disk."""
    cache_file = get_cache_file()
    
    if not cache_file.exists():
        return {}
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        print(f"[INFO] Cache loaded from {cache_file}")
        return cache
    except Exception as e:
        print(f"[WARNING] Failed to load cache: {e}")
        return {}


def save_cache(cache_data):
    """Save meetings cache to disk."""
    cache_file = get_cache_file()
    
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        print(f"[INFO] Cache saved to {cache_file}")
        return True
    except Exception as e:
        print(f"[WARNING] Failed to save cache: {e}")
        return False


def get_cached_meetings(date_qdate):
    """
    Get cached meetings for a specific date.
    Returns (meetings_dict, is_valid) tuple.
    - meetings_dict: dict of meetings or None if not cached
    - is_valid: bool indicating if the cache entry is still valid
    """
    cache = load_cache()
    date_key = get_cache_date_key(date_qdate)
    
    if date_key not in cache:
        print(f"[DEBUG] No cache entry for date {date_key}")
        return None, False
    
    entry = cache[date_key]
    
    # Check if cache entry has timestamp and is still valid (within 24 hours)
    if 'timestamp' in entry:
        try:
            cached_time = datetime.fromisoformat(entry['timestamp'])
            age = datetime.now() - cached_time
            
            # Cache valid if less than 24 hours old
            if age < timedelta(hours=24):
                meetings = entry.get('meetings', {})
                print(f"[INFO] Using cached meetings for {date_key} (age: {age.total_seconds() / 3600:.1f} hours)")
                return meetings, True
            else:
                print(f"[DEBUG] Cache entry for {date_key} is stale ({age.total_seconds() / 3600:.1f} hours old)")
                return None, False
        except Exception as e:
            print(f"[WARNING] Could not validate cache timestamp: {e}")
            return None, False
    
    # If no timestamp, use cached data anyway
    meetings = entry.get('meetings', {})
    print(f"[INFO] Using cached meetings for {date_key} (no timestamp)")
    return meetings, True


def cache_meetings(date_qdate, meetings):
    """
    Cache meetings for a specific date with timestamp.
    """
    cache = load_cache()
    date_key = get_cache_date_key(date_qdate)
    
    cache[date_key] = {
        'timestamp': datetime.now().isoformat(),
        'meetings': meetings
    }
    
    save_cache(cache)
    print(f"[INFO] Cached {len(meetings)} meetings for {date_key}")


def clear_cache(date_qdate=None):
    """
    Clear cache entries.
    If date_qdate is provided, only clear that date's entry.
    Otherwise, clear entire cache.
    """
    cache = load_cache()
    
    if date_qdate:
        date_key = get_cache_date_key(date_qdate)
        if date_key in cache:
            del cache[date_key]
            save_cache(cache)
            print(f"[INFO] Cleared cache for {date_key}")
    else:
        # Clear entire cache
        cache_file = get_cache_file()
        try:
            if cache_file.exists():
                cache_file.unlink()
            print("[INFO] Entire cache cleared")
        except Exception as e:
            print(f"[WARNING] Failed to clear cache: {e}")


def get_cache_info():
    """
    Get information about cached dates and their ages.
    Returns a dict with dates as keys and info dicts as values.
    """
    cache = load_cache()
    info = {}
    
    for date_key, entry in cache.items():
        if 'timestamp' in entry:
            try:
                cached_time = datetime.fromisoformat(entry['timestamp'])
                age = datetime.now() - cached_time
                age_hours = age.total_seconds() / 3600
                is_stale = age >= timedelta(hours=24)
                num_meetings = len(entry.get('meetings', {}))
                
                info[date_key] = {
                    'timestamp': entry['timestamp'],
                    'age_hours': age_hours,
                    'is_stale': is_stale,
                    'num_meetings': num_meetings
                }
            except Exception as e:
                print(f"[WARNING] Could not parse entry for {date_key}: {e}")
        else:
            info[date_key] = {
                'timestamp': None,
                'age_hours': None,
                'is_stale': False,
                'num_meetings': len(entry.get('meetings', {}))
            }
    
    return info
