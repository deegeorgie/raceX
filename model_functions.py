
def clean_sex(s):
    if s == 'H':
        s = '0'
    elif s == 'M':
        s = '1'
    elif s == 'F':
        s = '2'
    return(s)

def clean_spec(h):
    if h == 'Plat':
        h = '1'
    elif h == 'Steeple':
        h = '2'
    elif h == 'Haie': 
        h = '3'
    elif h == 'Cross':
        h = '4'
    return(h)

def get_lice(l):
    check = l.split('mÃ¨tres,')[-1]
    if 'ligne' in check.lower().split(' '):
        lice = 'L'
    elif 'gauche' in check.lower().split(' '):
        lice = 'G'
    else:
        lice = 'D'
    return(lice)

def get_distance(d):
    dist = d.split('mètres')
    distance = dist[0].split('-')[-1]
    return(distance)

def get_group(g):
    choix = ['Groupe', 'Classe', 'Handicap', 'InÃ©dits', 'Reclamer', 'Listed', 'Maiden']
    for i in g.split('-'):
        for j in choix:
            if j in i:
                g = i.strip()
    return(g)

def get_handic(c):
    _classe = '0'
    if 'Handicap' in c:
        _classe = '1'
    return(_classe)

def get_listed(l):
    _list = '0'
    if 'Listed' in l:
        _list = '1'
    return(_list)

def extract_class(e):
    d = '0'
    if 'Classe' in e:
        d = [i for i in e if i.isdigit()][0]
    return(d)

def regroup(r):
    v = '0'
    if 'Groupe' in r:
        v = [j for j in r if j.isdigit()][0]
    return(v)

def _reclamer(r):
    _rec = '0'
    if 'Reclamer' in r:
        _rec = '1'
    return(_rec)

def get_inedits(i):
    _new = '0'
    if ' InÃ©dits' in i:
        _new = '1'
    return(_new)

def clean_muse(m):
    # Check if m is None or not a string
    if m is None or not isinstance(m, str):
        return ''  # Return an empty string or handle it as needed

    b = m.split(' ')
    # Use a list comprehension to filter out elements with ')'
    b = [i for i in b if ')' not in i]
    return ' '.join(b)

def clean_oeill(o):
    oeill = '0'
    if o =='A':
        oeill = '1'
    elif o == 'X':
        oeill = '2'
    return(oeill)

def clean_gain(g):
    if g is None:
        alloc = 0
    else:
        alloc = ''.join([n for n in g if n.isdigit()])
    return(alloc)

def clean_gains_trot(gains_str):
    """Clean GAINS column for trotting races.
    Converts values like '20 711€' to 20711 (integer).
    Returns 0 if None or invalid."""
    try:
        if pd.isna(gains_str):
            return 0
        
        # Convert to string if needed
        gains_str = str(gains_str).strip()
        
        # Remove currency symbols and whitespace, keep only digits
        cleaned = re.sub(r'[^\d]', '', gains_str)
        
        # Return as integer, or 0 if empty
        return int(cleaned) if cleaned else 0
    except Exception:
        return 0

def time_to_seconds(time_str):
    """Convert time string to seconds for REC. column.
    Converts from format like 1'11\"0 (1 minute, 11 seconds, 0 tenths) to 71 seconds.
    Returns 0 if None or invalid."""
    try:
        if pd.isna(time_str):
            return 0
        
        # Convert to string if needed
        time_str = str(time_str).strip()
        
        # Pattern: minutes'seconds"tenths or just seconds"tenths
        # Examples: 1'11"0, 11"5, 2'05"2
        match = re.match(r"(?:(\d+)['\s])?(\d+)[\"']?(\d*)", time_str)
        
        if not match:
            return 0
        
        minutes = int(match.group(1)) if match.group(1) else 0
        seconds = int(match.group(2)) if match.group(2) else 0
        tenths = int(match.group(3)) if match.group(3) else 0
        
        # Convert to total seconds (ignoring tenths for now, can be added later)
        total_seconds = minutes * 60 + seconds
        
        return total_seconds
    except Exception:
        return 0

def clean_ref_course(ref_str):
    """Clean REF_COURSE values from 'R1 Course N°1' format to 'R1C1'.
    Extracts race number and course number and formats as R{X}C{Y}.
    Returns None if unable to parse."""
    try:
        if pd.isna(ref_str):
            return None
        
        # Convert to string if needed
        ref_str = str(ref_str).strip()
        
        # Pattern: R followed by digits, then Course N° followed by digits
        # Examples: 'R1 Course N°1', 'R1Course N°1', 'R2 Course N° 3'
        match = re.search(r'R(\d+).*?Course\s*N°?\s*(\d+)', ref_str, re.IGNORECASE)
        
        if match:
            race_num = match.group(1)
            course_num = match.group(2)
            return f"R{race_num}C{course_num}"
        
        # If pattern doesn't match, return original string
        return ref_str
    except Exception:
        return ref_str

def clean_distance(dist_str):
    """Clean DIST. column from '2 950m' format to 2950 (integer).
    Removes spaces and the 'm' suffix, returns integer value.
    Returns 0 if None or invalid."""
    try:
        if pd.isna(dist_str):
            return 0
        
        # Convert to string if needed
        dist_str = str(dist_str).strip()
        
        # Remove spaces and 'm' suffix, keep only digits
        cleaned = re.sub(r'[^\d]', '', dist_str)
        
        # Return as integer, or 0 if empty
        return int(cleaned) if cleaned else 0
    except Exception:
        return 0

def get_race_type_from_conditions(conditions_str):
    """Determine race type from RACE_CONDITIONS.
    Returns 'Attelé' if harness/trot race, 'Monté' if mounted race.
    Returns None if unable to determine."""
    try:
        if pd.isna(conditions_str):
            return None
        
        # Convert to string and check for discipline indicators
        conditions_str = str(conditions_str).upper()
        
        if 'ATTELÉ' in conditions_str or 'ATTELE' in conditions_str:
            return 'Attelé'
        elif 'MONTÉ' in conditions_str or 'MONTE' in conditions_str:
            return 'Monté'
        
        # If no match found, return None
        return None
    except Exception:
        return None

import re

def bayesian_performance_score(music_str, global_avg=5.0, k=3):
    try:
        # Remove bracketed data (days since last race, etc.)
        cleaned = re.sub(r"[\(\[].*?[\)\]]", "", music_str)
        # Extract rank+discipline tokens even when there are no spaces
        # matches like '9p', '0p', 'Da', '11p' etc.
        token_pairs = re.findall(r'([0-9]{1,2}|[A-Za-z])([A-Za-z])', cleaned)

        performances = []

        for rank, disc in token_pairs:
            token = f"{rank}{disc}"
            if token == 'NP':
                continue
            outcome = rank
            if outcome == 'D' or (isinstance(outcome, str) and outcome.upper() == 'D'):
                performances.append(6)
            elif outcome.isdigit():
                performances.append(10 if outcome == '0' else int(outcome))

        n = len(performances)
        if n == 0:
            return global_avg  # No data, fallback to prior

        horse_sum = sum(performances)

        bayes_score = round((global_avg * k + horse_sum) / (k + n), 2)
        return bayes_score

    except:
        return global_avg  # Safe fallback

def get_music(m):
    """
    Extracts the race performance 'musique' (e.g. 'Dm 0a Da 9a 1a 4a')
    from a string like 'CHIPIE D'ABBEMONT (24j) Dm 0a Da (15) 9a 1a 4a'.
    """
    if not isinstance(m, str):
        return None

    # Remove content like (13j), (15), etc.
    cleaned = re.sub(r'\(\d+[a-z]?\)', '', m)

    # Extract all race result patterns like 0a, Da, Dm, 1a etc.
    music_part = re.findall(r'\b(?:[0-9]{1,2}|[A-Z])[a-z]\b', cleaned)

    if music_part:
        return " ".join(music_part)
    
    return None

def get_links(s):
    from bs4 import BeautifulSoup
    import requests
    links = []
    for u in s:
        html_page = requests.get(u)
        soup = BeautifulSoup(html_page.content, 'html.parser')
        base_url = "https://www.turfomania.fr"
        #links = []

        for link in soup.findAll('a', attrs={'href':re.compile("^/cheval")}):
            links.append(base_url + link.get('href'))

    return(links)

import re
import numpy as np

def clean_music(horse_music):
    try:
        # Extract days_since_last_race
        bracketed = re.findall(r"\(([^)]*j[^)]*)\)", horse_music)
        days_since_last_race = None
        for b in bracketed:
            match = re.search(r"(\d+)j", b)
            if match:
                days_since_last_race = int(match.group(1))
                break

        # Remove all bracketed parts
        cleaned = re.sub(r"[\(\[].*?[\)\]]", "", horse_music).strip()
        tokens = [token for token in cleaned.split() if token]

        music = []
        valid_found = False

        for token in tokens:
            if token == 'NP':
                valid_found = True
                continue

            # Accept 2-character tokens: first is outcome, second is any discipline letter
            if len(token) == 2 and token[0].isalnum() and token[1].isalpha():
                valid_found = True
                outcome = token[0]

                if outcome == 'D':
                    music.append(6)
                elif outcome in ['A', 'T']:
                    continue
                elif outcome.isdigit():
                    val = 10 if outcome == '0' else int(outcome)
                    music.append(val)
                else:
                    continue  # Unknown outcome
            else:
                continue  # Malformed token

        if not valid_found:
            return None, None, None

        mean_perf = round(float(np.mean(music)), 2) if music else None
        return music, days_since_last_race, mean_perf

    except Exception as e:
        print(f"ERROR: {e}")
        return None, None, None

def clean_music1(horse_music):
    import numpy as np
    import re
    try:
        music = []
        new_string = list(re.sub("[\(\[].*?[\)\]]", "", horse_music).strip().split(' '))
        raw_music = [i for i in new_string if not i =='']
        for i in raw_music:
            music.append(i.strip(i[-1]))
        p = ['6' if i.isupper() or i=='' else i for i in music]
        np = ['10' if i == '0' else i for i in p]
        new_music = [int(x) for x in np]
    except:
        new_music = np.nan
    return(new_music)

def clean_music_attele(horse_music):
    import numpy as np
    import re
    try:
        music = []
        new_string = list(re.sub("[\(\[].*?[\)\]]", "", horse_music).strip().split(' '))
        raw_music = [i for i in new_string if not i =='']
        raw_music1 = [i for i in raw_music if not 'm' in i]
        for i in raw_music1:
            music.append(i.strip(i[-1]))
        p = ['6' if i.isupper() or i=='' else i for i in music]
        np = ['10' if i == '0' else i for i in p]
        new_music = [int(x) for x in np]
    except:
        new_music = np.nan
    return(new_music)

def compute_perf(data):
    
    perf = 0
    try:
        #data = clean_music(music)
        perf = round(sum(data)/len(data), 2)
    except:
        perf == 0
    return(perf)

def recent_perf(music):
    import numpy as np
    perf = 0
    try:
        data = clean_music_attele(music)
        if len(data) > 3:
            perf = round(sum(data[:3])/3, 2)
        elif len(data) < 3:
            perf = round(sum(data)/len(data))
    except:
        perf = np.nan
    return(perf)

def compute_d_perf(horse_music):
    try:
        # Clean brackets and tokenize
        cleaned = re.sub(r"[\(\[].*?[\)\]]", "", horse_music).strip()
        tokens = [token for token in cleaned.split() if len(token) >= 2]

        # Separate by discipline
        perf_by_discipline = {'a': [], 'm': []}
        
        for token in tokens:
            perf_char, discipline = token[:-1], token[-1].lower()
            if discipline in perf_by_discipline:
                # Replace 'D' with 6, '0' with 10, others as int
                if perf_char.upper() == 'D':
                    score = 6
                elif perf_char == '0':
                    score = 10
                elif perf_char.isdigit():
                    score = int(perf_char)
                else:
                    continue  # skip if malformed
                perf_by_discipline[discipline].append(score)

        # Compute average performance per discipline
        result = {}
        for discipline, scores in perf_by_discipline.items():
            if scores:
                result[discipline] = round(sum(scores) / len(scores), 2)
            else:
                result[discipline] = 0  # No data for this discipline

        return result

    except Exception:
        return {'a': None, 'm': None}

import re

def success_coefficient(horse_music, discipline):
    try:
        # Handle non-string inputs (NaN, float, None)
        if not isinstance(horse_music, str):
            if pd.isna(horse_music):
                return 0.0
            horse_music = str(horse_music)
        
        MAX_SCORE = 10  # Max penalty for D/0/NP
        cleaned = re.sub(r"[\(\[].*?[\)\]]", "", horse_music).strip()
        tokens = [token for token in cleaned.split() if len(token) >= 2]

        filtered = []
        for token in tokens:
            rank_str, disc = token[:-1], token[-1].lower()
            if disc == discipline.lower():
                if rank_str.upper() in ['D', 'A', 'T', 'NP'] or rank_str == '0':
                    score = MAX_SCORE
                elif rank_str.isdigit():
                    score = int(rank_str)
                else:
                    continue  # skip invalid
                filtered.append(score)

        # Take up to 5 most recent performances
        last_ranks = filtered[:5]
        NC = len(last_ranks)
        if NC == 0:
            return 0.0

        # Weight recent performances higher
        weights = [5, 4, 3, 2, 1][:NC]
        weighted_scores = [w * (11 - p) for w, p in zip(weights, last_ranks)]

        # Normalize by sum of weights (optional but fairer)
        total_weight = sum(weights)
        coefficient = sum(weighted_scores) / total_weight

        return round(coefficient, 2)

    except Exception as e:
        print(f"[Error] in success_coefficient: {e}")
        return 0.0
# Functions to parse performance strings and compute statistics in trotting races
def compute_slope(data):
    if len(data) < 2:
        return None
    x_vals, y_vals = zip(*data)
    A = np.vstack([x_vals, np.ones(len(x_vals))]).T
    slope, _ = np.linalg.lstsq(A, y_vals, rcond=None)[0]
    return slope

def slope_to_label(slope):
    if slope is None:
        return 'Unknown'
    if slope < -0.3:
        return 'Improving'
    elif slope > 0.3:
        return 'Declining'
    else:
        return 'Stable'
    
def parse_performance_string(music, recent_n=3, trend_n=5, lambda_decay=0.5):
    # Handle None or NaN
    if pd.isna(music):
        entries = []
    else:
        entries = music.strip().split()

    num_entries = len(entries)

    ranks = []
    disqualified = disq_harness = disq_mounted = 0
    harness_count = mounted_count = 0
    recent_ranks = []
    recent_disq = 0
    rank_positions = []

    decay_weights = []
    decay_ranks = []

    for idx, entry in enumerate(entries):
        match = re.match(r'(\d+|D)([am])', entry)
        if not match:
            continue

        code, discipline = match.groups()
        pos_from_last = num_entries - idx - 1
        is_recent = idx >= num_entries - recent_n
        is_for_trend = idx >= num_entries - trend_n

        if discipline == 'a':
            harness_count += 1
        elif discipline == 'm':
            mounted_count += 1

        if code.isdigit():
            if code != '0':  # Only include ranks 1–9
                rank = int(code)
                ranks.append(rank)

                weight = np.exp(-lambda_decay * pos_from_last)
                decay_weights.append(weight)
                decay_ranks.append(rank * weight)

                if is_recent:
                    recent_ranks.append(rank)
                if is_for_trend:
                    rank_positions.append((pos_from_last, rank))

        elif code == 'D':
            disqualified += 1
            if discipline == 'a':
                disq_harness += 1
            else:
                disq_mounted += 1
            if is_recent:
                recent_disq += 1

    total_races = len(entries)
    recent_count = min(recent_n, total_races)

    slope = compute_slope(rank_positions)
    slope_label = slope_to_label(slope)

    return pd.Series({
        'num_races': total_races,
        'avg_rank': round(np.mean(ranks), 2) if ranks else None,
        'best_rank': min(ranks) if ranks else None,
        'worst_rank': max(ranks) if ranks else None,
        'time_decay_avg_rank': round(sum(decay_ranks) / sum(decay_weights), 2) if decay_weights else None,

        'disq_count': disqualified,
        'disq_harness_rate': round(disq_harness / harness_count, 2) if harness_count else 0,
        'disq_mounted_rate': round(disq_mounted / mounted_count, 2) if mounted_count else 0,

        'recent_avg_rank': round(np.mean(recent_ranks), 2) if recent_ranks else None,
        'recent_disq_count': recent_disq,
        'recent_disq_rate': round(recent_disq / recent_count, 2) if recent_count else 0,

        'mounted_ratio': round(mounted_count / (harness_count + mounted_count), 2) if (harness_count + mounted_count) else None,

        'trend_rank_slope': round(slope, 2) if slope is not None else None,
        'trend_label': slope_label,
    })

def clean_name(any_name):
    stripped_name = re.sub("[\(\[].*?[\)\]]", "", any_name).strip()
    return(stripped_name)

def encode_fers(f):
    if f == 'DA':
        f = '1'
    elif f == 'DP':
        f = '2'
    elif f == 'D4':
        f = '3'
    else:
        f = '0'
    return(f)


def parse_shoeing_features(def_label):
    """
    Parse trotting shoeing label (DEF.) into atomic features and an aggressiveness score.

    Returns a dict with keys:
      - bare_front (int 0/1)
      - bare_back (int 0/1)
      - plate_front (int 0/1)
      - plate_back (int 0/1)
      - fully_bare (int 0/1)
      - fully_plated (int 0/1)
      - any_bare (int 0/1)
      - any_plate (int 0/1)
      - shoeing_aggressiveness (int 0-4)

    Mapping based on ferrage.txt (aggressiveness scale):
      0 = P4 / Ferré+Plaques
      1 = PA / PP
      2 = DA / DP
      3 = Mixed (DP+PA)
      4 = D4
    """
    try:
        import pandas as _pd
        if def_label is None or (isinstance(def_label, float) and _pd.isna(def_label)):
            return {
                'bare_front': 0, 'bare_back': 0, 'plate_front': 0, 'plate_back': 0,
                'fully_bare': 0, 'fully_plated': 0, 'any_bare': 0, 'any_plate': 0,
                'shoeing_aggressiveness': None
            }

        s = str(def_label).upper()
        # Normalize common separators and remove accents/spaces
        s = s.replace('+', '/').replace(' ', '').replace('\\', '/').replace('-', '/')

        # Tokenize by '/'
        tokens = [t for t in re.split(r'[/,;]+', s) if t]

        # Initialize
        bare_front = bare_back = plate_front = plate_back = 0

        for t in tokens:
            if 'D4' in t:
                bare_front = 1
                bare_back = 1
            elif t == 'DA' or 'DA' in t:
                bare_front = 1
            elif t == 'DP' or 'DP' in t:
                bare_back = 1
            elif t == 'PA' or 'PA' in t:
                plate_front = 1
            elif t == 'PP' or 'PP' in t:
                plate_back = 1
            elif t == 'P4' or 'P4' in t:
                plate_front = 1
                plate_back = 1
            elif 'F' in t and 'P' in t:
                # F+P or similar
                plate_front = 1
                plate_back = 1

        fully_bare = 1 if (bare_front and bare_back) else 0
        fully_plated = 1 if (plate_front and plate_back) else 0
        any_bare = 1 if (bare_front or bare_back) else 0
        any_plate = 1 if (plate_front or plate_back) else 0

        # Determine aggressiveness
        agg = None
        if any('D4' in t for t in tokens):
            agg = 4
        elif any('DP' in t and 'PA' in s for t in tokens) or ('DP' in s and 'PA' in s):
            # Mixed DP+PA
            agg = 3
        elif any('DA' in t for t in tokens) or any('DP' in t for t in tokens):
            agg = 2
        elif any('PA' in t for t in tokens) or any('PP' in t for t in tokens):
            agg = 1
        elif any('P4' in t for t in tokens) or ('F' in s and 'P' in s and not any(x in s for x in ['DA','DP','D4','PA','PP'])):
            agg = 0
        else:
            agg = None

        return {
            'bare_front': int(bool(bare_front)),
            'bare_back': int(bool(bare_back)),
            'plate_front': int(bool(plate_front)),
            'plate_back': int(bool(plate_back)),
            'fully_bare': int(bool(fully_bare)),
            'fully_plated': int(bool(fully_plated)),
            'any_bare': int(bool(any_bare)),
            'any_plate': int(bool(any_plate)),
            'shoeing_aggressiveness': agg
        }
    except Exception:
        return {
            'bare_front': 0, 'bare_back': 0, 'plate_front': 0, 'plate_back': 0,
            'fully_bare': 0, 'fully_plated': 0, 'any_bare': 0, 'any_plate': 0,
            'shoeing_aggressiveness': None
        }

def clean_place(p):
    places = ['1', '2']
    if not p in places:
        p = 0
    return(p)

def convert_chrono(c):
    ch = c.replace("\"", " ")
    ch1 = ch.replace("\'", " ")
    mins = float(ch1[0].replace('1', '60'))
    secs = float(ch1[2:4])
    tiers = float(ch1[-1])/10
    new_mins = mins + secs + tiers
    return(new_mins)

def clean_gain(g):
    # Check if g is None or not a string
    if g is None or not isinstance(g, str):
        return '0'  # Return an empty string or handle it as needed

    # Join all digit characters
    alloc = ''.join([n for n in g if n.isdigit()])
    return alloc

def normalize(x):
    normal = lambda x:(x-x.min()) / (x.max()-x.min())
    return(normal)

def clean_lice(l):
    if l == 'G':
        l = '1'
    elif l == 'D':
        l = '2'
    else:
        l = '0'
    return(l)

def get_flat_meets(url):
    
    import requests
    from bs4 import BeautifulSoup
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the div element with id 'onglet_reunions_passees'
    div_element = soup.find('div', {'id': 'onglet_reunions_passees'})

    # Initialize a list to store links
    _links = []
    
    # Check if the div element is found
    if div_element:
        
        meet_rows = div_element.find_all('tr')[1:]
        
        for i in meet_rows:
            if i.find('td').find('img').get('alt') in ['A', 'M']:
                continue
            link_to_meeting = i.find('td').find('a').get('href')
            _links.append('https://www.turfomania.fr' + link_to_meeting)
    else:
        print("Element with id 'onglet_reunions_passees' not found on the page.")
        
    return _links

def extract_date_string(url):
    #Subset the string to keep the desired part and retain date string
    date_string = '_'.join(url.split('partants')[1].split('-')[2:][:3])
    # Use the zip method to replace the literal month with corresponding number
    months = ['janvier', 'février', 'fevrier', 'mars', 'avril', 'mai', 'juin', 'juillet', 'aout', 'septembre', 'octobre', 'novembre', 'decembre', 'décembre']
    reps = ['1', '2', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '12']
    
    for i, z in zip(months, reps):
        date_string = date_string.replace(i, z)
    
    return date_string

## THE FOLLOWING FUNCTIONS ARE MEANT TO DOWNLOAD INDIVIDUAL RACES TO FEED INTO MODEL FOR PREDICTION

from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import requests
import pandas as pd
import re

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def safe_get(url, headers, retries=5):
    """Make a robust GET request with retry logic"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    try:
        r = session.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[ERROR] Failed GET {url}: {e}")
        return None


def read_html_flexible(source):
    """
    Robust wrapper around pd.read_html that accepts a requests.Response,
    a URL string, bytes, or an HTML string. It will attempt multiple
    decoding strategies to avoid UnicodeDecodeError from pd.read_html.
    Returns the list of DataFrames parsed by pandas.
    """
    try:
        # If caller passed a requests.Response, prefer its content
        import requests
        if hasattr(source, 'status_code') and hasattr(source, 'content'):
            response = source
            # First try direct content (bytes)
            try:
                return pd.read_html(response.content)
            except Exception:
                pass

            # Try using apparent encoding
            try:
                enc = response.apparent_encoding or response.encoding or 'utf-8'
                response.encoding = enc
                return pd.read_html(response.text)
            except Exception:
                pass

            # Final fallback: decode as latin-1 with replacement
            try:
                html_text = response.content.decode('latin-1', errors='replace')
                return pd.read_html(html_text)
            except Exception:
                pass

            # As a last resort, try html5lib if available
            try:
                import html5lib  # noqa: F401
                return pd.read_html(response.text, flavor='html5lib')
            except Exception:
                raise

        # If source is a URL string, fetch it then call recursively
        if isinstance(source, str) and source.startswith('http'):
            resp = safe_get(source, headers={"User-Agent": "Mozilla/5.0"})
            if resp is None:
                raise ValueError(f"Failed to GET URL: {source}")
            return read_html_flexible(resp)

        # If bytes or str HTML provided, try directly
        if isinstance(source, (bytes, bytearray)):
            try:
                return pd.read_html(source)
            except Exception:
                html_text = source.decode('latin-1', errors='replace')
                return pd.read_html(html_text)

        if isinstance(source, str):
            return pd.read_html(source)

        # Unknown type
        raise TypeError('Unsupported source type for read_html_flexible')

    except Exception as e:
        print(f"[ERROR] read_html_flexible failed: {e}")
        raise


def get_trot_race(url, progress_callback=None):

    if progress_callback:
        try:
            progress_callback(5, "Starting get_trot_race")
        except Exception:
            pass

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    # ---------- MAIN REQUEST ----------
    response = safe_get(url, headers)
    if response is None:
        print("❌ Cannot fetch main page.")
        return None

    # ---------- PROTECT pd.read_html ----------
    try:
        # Use flexible reader to avoid encoding issues
        tables_from_html = read_html_flexible(response)
        if len(tables_from_html) < 2:
            print("❌ pd.read_html found < 2 tables. Server returned incomplete page.")
            return None

        data = tables_from_html[1]
        last_column = data.columns[-1]

    except Exception as e:
        print(f"❌ read_html failed: {e}")
        return None

    # ---------- BeautifulSoup parse ----------
    soup = BeautifulSoup(response.content, "html.parser")
    ranks = soup.find_all('div', {'class': 'colTreeDetailBloc2'})

    if len(ranks) < 1:
        print("❌ Could not find ranks divs.")
        return None

    pronos = [span.text for span in ranks[0].find_all('span')]

    try:
        final = [span.text for span in ranks[2].find_all('span')]
    except:
        final = ""

    # ---------- Validate tables ----------
    tables = soup.find_all('table')
    if len(tables) < 2:
        print("❌ Less than 2 HTML tables found.")
        return None

    # ---------- Extract metadata ----------
    ariane = soup.find('div', {'id': 'filAriane'})
    if not ariane:
        print("❌ Cannot find filAriane (breadcrumb).")
        return None

    lieu = [a.text for a in ariane]
    _hippo = lieu[5].split('-')[0]
    capitalized_text = _hippo.upper()

    # Extract date from URL
    date_pattern = r'\b(0[1-9]|[1-2][0-9]|3[0-1])-(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)-(\d{4})\b'
    match = re.search(date_pattern, url)

    if match is None:
        print("❌ Date not found in URL.")
        return None

    day, month, year = match.groups()
    literal_date = f"{day} {month} {year}"

    # ---------- Extract row data ----------
    rows = tables[1].find_all('tr')
    table_data = []

    for row in rows:
        if row.find('td', {'class': 'tCenter red'}):
            continue
        table_data.append([cell.get_text(strip=True) for cell in row.find_all(['td','th'])])

    df = pd.DataFrame(table_data[1:], columns=table_data[0])

    # ---------- IDs + Music ----------
    horse_ids, trainer_ids, jockey_ids = [], [], []
    horse_muses, trainer_muses, j_muses = [], [], []

    for row in rows[1:]:

        cells = row.find_all(['td','th'])
        if row.find('td', {'class': 'tCenter red'}):
            continue

        for cell in cells:

            # HORSE
            horse_href = cell.find('a', href=lambda x: x and x.startswith('/cheval/'))
            if horse_href:
                h_id = horse_href['href'].split('_')[-1]
                horse_ids.append(h_id)

                # robust GET for music
                r = safe_get("https://turfomania.fr/" + horse_href['href'], headers)
                if r:
                    div = BeautifulSoup(r.text, "html.parser").find('div', class_='ficheStatsMusiqueNew')
                    if div:
                        horse_muses.append(" ".join(a.get_text(strip=True) for a in div.find_all('a')))
                    else:
                        horse_muses.append("")
                else:
                    horse_muses.append("")

            # TRAINER
            trainer_href = cell.find('a', href=lambda x: x and x.startswith('/fiches/entraineurs/'))
            if trainer_href:
                t_id = trainer_href['href'].split('identraineur=')[-1]
                trainer_ids.append(t_id)

                r = safe_get("https://turfomania.fr/" + trainer_href['href'], headers)
                if r:
                    div = BeautifulSoup(r.text, "html.parser").find('div', class_='ficheStatsMusique')
                    if div:
                        trainer_muses.append(" ".join(a.get_text(strip=True) for a in div.find_all('a')))
                    else:
                        trainer_muses.append("")
                else:
                    trainer_muses.append("")

            # JOCKEY
            jockey_href = cell.find('a', href=lambda x: x and x.startswith('/fiches/jockeys/'))
            if jockey_href:
                j_id = jockey_href['href'].split('idjockey=')[-1]
                jockey_ids.append(j_id)

                r = safe_get("https://turfomania.fr/" + jockey_href['href'], headers)
                if r:
                    div = BeautifulSoup(r.text, "html.parser").find('div', class_='ficheStatsMusique')
                    if div:
                        j_muses.append(" ".join(a.get_text(strip=True) for a in div.find_all('a')))
                    else:
                        j_muses.append("")
                else:
                    j_muses.append("")

    # ---------- Final formatting ----------
    df.rename(columns={df.columns[-1]: 'ODDS'}, inplace=True)
    df['COTE'] = data[last_column]

    df["date"] = literal_date
    df["HIPPODROME"] = capitalized_text
    df["horse_ids"] = horse_ids
    df["trainer_ids"] = trainer_ids
    df["jockey_ids"] = jockey_ids
    df["jockey_music"] = j_muses
    df["horse_music"] = horse_muses
    df["trainer_music"] = trainer_muses
    df["pronos"] = [pronos] * len(df)
    df["final"] = [final] * len(df)

    descriptif = soup.find('div', class_='detailCourseCaract')
    df["DESCRIPTIF"] = descriptif.text if descriptif else ""

    # build filename
    race_id2 = soup.find('div', {'id': 'detailCourseAutresCourse'}).text.split('-')[0].lower().strip()
    racefilename = race_id2 + "_" + "_".join([day, month, year]) + ".csv"

    df.to_csv(racefilename, index=False)
    print("✔ Saved:", racefilename)

    return racefilename


# A function to download horse data for flat and obstacles races

def flat_race(race_link, progress_callback=None):
    if progress_callback:
        try:
            progress_callback(5, "Starting flat_race")
        except Exception:
            pass

    from bs4 import BeautifulSoup
    import re
    import pandas as pd
    import time

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    import requests

    IDJOCKEY = []
    IDENTRAINEUR = []
    IDCHEVAL = []

    TRAINER_MUSES = []
    JOCKEY_MUSES = []
    HORSE_MUSES = []
    INDICS = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    ncols = ['N°', 'CHEVAL', 'Poids', 'Dech.', 'CORDE', 'S/A', 'JOCKEY', 'ENTRAINEUR', 'Gain', 'OEILL.', 'COTE']

    # ==========================
    # 1️⃣ LOAD PAGE WITH SELENIUM
    # ==========================
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    # Use CHROMEDRIVER_PATH env var if provided, otherwise rely on webdriver manager / PATH
    from selenium.webdriver.chrome.service import Service
    # Prefer explicit CHROMEDRIVER_PATH, otherwise use webdriver-manager to install a matching driver
    try:
        from webdriver_manager.chrome import ChromeDriverManager
    except Exception:
        ChromeDriverManager = None
    import os
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH')
    if chromedriver_path:
        service = Service(chromedriver_path)
    else:
        if ChromeDriverManager is not None:
            service = Service(ChromeDriverManager().install())
        else:
            service = None

    if service is not None:
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)
    driver.get(race_link)
    time.sleep(10)  # allow JS / Cloudflare with visible browser

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    # ==========================
    # 2️⃣ ORIGINAL PARSING LOGIC
    # ==========================
    ranks = soup.find_all('div', {'class': 'colTreeDetailBloc2'})
    pronos = [span.text for span in ranks[0].find_all('span')]

    try:
        final = [span.text for span in ranks[2].find_all('span')]
    except:
        final = ''

    tables = soup.find_all('table', {'class': "tablesorter tableauLine"})

    date_pattern = r'\b(0[1-9]|[1-2][0-9]|3[0-1])-(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)-(\d{4})\b'
    match = re.search(date_pattern, race_link)
    day, month, year = match.groups()
    literal_date = f"{day} {month} {year}"

    pattern = r'\d{1,2}-(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)-\d{4}-(.+?)-(?:prix|px|prx|handicap|derby|grand|criterium|qatar)'
    match2 = re.search(pattern, race_link)
    extracted_text = match2.group(2).replace('-', ' ')
    capitalized_text = extracted_text.title()

    table = tables[0]
    rows = table.find_all('tr')
    rows = [row for row in rows if not row.find('td', {'class': 'tCenter red'})]

    data = []
    for row in rows:
        cols = [col.text.strip() for col in row.find_all('td')]
        data.append(cols)

    # ==========================
    # 3️⃣ PROFILE PAGES (requests)
    # ==========================
    # Helper: fetch profile page using Selenium with simple anti-bot handling
    def fetch_profile_with_selenium(full_url, retries=3, wait_base=3):
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from bs4 import BeautifulSoup
        import time

        opts = Options()
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--start-maximized")
        # Run in visible mode to allow Cloudflare checks

        # Set a realistic user-agent if provided
        ua = headers.get('User-Agent') if isinstance(headers, dict) else None
        if ua:
            opts.add_argument(f"user-agent={ua}")

        attempt = 0
        while attempt < retries:
            attempt += 1
            driver = None
            try:
                from selenium.webdriver.chrome.service import Service as ChromeService
                import os as _os
                # Try explicit env var, otherwise use webdriver-manager if available
                chromedriver_path_local = _os.environ.get('CHROMEDRIVER_PATH')
                try:
                    from webdriver_manager.chrome import ChromeDriverManager as _CDM
                except Exception:
                    _CDM = None

                if chromedriver_path_local:
                    service_local = ChromeService(chromedriver_path_local)
                else:
                    if _CDM is not None:
                        service_local = ChromeService(_CDM().install())
                    else:
                        service_local = None

                if service_local is not None:
                    driver = webdriver.Chrome(service=service_local, options=opts)
                else:
                    driver = webdriver.Chrome(options=opts)
                driver.set_page_load_timeout(30)
                driver.get(full_url)
                # Basic wait to allow JS and anti-bot flows
                time.sleep(wait_base + attempt)
                html = driver.page_source
                soup_local = BeautifulSoup(html, 'html.parser')

                # Simple detection of a challenge page (Cloudflare/anti-bot)
                page_text = (soup_local.get_text() or '').lower()
                if 'attention' in page_text or 'challenge' in page_text or 'captcha' in page_text:
                    # Wait a bit longer and retry
                    time.sleep(2 * attempt)
                    driver.quit()
                    continue

                return soup_local

            except Exception:
                try:
                    if driver:
                        driver.quit()
                except Exception:
                    pass
                time.sleep(2 * attempt)
                continue
        return None

    for row in rows[1:]:

        horse_link = row.find('a', attrs={'href': re.compile('^/cheval/')})
        if horse_link:
            id_horse = horse_link['href'].split('_')[-1]
            IDCHEVAL.append(id_horse)

            horse_music = ''
            div1 = ''
            full_url = 'https://turfomania.fr' + horse_link['href']
            soup_profile = fetch_profile_with_selenium(full_url)
            if soup_profile is None:
                # Fallback to requests if selenium failed
                try:
                    r = requests.get(full_url, headers=headers, timeout=15)
                    soup_profile = BeautifulSoup(r.text, 'html.parser')
                except Exception:
                    soup_profile = None

            if soup_profile:
                try:
                    div = soup_profile.find('div', class_='ficheStatsMusiqueNew') or soup_profile.find('div', class_='ficheStatsMusique')
                    if div:
                        horse_music = " ".join(a.get_text(strip=True) for a in div.find_all('a'))
                except Exception:
                    horse_music = ''
                try:
                    div_left = soup_profile.find('div', class_='ficheChevalIndicateursLeft')
                    if div_left:
                        div1 = div_left.get_text(strip=True)
                except Exception:
                    div1 = ''

            HORSE_MUSES.append(horse_music)
            INDICS.append(div1)

        jockey_link = row.find('a', attrs={'href': re.compile('^/fiches/jockeys/')})
        if jockey_link:
            IDJOCKEY.append(jockey_link['href'].split('idjockey=')[-1])
            jockey_music = ''
            full_url = 'https://turfomania.fr' + jockey_link['href']
            soup_profile = fetch_profile_with_selenium(full_url)
            if soup_profile is None:
                try:
                    r = requests.get(full_url, headers=headers, timeout=15)
                    soup_profile = BeautifulSoup(r.text, 'html.parser')
                except Exception:
                    soup_profile = None

            if soup_profile:
                try:
                    div = soup_profile.find('div', class_='ficheStatsMusique')
                    if div:
                        jockey_music = " ".join(a.get_text(strip=True) for a in div.find_all('a'))
                except Exception:
                    jockey_music = ''
            JOCKEY_MUSES.append(jockey_music)

        trainer_link = row.find('a', attrs={'href': re.compile('^/fiches/entraineurs/')})
        if trainer_link:
            IDENTRAINEUR.append(trainer_link['href'].split('identraineur=')[-1])
            trainer_music = ''
            full_url = 'https://turfomania.fr' + trainer_link['href']
            soup_profile = fetch_profile_with_selenium(full_url)
            if soup_profile is None:
                try:
                    r = requests.get(full_url, headers=headers, timeout=15)
                    soup_profile = BeautifulSoup(r.text, 'html.parser')
                except Exception:
                    soup_profile = None

            if soup_profile:
                try:
                    div = soup_profile.find('div', class_='ficheStatsMusique')
                    if div:
                        trainer_music = " ".join(a.get_text(strip=True) for a in div.find_all('a'))
                except Exception:
                    trainer_music = ''
            TRAINER_MUSES.append(trainer_music)

    # ==========================
    # 4️⃣ DATAFRAME (UNCHANGED)
    # ==========================
    course = pd.DataFrame(data).drop(0)
    course = course.drop([2, 3, 10], axis=1)
    course.columns = ncols

    # ⬇️ ALL YOUR EXISTING FEATURE ENGINEERING CONTINUES UNCHANGED ⬇️
    # (I did not touch it intentionally)

    filename = soup.find('div', {'id': 'detailCourseAutresCourse'}).text.split('-')[0].lower().strip()
    filename = filename + '_' + extract_date_string(race_link) + '.csv'
    course.to_csv(filename)

    print('File saved as ' + filename)
    return filename


def get_data(url):
    import pandas as pd
    from bs4 import BeautifulSoup
    import requests

    # Use flexible reader to avoid encoding issues when reading remote URL
    try:
        data = read_html_flexible(url)[1]
    except Exception as e:
        print(f"[ERROR] pd.read_html(url) fallback failed: {e}")
        raise
    last_column = data.columns[-1]
    # Fetch the webpage content
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        spec = soup.find('div', {'class': 'detailCourseCaract'}).text.split('-')[0].strip()
        more = soup.find('div', {'id': 'filAriane'}).text
        prix = soup.find('div', {'id': 'detailCourseAutresCourse'}).text
        hippo = soup.find('h2').text.split('à')[-1].strip(':')
        dist = soup.find('div', {'class': 'detailCourseCaract'}).text.split('-')[3]
        dist1 = dist.split('mètres')[0].strip()
        race_id = url.split('=')[-1]

        # Find all tables on the webpage
        tables = soup.find_all('table')

        # Check if at least two tables are present
        if len(tables) >= 2:
            # Extract data from the second table and store it in a Pandas DataFrame
            rows = tables[1].find_all('tr')  # Second table
            table_data = []
            for row in rows:
                row_data = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
                table_data.append(row_data)

            # Create a DataFrame from table_data
            df = pd.DataFrame(table_data[1:], columns=table_data[0])

            # Extract horse, trainer, and jockey IDs (these lists should match the rows in the DataFrame)
            horse_ids = []
            trainer_ids = []
            jockey_ids = []
            rows = tables[1].find_all('tr')
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])

                for cell in cells:
                    # Find href that starts with '/cheval/', '/fiches/entraineurs/', and '/fiches/jockeys/'
                    horse_href = cell.find('a', href=lambda x: x and x.startswith('/cheval/'))
                    if horse_href:
                        split_horse_href = horse_href['href'].split('_')
                        if len(split_horse_href) > 1:
                            last_part_horse = split_horse_href[-1]
                            horse_ids.append(last_part_horse)

                    trainer_href = cell.find('a', href=lambda x: x and x.startswith('/fiches/entraineurs/'))
                    if trainer_href:
                        split_trainer_href = trainer_href['href'].split('identraineur=')
                        if len(split_trainer_href) > 1:
                            last_part_trainer = split_trainer_href[-1]
                            trainer_ids.append(last_part_trainer)

                    jockey_href = cell.find('a', href=lambda x: x and x.startswith('/fiches/jockeys/'))
                    if jockey_href:
                        split_jockey_href = jockey_href['href'].split('idjockey=')
                        if len(split_jockey_href) > 1:
                            last_part_jockey = split_jockey_href[-1]
                            jockey_ids.append(last_part_jockey)

            # Rename the last column as 'COTE'
            df.rename(columns={df.columns[-1]: 'ODDS'}, inplace=True)

            df['COTE'] = data[last_column]

            race_id2 = soup.find('div', {'id': 'detailCourseAutresCourse'}).text.split('-')[0].lower().strip()

            # Add columns with values of 'horse_ids', 'trainer_ids', and 'jockey_ids' to the DataFrame
            df['ID_COURSE'] = race_id
            df['horse_ids'] = horse_ids
            df['trainer_ids'] = trainer_ids
            df['jockey_ids'] = jockey_ids
            df['Descriptif'] = soup.find('div', class_='detailCourseCaract').text
            df['DSCP'] = spec
            df['Hippodrome'] = hippo
            df['Detail'] = more
            df['Prix'] = prix
            df['Distance'] = dist1

            df = df.drop('ODDS', axis=1)

            racefilename = 'data_table' + '.csv'
            df.to_csv(racefilename)

            # Display the modified DataFrame
            print('File saved as ' + racefilename)
        else:
            print("There are less than two tables on the webpage.")
    else:
        print("Failed to fetch the webpage. Status code:", response.status_code)

def race_id(url):
    import requests
    from bs4 import BeautifulSoup
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    race_id2 = soup.find('div', {'id': 'detailCourseAutresCourse'}).text.split('-')[0].lower().strip()
    return race_id2


# THESE FUNCTIONS ARE APPLIED TO DOWNLOADING HISTORICAL DATA FOR HORSE RACES

'''Takes the url of a race track as an argument and returns a list of meeting urls contained in the page'''
def get_horse_meets(url):
            
    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    import requests
    from bs4 import BeautifulSoup
    
    # Send a GET request to the URL
    response = requests.get(url, headers=headers)
    
    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the div element with id 'onglet_reunions_passees'
    div_element = soup.find('div', {'id': 'onglet_reunions_passees'})

    # Initialize a list to store links
    _links = []
    
    # Check if the div element is found
    if div_element:
        
        meet_rows = div_element.find_all('tr')[1:]
        
        for i in meet_rows:
            if not i.find('td').find('img').get('alt') in ['Obs', 'P']:
                continue
            link_to_meeting = i.find('td').find('a').get('href')
            _links.append('https://www.turfomania.fr' + link_to_meeting)
    
        # Extract unique href attributes
        unique_links = set(_links)

        # Get unique meeting links
        reunions = [l for l in unique_links if 'idreunion' in l]
    else:
        print("Element with id 'onglet_reunions_passees' not found on the page.")
        
    return reunions

def get_trot_meets(url):
    
    import requests
    from bs4 import BeautifulSoup

    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    
    # Send a GET request to the URL
    response = requests.get(url, headers=headers)
    
    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the div element with id 'onglet_reunions_passees'
    div_element = soup.find('div', {'id': 'onglet_reunions_passees'})

    # Initialize a list to store links
    _links = []
    
    # Check if the div element is found
    if div_element:
        
        meet_rows = div_element.find_all('tr')[1:]
        
        for i in meet_rows:
            if not i.find('td').find('img').get('alt') in ['A', 'M']:
                continue
            link_to_meeting = i.find('td').find('a').get('href')
            _links.append('https://www.turfomania.fr' + link_to_meeting)
    
        # Extract unique href attributes
        unique_links = set(_links)

        # Get unique meeting links
        reunions = [l for l in unique_links if 'idreunion' in l]
    else:
        print("Element with id 'onglet_reunions_passees' not found on the page.")
        
    return reunions

# Define a function to convert the date format to 2024-12-25
def convert_date(date_str):
    import numpy as np
    # Check if the date string is NaN
    if pd.isna(date_str):
        return np.nan  # Return NaN for missing values
    
    # Create a mapping for month names in French to numbers
    month_mapping = {
        'Janvier': '01', 'Février': '02', 'Fevrier': '02', 'Mars': '03', 'Avril': '04',
        'Mai': '05', 'Juin': '06', 'Juillet': '07', 'Août': '08', 'Aout': '08',
        'Septembre': '09', 'Octobre': '10', 'Novembre': '11', 'Décembre': '12', 'Decembre': '12'
    }
    
    # Split the date string
    parts = date_str.split()
    
    # Check if the expected parts exist
    if len(parts) < 4:
        return None  # Return None if the date string is not formatted correctly
    
    day = parts[1]
    month = month_mapping.get(parts[2], None)  # Use .get to avoid KeyError
    year = parts[3]
    
    if month is None:
        return None  # Return None if the month is not found
    
    # Return the formatted string
    return f"{year}-{month}-{day}"

#A FUNCTION TO COUNT HORSES PER RACE
def map_horse_counts(df):
    # Count the number of horses in each race
    horse_count = df['ID_COURSE'].value_counts().reset_index()
    horse_count.columns = ['ID_COURSE', 'num_horses']
    
    # Create a mapping dictionary
    horse_count_dict = dict(zip(horse_count['ID_COURSE'], horse_count['num_horses']))
    
    # Map the horse counts to the original DataFrame
    df['num_horses'] = df['ID_COURSE'].map(horse_count_dict)
    
    return df

def compute_trot_composite_score(row, trot_weights=None):
    """Compute a trotting-specific composite score (0-100).
    
    Uses trotting-specific metrics:
    - FA (harness fitness) or FM (mounted fitness)
    - Speed (REC.) - lower is better
    - Average rank - lower is better
    - Recent average rank - lower is better
    - Trend slope - negative is better (improving trend)
    - Odds (Cote) - lower odds = better favorite
    - Aggressiveness (0-4) - trainer shoeing strategy indicator
    
    Returns score 0-100 where higher is better.
    
    Accepts optional `trot_weights` dict to allow runtime-configurable weighting.
    If `trot_weights` is None, falls back to built-in defaults. Callers may also
    pass a dict by attaching it to the pandas Series as `_trot_weights`.

    Note on Aggressiveness:
    - 0 = P4 (Conservative) - preparation/recovery form
    - 4 = D4 (Aggressive) - peak form expected
    Horses with aggressive shoeing (D4, DP/PA) may show higher expected
    performance ceiling. Conservative shoeing (P4) suggests recovery phase.
    """
    try:
        # Use named components so we can apply trotting-specific weights
        component_values = {}
        
        # For trotting races, gather component values (some may be lists)
        fa_used = False
        fm_used = False

        if pd.notna(row.get('FA')):
            try:
                fa = float(row['FA'])
                # Treat 0 or negative as missing (no data for this race type)
                if fa > 0:
                    fa_score = max(0, 1 - (fa - 1) / 8)
                    component_values['fitness'] = component_values.get('fitness', []) + [fa_score]
                    fa_used = True
            except (ValueError, TypeError):
                pass

        if pd.notna(row.get('FM')):
            try:
                fm = float(row['FM'])
                # Treat 0 or negative as missing (no data for this race type)
                if fm > 0:
                    fm_score = max(0, 1 - (fm - 1) / 8)
                    component_values['fitness'] = component_values.get('fitness', []) + [fm_score]
                    fm_used = True
            except (ValueError, TypeError):
                pass

        # Speed
        if pd.notna(row.get('REC.')):
            try:
                rec_str = str(row['REC.']).strip()
                if "'" in rec_str or '"' in rec_str:
                    time_val = rec_str.replace('"', '').replace("'", ' ').split()
                    if len(time_val) >= 2:
                        minutes = float(time_val[0])
                        seconds = float(time_val[1])
                        rec = minutes * 60 + seconds
                    else:
                        rec = float(rec_str)
                else:
                    rec = float(rec_str)
                rec_score = max(0, 1 - (rec - 30) / 90)
                component_values['speed'] = rec_score
            except (ValueError, TypeError):
                pass

        # Average rank
        if pd.notna(row.get('avg_rank')):
            try:
                avg_rank = float(row['avg_rank'])
                avg_rank_score = max(0, 1 - (avg_rank - 1) / 8)
                component_values['avg_rank'] = avg_rank_score
            except (ValueError, TypeError):
                pass

        # Recent average rank
        if pd.notna(row.get('recent_avg_rank')):
            try:
                recent_avg = float(row['recent_avg_rank'])
                recent_avg_score = max(0, 1 - (recent_avg - 1) / 8)
                component_values['recent_avg_rank'] = recent_avg_score
            except (ValueError, TypeError):
                pass

        # Trend slope
        if pd.notna(row.get('trend_rank_slope')):
            try:
                slope = float(row['trend_rank_slope'])
                slope_score = max(0, min(1, (0.5 - slope / 10)))
                component_values['trend'] = slope_score
            except (ValueError, TypeError):
                pass

        # Disqualification rates
        if pd.notna(row.get('disq_harness_rate')):
            try:
                disq_rate = float(row['disq_harness_rate'])
                disq_score = max(0, 1 - disq_rate)
                component_values['disq'] = component_values.get('disq', []) + [disq_score]
            except (ValueError, TypeError):
                pass

        if pd.notna(row.get('disq_mounted_rate')):
            try:
                disq_rate = float(row['disq_mounted_rate'])
                disq_score = max(0, 1 - disq_rate)
                component_values['disq'] = component_values.get('disq', []) + [disq_score]
            except (ValueError, TypeError):
                pass

        # Odds
        if pd.notna(row.get('COTE')):
            try:
                cote = float(row['COTE'])
                import math
                cote_log = math.log(max(1, cote))
                cote_score = max(0, 1 - cote_log / 4.0)
                component_values['cote'] = cote_score
            except (ValueError, TypeError):
                pass

        # Aggressiveness
        if pd.notna(row.get('aggressiveness')):
            try:
                agg = float(row['aggressiveness'])
                agg_score = agg / 4.0
                component_values['aggressiveness'] = agg_score
            except (ValueError, TypeError):
                pass

        # Success coefficient normalized (precomputed)
        if pd.notna(row.get('S_COEFF_norm')):
            try:
                s_norm = float(row['S_COEFF_norm'])
                s_norm = max(0.0, min(1.0, s_norm))
                component_values['s_coeff'] = s_norm
            except (ValueError, TypeError):
                pass

        # Default trotting weights (tunable)
        DEFAULT_TROT_WEIGHTS = {
            'fitness': 0.20,
            'speed': 0.20,
            'avg_rank': 0.10,
            'recent_avg_rank': 0.10,
            'trend': 0.10,
            'disq': 0.05,
            'cote': 0.10,
            'aggressiveness': 0.05,
            's_coeff': 0.10
        }
        # Use runtime-provided weights if given (should be a dict), otherwise defaults
        TROT_WEIGHTS = DEFAULT_TROT_WEIGHTS
        # Prefer explicit function argument if provided
        try:
            if isinstance(trot_weights, dict):
                TROT_WEIGHTS = trot_weights
            else:
                # Allow callers to pass runtime weights via row._trot_weights (internal)
                if hasattr(row, '_trot_weights') and isinstance(row._trot_weights, dict):
                    TROT_WEIGHTS = row._trot_weights
        except Exception:
            pass
        # Aggregate lists (fitness, disq) by averaging
        aggregated = {}
        for k, v in component_values.items():
            if isinstance(v, list):
                vals = [float(x) for x in v if x is not None]
                aggregated[k] = float(np.mean(vals)) if vals else None
            else:
                try:
                    aggregated[k] = float(v)
                except Exception:
                    aggregated[k] = None

        # Weighted average using present components only
        weighted_sum = 0.0
        weight_total = 0.0
        for comp, weight in TROT_WEIGHTS.items():
            val = aggregated.get(comp, None)
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                weighted_sum += val * weight
                weight_total += weight

        if weight_total > 0:
            composite = (weighted_sum / weight_total) * 100
            return round(composite, 2)
        else:
            return None
    except Exception as e:
        print(f"[ERROR] Failed to compute trot composite score: {e}")
        return None


def detect_race_type_from_html(soup):
    """
    Detect race type ('trot' or 'flat') from HTML table.
    Parses the "Type" column looking for 'Monté' (mounted) or 'Attelé' (harnessed) indicators.
    
    Looks for: <div class="tableau" id="tab-0"> containing <table class="hidden-smallDevice">
    The Type column is the 6th column (index 5) in data rows (rows with <td>, not <th>).
    
    Args:
        soup: BeautifulSoup object of the HTML page
        
    Returns:
        'trot' if any race has 'Monté' or 'Attelé', else 'flat'
    """
    try:
        # Find the main race table div with id="tab-0"
        tableau_div = soup.find('div', class_='tableau', id='tab-0')
        if not tableau_div:
            print("[DEBUG] No tableau div (id='tab-0') found, defaulting to flat")
            return 'flat'
        
        # Find the table with class "hidden-smallDevice"
        table = tableau_div.find('table', class_='hidden-smallDevice')
        if not table:
            print("[DEBUG] No table with class 'hidden-smallDevice' found, defaulting to flat")
            return 'flat'
        
        # Find all rows in the table
        rows = table.find_all('tr')
        if not rows:
            print("[DEBUG] No rows found in table, defaulting to flat")
            return 'flat'
        
        # Iterate through rows, skipping the header row (which has <th> elements)
        # Look for "Monté" or "Attelé" in the "Type" column (index 5)
        for row in rows:
            # Get data cells (<td> elements, not headers <th>)
            cells = row.find_all('td')
            if len(cells) >= 6:  # Type is the 6th column (index 5)
                type_cell = cells[5].get_text(strip=True)
                if 'Monté' in type_cell or 'Attelé' in type_cell:
                    print(f"[DEBUG] Detected trotting race: found '{type_cell}' in Type column")
                    return 'trot'
        
        print("[DEBUG] No 'Monté' or 'Attelé' found in Type column, defaulting to flat")
        return 'flat'
        
    except Exception as e:
        print(f"[ERROR] Failed to detect race type from HTML: {e}")
        return 'flat'
