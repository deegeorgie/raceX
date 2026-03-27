import os

# Allow tests to skip selenium imports which can be slow or pull heavy deps.
if os.environ.get('ZT_NO_SELENIUM'):
    webdriver = None
    By = None
    Options = None
else:
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
    except Exception:
        webdriver = None
        By = None
        Options = None
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from model_functions import success_coefficient, bayesian_performance_score

def get_music_from_profile(link):
    """Extract music text from jockey/trainer profile page"""
    if not link:
        return None
    
    try:
        full_url = f"https://zone-turf.fr{link}"
        response = requests.get(full_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        fiche_div = soup.find('div', class_='fiche')
        if fiche_div:
            musique_div = fiche_div.find('div', class_='musique')
            if musique_div:
                return musique_div.get_text(strip=True)
        return None
    except Exception as e:
        print(f"[ERROR] Failed to get music from {link}: {e}")
        return None


def get_former_weight_from_link(link):
    """Given a relative horse link (e.g. '/cheval/...'), fetch the horse page
    and extract the former weight: the 5th <td> of the first row in the
    table located under the div with classes including 'inner-bloc' and 'course'.
    Returns a float or None.
    """
    if not link:
        return None

    try:
        full_url = f"https://zone-turf.fr{link}"
        resp = requests.get(full_url, timeout=10)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # locate the div that contains course data; match when both tokens present
        div = soup.find('div', class_=lambda c: c and 'inner-bloc' in c and 'course' in c)
        if not div:
            # fallback: try matching the full class string
            div = soup.find('div', class_='bloc data inner-bloc course')

        if not div:
            return None

        table = div.find('table')
        if not table:
            return None

        # Skip header rows (with <th> elements), find first data row (with <td> elements)
        rows = table.find_all('tr')
        first_row = None
        for row in rows:
            if row.find_all('td'):  # Check if row has td elements (data row, not header)
                first_row = row
                break
        
        if not first_row:
            return None

        tds = first_row.find_all('td')
        if len(tds) < 5:
            return None

        raw = tds[4].get_text(strip=True)
        if not raw or raw in ['-', '']:
            return None

        # handle comma as decimal separator and non-breaking spaces
        raw = raw.replace('\xa0', '').replace(' ', '').replace(',', '.')
        try:
            return float(raw)
        except Exception:
            return None
    except Exception:
        return None


def get_former_weight_from_row(row):
    """From a parsed <tr> row, extract the first link in the 11th <td> and
    return the numeric former weight (float) or None.
    """
    try:
        tds = row.find_all('td')
        if len(tds) < 11:
            return None
        link = tds[10].find('a')
        if not link or not link.get('href'):
            return None
        return get_former_weight_from_link(link.get('href'))
    except Exception:
        return None

def scrape_zone_turf(url, progress_callback=None, cancel_check=None):
    """Scrape tables using pandas read_html with class and course ID filters
    Accepts an optional progress_callback(percent:int, message:str) to report progress.
    Accepts an optional cancel_check callable that returns True if cancellation requested.
    """
    print(f"[INFO] Scraping Zone-Turf with pandas: {url}")
    if progress_callback:
        try:
            progress_callback(5, "Starting scrape_zone_turf")
        except Exception:
            pass
    
    # Check for early cancellation
    if cancel_check and cancel_check():
        print("[INFO] Cancellation requested before scraping begins")
        return pd.DataFrame()
    
    try:
        # Extract meeting id from URL robustly (digits before .html at end of URL)
        # e.g. https://www.zone-turf.fr/programmes/r2-cagnes-sur-mer-194293.html -> 194293
        import re
        meeting_id = None
        try:
            meeting_id_match = re.search(r'-(\d+)\.html$', url)
            if meeting_id_match:
                meeting_id = meeting_id_match.group(1)
            else:
                # fallback: any digits before .html
                meeting_id_match = re.search(r'(\d+)\.html', url)
                meeting_id = meeting_id_match.group(1) if meeting_id_match else None
        except Exception:
            meeting_id = None

        # Check for cancellation before expensive operations
        if cancel_check and cancel_check():
            print("[INFO] Cancellation requested before HTML read")
            return pd.DataFrame()

        # Read all tables from the page using robust reader
        from model_functions import read_html_flexible
        all_tables = read_html_flexible(url)
        print(f"[INFO] Found {len(all_tables)} tables on page")
        
        # Check for cancellation after network request
        if cancel_check and cancel_check():
            print("[INFO] Cancellation requested after initial read_html")
            return pd.DataFrame()
        
        # Get page HTML to find course table IDs
        import requests
        from bs4 import BeautifulSoup
        import re
        
        response = requests.get(url, timeout=10)
        
        # Check for cancellation after second request
        if cancel_check and cancel_check():
            print("[INFO] Cancellation requested after BeautifulSoup request")
            return pd.DataFrame()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract date and track name from page header
        race_date = track_name = None
        title_div = soup.find('div', class_='title')
        if title_div:
            # Find the strong tag with date
            strong_tag = title_div.find('strong')
            if strong_tag:
                race_date = strong_tag.get_text(strip=True)
            # Find the span with track name
            span_print = title_div.find('span', class_='print')
            if span_print:
                track_name = span_print.get_text(strip=True).replace('à ', '')
        
        # Find tables with specific class AND course ID pattern
        all_class_tables = soup.find_all('table', class_='inner-bloc sorting engages tableResponsive')
        
        # Filter tables that also have course ID pattern
        course_tables = []
        for table in all_class_tables:
            table_id = table.get('id', '')
            if re.match(r'course\d+', table_id):
                course_tables.append(table)
        
        course_ids = [table.get('id') for table in course_tables]
        print(f"[INFO] Found {len(course_tables)} tables with class 'inner-bloc sorting engages tableResponsive' AND course ID pattern")
        print(f"[INFO] Course table IDs: {course_ids}")
        if progress_callback:
            try:
                progress_callback(15, f"Found {len(course_tables)} course tables")
            except Exception:
                pass
        
        # Only process tables that match our criteria
        all_data = []
        
        # Process only the matching course tables
        for course_idx, course_table in enumerate(course_tables):
            # Check for cancellation before processing each course
            if cancel_check and cancel_check():
                print(f"[INFO] Cancellation requested during course processing (course {course_idx+1}/{len(course_tables)})")
                break
            
            course_id = course_table.get('id')
            print(f"[INFO] Processing course table '{course_id}'")
            if progress_callback:
                try:
                    total = len(course_tables) if len(course_tables) else 1
                    pct = 20 + int((course_idx / total) * 40)
                    progress_callback(pct, f"Processing course {course_id} ({course_idx+1}/{total})")
                except Exception:
                    pass
            
            # Extract race information before table
            race_id = prize_name = race_conditions = None
            
            # Find nomCourse div before this table
            nom_course = course_table.find_previous('div', class_='nomCourse')
            if nom_course:
                text = nom_course.get_text(strip=True)
                # normalize encoding artifacts around the N marker
                text = re.sub(r'N\D+(\d+)', r'N\1', text)
                # Extract race ID (R2 Course N2)
                race_match = re.search(r'(R\d+\s+Course\s+N\d+)', text)
                if race_match:
                    race_id = race_match.group(1)
                # Extract prize name (everything after ':')
                if ':' in text:
                    try:
                        after_colon = text.split(':', 1)[1]
                        parts = after_colon.split('-', 1)
                        prize_name = parts[1].strip() if len(parts) > 1 else after_colon.strip()
                    except Exception:
                        prize_name = None

            # Find numerical id of race in div with class 'bloc data course' before this table
            # Prefer extracting numeric id from the link href if available, otherwise parse text
            id_course = None
            course_div = course_table.find_previous('div', class_='bloc data course')
            if course_div:
                try:
                    # Prefer anchor with name attribute (e.g. <a name="1126149"></a>)
                    name_anchor = course_div.find('a', attrs={'name': True})
                    if name_anchor:
                        nm = name_anchor.get('name')
                        if nm:
                            id_course = nm

                    # Fallback: find first anchor with an href containing digits
                    if not id_course:
                        a_with_href = course_div.find('a', href=True)
                        if a_with_href:
                            href = a_with_href.get('href', '')
                            if href:
                                nums = re.findall(r"(\d+)", href)
                                if nums:
                                    id_course = nums[-1]

                    # Last fallback: any digits inside the course_div text
                    if not id_course:
                        text_val = course_div.get_text(strip=True)
                        m = re.search(r"(\d+)", text_val)
                        if m:
                            id_course = m.group(1)

                except Exception as e:
                    print(f"[DEBUG] Failed to extract id_course from course_div: {e}")
                if id_course:
                    print(f"[DEBUG] Extracted id_course: {id_course}")
            
            # Find race conditions in presentation div
            presentation = course_table.find_previous('div', class_='inner-bloc presentation')
            ext_conditions = None
            if presentation:
                desc_course = presentation.find('p', class_='descCourse')
                # find all candidate hidden-smallDevice <p> elements and pick the most useful one
                hidden_ps = presentation.find_all('p', class_='hidden-smallDevice')
                chosen_text = None
                
                print(f"[DEBUG] Course {course_id}: Found {len(hidden_ps)} hidden-smallDevice elements")
                
                if hidden_ps:
                    import re
                    for idx, ptag in enumerate(hidden_ps):
                        text = ptag.get_text(strip=True)
                        print(f"[DEBUG]   [{idx}] Raw text: {text!r}")
                        
                        # skip empty/nbsp-only entries
                        if not text or text == '\xa0' or text == '&nbsp;':
                            print(f"[DEBUG]     -> SKIP: empty/nbsp")
                            continue
                        # skip legend lines that mention oeillères or other legends
                        low = text.lower()
                        if 'oeill' in low or 'legend' in low:
                            print(f"[DEBUG]     -> SKIP: contains 'oeill' or 'legend'")
                            continue
                        # prefer text that contains letters or digits (meaningful)
                        if re.search(r'[A-Za-z0-9]', text):
                            print(f"[DEBUG]     -> SELECTED (has alphanumeric)")
                            chosen_text = text
                            break
                        else:
                            print(f"[DEBUG]     -> SKIP: no alphanumeric content")
                
                # fallback: if none matched above, try the first hidden p that has any text
                if chosen_text is None and hidden_ps:
                    print(f"[DEBUG] No preferred match found, trying fallback...")
                    for ptag in hidden_ps:
                        text = ptag.get_text(strip=True)
                        if text and text not in ('\xa0', '&nbsp;'):
                            print(f"[DEBUG]   Fallback selected: {text!r}")
                            chosen_text = text
                            break

                if desc_course:
                    race_conditions = desc_course.get_text(strip=True)
                
                # Always set ext_conditions if we found chosen_text from hidden-smallDevice
                if chosen_text:
                    ext_conditions = chosen_text
                    # If descCourse is missing/empty, also use ext_conditions for race_conditions
                    if not race_conditions:
                        race_conditions = ext_conditions
                        print(f"[DEBUG] Course {course_id} using ext_conditions as race_conditions fallback: {ext_conditions!r}")
                    else:
                        print(f"[DEBUG] Course {course_id} set ext_conditions: {ext_conditions!r}")
            
            # Check for Quinté+ flag in combinaison div
            is_quinte_plus = False
            if presentation:
                combinaison_div = presentation.find('div', class_='combinaison fr')
                if combinaison_div:
                    quinte_img = combinaison_div.find('img', alt='Quinté+')
                    is_quinte_plus = quinte_img is not None
            
            # Convert the specific table to DataFrame and extract links
            table_html = str(course_table)
            try:
                course_dfs = pd.read_html(table_html)
                if course_dfs:
                    df = course_dfs[0]  # Take the first (and should be only) table
                    print(f"[INFO] Table shape: {df.shape}")
                    
                    # Extract links and data from data rows only (skip header)
                    jockey_links = []
                    trainer_links = []
                    oeill_list = []
                    former_weights = []
                    horse_links = []

                    data_rows = course_table.find_all('tr')[1:]  # Skip header row
                    for row in data_rows:
                        jockey_link = trainer_link = fer_plaque = None
                        oeill_val = None
                        former_weight = None
                        horse_link = None

                        # Find jockey and trainer links in intervenant cells
                        intervenant_cells = row.find_all('td', class_='intervenant')
                        for cell in intervenant_cells:
                            link = cell.find('a', class_='link')
                            if link and link.get('href'):
                                href = link.get('href')
                                if '/jockey/' in href:
                                    jockey_link = href
                                elif '/entraineur/' in href:
                                    trainer_link = href

                        # Check second <td> for an <img> whose alt contains 'oeill' (prefer oeillere icons)
                        tds = row.find_all('td')
                        if len(tds) > 1:
                            second_td = tds[1]
                            # look through all images and prefer one with 'oeill' in alt text
                            imgs = second_td.find_all('img')
                            for img in imgs:
                                alt = img.get('alt')
                                if alt and 'oeill' in alt.lower():
                                    oeill_val = alt.strip()
                                    break

                        jockey_links.append(jockey_link)
                        trainer_links.append(trainer_link)
                        oeill_list.append(oeill_val)
                        # extract horse link from second td when present
                        try:
                            tds_local = row.find_all('td')
                            if len(tds_local) > 1:
                                a_horse = tds_local[1].find('a', class_='link')
                                if a_horse and a_horse.get('href'):
                                    horse_link = a_horse.get('href')
                        except Exception:
                            horse_link = None
                        horse_links.append(horse_link)
                        # we'll resolve former weights in parallel later; append placeholder
                        former_weights.append(None)
                    
                    # Clean the dataframe
                    df = df.dropna(how='all')  # Remove empty rows
                    
                    # Filter out NON PARTANT rows
                    if not df.empty:
                        # Convert all columns to string for text search
                        df_str = df.astype(str)
                        
                        # Create mask for NON PARTANT rows
                        non_partant_mask = df_str.apply(
                            lambda row: row.str.contains('NON PARTANT|NON PARTANTE', case=False, na=False).any(), 
                            axis=1
                        )
                        
                        # Filter out NON PARTANT rows
                        df_clean = df[~non_partant_mask].copy()
                        
                        print(f"[INFO] Removed {non_partant_mask.sum()} NON PARTANT rows")
                        print(f"[INFO] Clean table shape: {df_clean.shape}")

                        if not df_clean.empty:
                            # Resolve former weights in parallel for unique horse links
                            try:
                                if progress_callback:
                                    try:
                                        progress_callback(60, "Resolving former weights (parallel)")
                                    except Exception:
                                        pass
                                unique_links = [l for l in set(horse_links) if l]
                                link_to_weight = {}
                                if unique_links:
                                    with ThreadPoolExecutor(max_workers=8) as exc:
                                        futures = {exc.submit(get_former_weight_from_link, link): link for link in unique_links}
                                        for fut in as_completed(futures):
                                            link = futures[fut]
                                            try:
                                                link_to_weight[link] = fut.result()
                                            except Exception:
                                                link_to_weight[link] = None
                                # build filled former_weights list aligned with horse_links order
                                former_weights_filled = [link_to_weight.get(l) if l else None for l in horse_links]

                                if progress_callback:
                                    try:
                                        progress_callback(75, "Former weights resolved")
                                    except Exception:
                                        pass

                                # Create series mapped to the original dataframe index to keep alignment
                                idx = df.index
                                jockey_series = pd.Series(jockey_links, index=idx)
                                trainer_series = pd.Series(trainer_links, index=idx)
                                oeill_series = pd.Series(oeill_list, index=idx)
                                horse_series = pd.Series(horse_links, index=idx)
                                former_series = pd.Series(former_weights_filled, index=idx)

                                df_clean['jockey_link'] = jockey_series.loc[df_clean.index].values
                                df_clean['trainer_link'] = trainer_series.loc[df_clean.index].values
                                df_clean['Oeill.'] = oeill_series.loc[df_clean.index].values
                                df_clean['horse_link'] = horse_series.loc[df_clean.index].values
                                df_clean['PAST_POIDS'] = pd.to_numeric(former_series.loc[df_clean.index].values, errors='coerce')
                            except Exception as e:
                                print(f"[WARN] Failed to align link/weight series or fetch weights: {e}")

                            # Extract music data for each row
                            jockey_music = []
                            trainer_music = []
                            if progress_callback:
                                try:
                                    progress_callback(80, "Fetching music for jockeys/trainers")
                                except Exception:
                                    pass

                            for i, (idx, row) in enumerate(df_clean.iterrows()):
                                jockey_link = row.get('jockey_link')
                                trainer_link = row.get('trainer_link')
                                print(f"[INFO] Fetching music for row {idx}...")
                                jockey_music.append(get_music_from_profile(jockey_link))
                                trainer_music.append(get_music_from_profile(trainer_link))
                                if progress_callback and i and (i % 10 == 0):
                                    try:
                                        subpct = 80 + int((i / max(1, len(df_clean))) * 15)
                                        progress_callback(subpct, f"Fetching music {i+1}/{len(df_clean)}")
                                    except Exception:
                                        pass
                            
                            df_clean['jockey_music'] = jockey_music
                            df_clean['trainer_music'] = trainer_music
                            
                            # Drop jockey and trainer link columns
                            df_clean = df_clean.drop(columns=['jockey_link', 'trainer_link'], errors='ignore')
                            
                            # Add race information and metadata
                            df_clean['race_date'] = race_date
                            df_clean['HIPPODROME'] = track_name
                            df_clean['REF_COURSE'] = race_id
                            df_clean['prize_name'] = prize_name
                            df_clean['race_conditions'] = race_conditions
                            df_clean['Q+'] = is_quinte_plus
                            df_clean['DESCRIPTIF'] = ext_conditions
                            #df_clean['table_index'] = course_idx + 1
                            df_clean['course_id'] = course_id
                            df_clean['ID_COURSE'] = id_course
                            df_clean['RACE_URL'] = f"{url}#{id_course}"
                            
                            all_data.append(df_clean)
            except Exception as e:
                print(f"[ERROR] Failed to process table {course_id}: {e}")
                continue
        
        # Combine all tables
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            
            print(f"[SUCCESS] Combined {len(all_data)} tables into DataFrame with {len(final_df)} rows")
            if progress_callback:
                try:
                    progress_callback(95, "Cleaning data and finalizing")
                except Exception:
                    pass
            
            # Clean up data
            try:
                # Extract distance from Cheval column (text in brackets)
                if 'Cheval' in final_df.columns:
                    try:
                        final_df['C.'] = final_df['Cheval'].str.extract(r'\[(.*?)\]')[0]
                    except Exception as e:
                        print(f"[WARN] Failed to extract Corde: {e}")
                    if 'REF_COURSE' in final_df.columns:
                        try:
                            final_df['Cote'] = final_df.groupby('REF_COURSE')['Cote'].transform(lambda g: g.fillna(g.median()))
                        except Exception as e:
                            print(f"[WARN] Failed to fill Cote by REF_COURSE: {e}")

                # Create a new column 'Dech.' from Cheval column by extracting any digits in parentheses that are followed by kg (e.g., (58kg))
                if 'REF_COURSE' in final_df.columns:
                    try:
                        final_df['starters'] = final_df.groupby('REF_COURSE').transform('size')
                    except Exception as e:
                        print(f"[WARN] Failed to compute num_starters: {e}")

                # Create a new column 'j-Dech.' from the Jockey column by extracting any digits in parentheses and converting to numeric
                if 'REF_COURSE' in final_df.columns:
                    try:
                        final_df['REF_COURSE'] = final_df['REF_COURSE'].str.replace(r'R(\d+)\s+Course\s+N(\d+)', r'R\1C\2', regex=True)
                        final_df['j-Dech.'] = pd.to_numeric(final_df['j-Dech.'], errors='coerce')
                    except Exception as e:
                        print(f"[WARN] Failed to extract j-Dech.: {e}")
                
                # Extract Cote using heuristic across candidate columns (Unnamed:13, Unnamed:12, Unnamed:11, existing Cote)
                # Heuristic: parse numeric tokens (decimals/fractions) and prefer the column with the most numeric tokens
                # (evolution history like '2.1 2.0 3.0'). Take the last numeric token as the latest odds.
                # We log counts for which column was selected to help tune heuristics. Preserve final_df['Cote'] name
                try:
                    import re

                    candidates = ['Unnamed: 13', 'Unnamed: 12', 'Unnamed: 11', 'Cote']

                    def parse_odds_string(s):
                        if pd.isna(s):
                            return None
                        raw = str(s).strip()
                        # strip simple HTML tags if any
                        raw = re.sub(r'<[^>]+>', '', raw)
                        # normalize nbsp and comma
                        raw = raw.replace('\xa0', ' ').replace(',', '.')
                        # try fraction like '2/1' -> decimal odds = 1 + 2/1
                        frac = re.search(r'(\d+)\s*/\s*(\d+)', raw)
                        if frac:
                            try:
                                return float(1.0 + int(frac.group(1)) / int(frac.group(2)))
                            except Exception:
                                pass
                        # find decimal/integer tokens
                        nums = re.findall(r'\d+(?:\.\d+)?', raw)
                        if not nums:
                            return None
                        try:
                            return float(nums[-1])
                        except Exception:
                            return None

                    used_counts = {c: 0 for c in candidates}
                    cote_values = []

                    # iterate rows and choose best candidate per row
                    for i, row in final_df.iterrows():
                        found = []
                        for c in candidates:
                            if c in final_df.columns:
                                raw = row.get(c)
                                if pd.isna(raw) or str(raw).strip() in ['', '-', 'nan']:
                                    continue
                                tokens = re.findall(r'\d+(?:\.\d+)?', str(raw))
                                parsed = parse_odds_string(raw)
                                if parsed is not None:
                                    found.append((c, parsed, len(tokens)))
                        if not found:
                            cote_values.append(np.nan)
                            continue
                        # prefer most tokens (evolution). If tie, prefer right-most candidate in list (later index)
                        order_index = {name: idx for idx, name in enumerate(candidates)}
                        found.sort(key=lambda x: (x[2], order_index.get(x[0], 0)), reverse=True)
                        choice = found[0]
                        cote_values.append(choice[1])
                        used_counts[choice[0]] = used_counts.get(choice[0], 0) + 1

                    final_df['Cote'] = pd.to_numeric(pd.Series(cote_values, index=final_df.index), errors='coerce')
                    # Fill missing with race median when possible to maintain previous behaviour
                    if 'race_id' in final_df.columns:
                        final_df['Cote'] = final_df.groupby('race_id')['Cote'].transform(lambda g: g.fillna(g.median()))
                    # round to 1 decimal as before
                    final_df['Cote'] = np.round(final_df['Cote'], 1)

                    # logging summary of which candidate columns were used
                    try:
                        print(f"[INFO] Cote extraction counts: {used_counts}")
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[WARN] Failed to extract Cote: {e}")

                # Let's work on cleaning Poids column. Here is the logic:
                # Check each value in Dech. column and j-Dech. column. If both are present, then Poids is equal to j-Dech.
                # If only Dech. is present, then Poids is equal to Dech. Else Poids remains as is.
                if 'Poids' in final_df.columns:
                    try:
                        def compute_poids(row):
                            dech = row.get('Dech.')
                            j_dech = row.get('j-Dech.')
                            poids = row.get('Poids')
                            if pd.notna(j_dech):
                                return j_dech
                            elif pd.notna(dech):
                                return dech
                            else:
                                return poids
                        final_df['Poids'] = final_df.apply(compute_poids, axis=1)
                    except Exception as e:
                        print(f"[WARN] Failed to compute Poids: {e}")
                
                # Defer IC computation until after Poids/Cote are cleaned to numeric
                # (computed later below after numeric cleaning)

                # n_weight will be computed later after Poids is cleaned to numeric

                # Clean Jockey names by removing any trailing digits in parentheses
                if 'Jockey' in final_df.columns:
                    try:
                        final_df['Jockey'] = final_df['Jockey'].str.replace(r'\s*\(\d+\)$', '', regex=True).str.strip()
                    except Exception as e:
                        print(f"[WARN] Failed to clean Jockey: {e}")

                # Create two new columns for Sex and Age from the SA column
                if 'SA' in final_df.columns:
                    try:
                        sa_split = final_df['SA'].str.extract(r'([A-Za-z]+)\s*(\d+)')
                        final_df['Sexe'] = sa_split[0].map({'H':  0, 'M': 1, 'F': 2 })
                        final_df['Age'] = sa_split[1]
                    except Exception as e:
                        print(f"[WARN] Failed to extract Sex/Age: {e}")

                # Create new column num_starters from race_id by counting number of entries per race
                if 'race_id' in final_df.columns:
                    try:
                        final_df['num_starters'] = final_df.groupby('race_id').transform('size')
                    except Exception as e:
                        print(f"[WARN] Failed to compute num_starters: {e}")

                # Create a new_column for Allocation from the race_conditions column by extracting the amount before '€'. Said amount may have spaces before € (20 000 €)
                if 'race_conditions' in final_df.columns:
                    try:
                        allocation_match = final_df['race_conditions'].str.extract(r'([\d\s]+)\s*€')
                        final_df['Allocation'] = allocation_match[0]
                        final_df['Allocation'] = final_df['Allocation'].str.replace(r'\s+', '', regex=True)
                        final_df['Allocation'] = pd.to_numeric(final_df['Allocation'], errors='coerce')
                    except Exception as e:
                        print(f"[WARN] Failed to extract Allocation: {e}")

                # create a new column for Lice by looking in race_conditions for 'Droite' or 'Gauche'
                if 'race_conditions' in final_df.columns:
                    try:
                        def extract_lice(text):
                            if pd.isna(text):
                                return None
                            if 'droite' in text.lower():
                                return 2
                            elif 'gauche' in text.lower():
                                return 1
                            else:
                                return 0
                        final_df['Lice'] = final_df['race_conditions'].apply(extract_lice)
                    except Exception as e:
                        print(f"[WARN] Failed to extract Lice: {e}")
                    
                # Create a new column for Distance from the race_conditions column
                if 'race_conditions' in final_df.columns:
                    try:
                        distance_match = final_df['race_conditions'].str.extract(r'(\d{3,4})\s*mètres')
                        final_df['Distance'] = distance_match[0]
                    except Exception as e:
                        print(f"[WARN] Failed to extract Distance: {e}")

                # Create a new column for signaling a Handicap race from race_conditions
                if 'race_conditions' in final_df.columns:
                    try:
                        final_df['HANDICAP'] = final_df['race_conditions'].str.contains('handicap', case=False, na=False)
                    except Exception as e:
                        print(f"[WARN] Failed to extract HANDICAP: {e}")

                # create a new column for signaling Reclamer races from DESCRIPTIF
                if 'DESCRIPTIF' in final_df.columns:
                    try:
                        final_df['RECLAMER'] = final_df['DESCRIPTIF'].str.contains('reclamer', case=False, na=False)
                    except Exception as e:
                        print(f"[WARN] Failed to extract RECLAMER: {e}")

                # Create a new column for signaling a listed race from the race_conditions
                if 'race_conditions' in final_df.columns:
                    try:
                        final_df['LISTED'] = final_df['race_conditions'].str.contains('course l', case=False, na=False)
                    except Exception as e:
                        print(f"[WARN] Failed to extract LISTED: {e}")

                # Create a new column for Groupe races from race_conditions. A group race is generally described as 'Groupe I', 'Groupe II' or 'Groupe III'
                # and the regex should match these patterns case-insensitively. note that it can also be 'Groupe  1' , 'Groupe  2' etc
                # Instead of a boolean flag, we will store the matched group level (I, II, III or 1, 2, 3) or None if
                if 'race_conditions' in final_df.columns:
                    try:
                        def extract_groupe(text):
                            if pd.isna(text):
                                return None
                            match = re.search(r'Groupe\s*(I{1,3}|[1-3])', text, re.IGNORECASE)
                            if match:
                                return match.group(1)
                            return None
                        final_df['GRP'] = final_df['race_conditions'].apply(extract_groupe)
                    except Exception as e:
                        print(f"[WARN] Failed to extract Groupe: {e}")

                # Create a new column for Classe races from race_conditions by extracting 'Classe X' where X is a number or a single letter
                # and store the extracted class (e.g., '1', '2', 'A', 'B') or None
                if 'race_conditions' in final_df.columns:
                    try:
                        def extract_classe(text):
                            if pd.isna(text):
                                return None
                            match = re.search(r'Classe\s*([A-Za-z]|\d+)', text, re.IGNORECASE)
                            if match:
                                return match.group(1)
                            return None
                        final_df['Classe'] = final_df['race_conditions'].apply(extract_classe)
                    except Exception as e:
                        print(f"[WARN] Failed to extract Classe: {e}")

                # Create a new column HIPPOID based on HIPPODROME mapping
                if 'HIPPODROME' in final_df.columns:
                    try:
                        HIPPODROME_MAPPING = {
                            'AUTEUIL': '1', 'COMPIEGNE': '2', 'CLAIREFONTAINE': '3', 
                            'CHANTILLY': '4', 'DEAUVILLE': '5', 'PAU': '6', 
                            'SAINT CLOUD': '7', 'LONGCHAMP': '8', 'FONTAINEBLEAU': '9',
                            'TOULOUSE': '10', 'CAGNES SUR MER': '11', 'LYON LA SOIE': '12',
                            'VICHY': '13', 'STRASBOURG': '14', 'DIEPPE': '15', 'BORDEAUX LE BOUSCAT': '16',
                            'MARSEILLE BORELY': '17', 'MAISONS LAFFITTE': '18', 'LE LION D ANGERS': '19',
                            'LA TESTE DE BUCH': '20', 'NANTES': '21', 'MARSEILLE VIVAUX': '22'
                        }
                        final_df['HIPPOID'] = final_df['HIPPODROME'].str.upper().map(HIPPODROME_MAPPING)
                    except Exception as e:
                        print(f"[WARN] Failed to extract HIPPOID: {e}")

                # create a new column for surface type from descriptif
                if 'DESCRIPTIF' in final_df.columns:
                    try:
                        def _surface_from_desc(x):
                            if pd.isna(x):
                                return None
                            s = str(x).lower()
                            if 'psf' in s:
                                return 'Psf'
                            if 'turf' in s:
                                return 'Turf'
                            if 'dirt' in s:
                                return 'Dirt'
                            return None
                        final_df['Surface'] = final_df['DESCRIPTIF'].apply(_surface_from_desc)
                    except Exception as e:
                        print(f"[WARN] Failed to extract Surface: {e}")

                # Clean the Oeill. column so that if the value contains 'oeillères australiennes' it is replaced with just 'A'
                # And if not 'australiennes' in the text replace with 'X'. If Oeill. is None or NaN, leave it as is.
                if 'Oeill.' in final_df.columns:
                    try:
                        final_df['Oeill.'] = final_df['Oeill.'].apply(
                            lambda x: 1 if pd.notna(x) and 'australiennes' in str(x).lower() else (2 if pd.notna(x) and 'australiennes' not in str(x).lower() else x)
                        )
                    except Exception as e:
                        print(f"[WARN] Failed to clean Oeill.: {e}")

                # create new column for race type from race conditions columns by looking for any of the following keywords: 'Plat', 'Steeple', 'Cross', 'Haies'
                if 'race_conditions' in final_df.columns:
                    try:
                        def extract_race_type(text):
                            if pd.isna(text):
                                return None
                            text_str = str(text).lower()
                            if 'plat' in text_str:
                                return 1
                            elif 'steeple' in text_str:
                                return 2
                            elif 'cross' in text_str:
                                return 4
                            elif 'haies' in text_str:
                                return 3
                            else:
                                return None
                        final_df['dscp'] = final_df['race_conditions'].apply(extract_race_type)
                    except Exception as e:
                        print(f"[WARN] Failed to extract dscp: {e}")

                # Clean music columns - handle None values
                if 'jockey_music' in final_df.columns:
                    try:
                        final_df['jockey_music'] = final_df['jockey_music'].fillna('').astype(str).str.replace('Musique :', '', regex=False).str.strip()
                        final_df['jockey_music'] = final_df['jockey_music'].replace('', None)
                    except Exception as e:
                        print(f"[WARN] Failed to clean jockey_music: {e}")
                        
                if 'trainer_music' in final_df.columns:
                    try:
                        final_df['trainer_music'] = final_df['trainer_music'].fillna('').astype(str).str.replace('Musique :', '', regex=False).str.strip()
                        final_df['trainer_music'] = final_df['trainer_music'].replace('', None)
                    except Exception as e:
                        print(f"[WARN] Failed to clean trainer_music: {e}")

                # Clean Cheval names by splitting at '' and taking from the beginning to H F or M followed by digit (H4, F3, M5 etc)
                if 'Cheval' in final_df.columns:
                    try:
                        final_df['Cheval'] = final_df['Cheval'].str.split(r'\s+(?=[HFM]\d)').str[0].str.strip()
                    except Exception as e:
                        print(f"[WARN] Failed to clean Cheval: {e}")

                # Clean the race_id column so that instead of 'R2 Course N2' it is just 'R2C2'
                if 'race_id' in final_df.columns:
                    try:
                        final_df['race_id'] = final_df['race_id'].str.replace(r'R(\d+)\s+Course\s+N(\d+)', r'R\1C\2', regex=True)
                    except Exception as e:
                        print(f"[WARN] Failed to clean race_id: {e}")

                # Clean Corde by removing the string 'c:' if present
                if 'C.' in final_df.columns:
                    try:
                        final_df['C.'] = final_df['C.'].str.replace('c:', '', regex=False).str.strip()
                    except Exception as e:
                        print(f"[WARN] Failed to clean Corde: {e}")

                # Clean the Gain column to strip '???' and spaces (normalize Gains -> Gain)
                if 'Gains' in final_df.columns and 'Gain' not in final_df.columns:
                    final_df = final_df.rename(columns={'Gains': 'Gain'})
                if 'Gain' in final_df.columns:
                    try:
                        final_df['Gain'] = final_df['Gain'].astype(str).str.replace('???', '', regex=False).str.replace('\xa0', '', regex=False).str.strip()
                    except Exception as e:
                        print(f"[WARN] Failed to clean Gain: {e}")

                # Add meeting_id column (same for all rows on a page)
                try:
                    final_df['MEETING_ID'] = meeting_id
                except Exception as e:
                    print(f"[WARN] Failed to add meeting_id: {e}")

                # Rename columns
                try:
                    final_df = final_df.rename(columns={
                        'C.': 'Corde',
                        'Unnamed: 11': 'PMU',
                        'Unnamed: 12': 'PMU_FR',
                        'VH': 'VALEUR',
                        'Dernières perf.': 'Musique'
                    })
                except Exception as e:
                    print(f"[WARN] Failed to rename columns: {e}")

                # Check values in the VALEUR, Poids and Dech. columns and convert to numeric
                for col in ['VALEUR', 'Poids', 'Dech.']:
                    if col in final_df.columns:
                        try:
                            def clean_value(val):
                                try:
                                    if pd.isna(val):
                                        return None
                                    val_str = str(val).replace(',', '.').strip()
                                    if val_str in ['-', '']:
                                        return None
                                    num_val = float(val_str)
                                    if num_val >= 100:
                                        num_val = num_val / 10.0
                                    return num_val
                                except:
                                    return None
                            final_df[col] = final_df[col].apply(clean_value)
                        except Exception as e:
                            print(f"[WARN] Failed to clean {col}: {e}")

                # Compute n_weight after Poids is cleaned to numeric
                if 'PAST_POIDS' in final_df.columns and 'Poids' in final_df.columns:
                    try:
                        final_df['n_weight'] = pd.to_numeric(final_df['PAST_POIDS'], errors='coerce') - pd.to_numeric(final_df['Poids'], errors='coerce')
                    except Exception as e:
                        print(f"[WARN] Failed to compute n_weight: {e}")

                # Compute IC after numeric cleaning of Poids and Cote
                if 'Poids' in final_df.columns and 'Cote' in final_df.columns:
                    try:
                        final_df['IC'] = np.round(pd.to_numeric(final_df['Poids'], errors='coerce') - pd.to_numeric(final_df['Cote'], errors='coerce'), 1)
                    except Exception as e:
                        print(f"[WARN] Failed to compute IC: {e}")

                # Compute bayesian_performance_score if music columns are present
                if 'jockey_music' in final_df.columns:
                    try:
                        final_df['FORME_J'] = final_df['jockey_music'].apply(bayesian_performance_score)
                    except Exception as e:
                        print(f"[WARN] Failed to compute FORME_J: {e}")

                if 'trainer_music' in final_df.columns:
                    try:
                        final_df['FORME_T'] = final_df['trainer_music'].apply(bayesian_performance_score)
                    except Exception as e:
                        print(f"[WARN] Failed to compute FORME_T: {e}")

                if 'Musique' in final_df.columns:
                    try:
                        final_df['FORME'] = final_df['Musique'].apply(bayesian_performance_score)
                    except Exception as e:
                        print(f"[WARN] Failed to compute FORME: {e}")

                # Create a column IF that computes the average of FORME_J, FORME_T and FORME when present
                try:
                    def compute_if(row):
                        cols = ['FORME_J', 'FORME_T', 'FORME']
                        vals = []
                        for c in cols:
                            v = row.get(c)
                            if pd.notna(v):
                                try:
                                    vals.append(float(v))
                                except Exception:
                                    # ignore non-numeric values
                                    continue
                        return sum(vals) / len(vals) if vals else None

                    final_df['IF'] = np.round(final_df.apply(compute_if, axis=1), 2)
                except Exception as e:
                    print(f"[WARN] Failed to compute IF: {e}")

                # ------------------------------------------------------------
                # SUCCESS COEFFICIENT (S_COEFF)
                # Computes performance score based on discipline-specific music.
                # ------------------------------------------------------------

                # 1. Determine discipline character (p/s/h/c)
                disc_char = None

                if "dscp" in final_df.columns:
                    try:
                        # Extract unique non-null discipline codes
                        dvals = final_df["dscp"].dropna().unique()

                        if len(dvals) > 0:
                            code = int(dvals[0])

                            DISC_MAP = {
                                1: "p",  # Plat
                                2: "s",  # Steeple
                                3: "h",  # Haies
                                4: "c",  # Cross
                            }

                            disc_char = DISC_MAP.get(code, None)

                            if disc_char is None:
                                print(f"[WARN] Unknown discipline code: {code}")

                    except Exception as e:
                        print(f"[WARN] Failed to determine disc_char: {e}")


                # 2. Compute S_COEFF
                if "Musique" in final_df.columns:
                    try:
                        def compute_scoeff(music):
                            """
                            Wraps success_coefficient() with fallbacks:
                            - Empty / NaN music → return Bayesian score from empty string.
                            - If discipline-specific score = 0 → fallback to Bayesian aggregate.
                            """
                            try:
                                if pd.isna(music) or not isinstance(music, str) or not music.strip():
                                    return bayesian_performance_score("")

                                # discipline-specific coefficient
                                sc = success_coefficient(music, disc_char)

                                # fallback if no tokens matched the discipline
                                if sc == 0.0:
                                    return bayesian_performance_score(music)

                                return sc

                            except Exception:
                                return bayesian_performance_score(music if isinstance(music, str) else "")

                        final_df["S_COEFF"] = final_df["Musique"].apply(compute_scoeff)

                    except Exception as e:
                        print(f"[WARN] Failed to compute S_COEFF: {e}")

                # let me drop some columns that are not needed
                try:
                    cols_to_drop = ['SA', 'Unnamed: 2', 'Unnamed: 14', 'Avis']
                    final_df = final_df.drop(columns=[col for col in cols_to_drop if col in final_df.columns], errors='ignore')
                except Exception as e:
                    print(f"[WARN] Failed to drop columns: {e}")

                # Clean race_date to dd/mm/yyyy format
                if 'race_date' in final_df.columns:
                    try:
                        date_parts = final_df['race_date'].str.extract(r'(\d{1,2})\s+(\w+)\s+(\d{4})')
                        month_map = {'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04', 'mai': '05', 'juin': '06',
                                   'juillet': '07', 'août': '08', 'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'}
                        final_df['race_date'] = date_parts[0].str.zfill(2) + '/' + date_parts[1].map(month_map) + '/' + date_parts[2]
                    except Exception as e:
                        print(f"[WARN] Failed to clean race_date: {e}")
            except Exception as e:
                print(f"[WARN] Outer data cleaning error: {e}")

            # Convert all column names to uppercase before returning
            final_df.columns = final_df.columns.str.upper()
            return final_df
        else:
            print("[WARN] No valid data found")
            if progress_callback:
                try:
                    progress_callback(100, "No valid data found")
                except Exception:
                    pass
            return pd.DataFrame()
            
    except Exception as e:
        print(f"[ERROR] Failed to scrape with pandas: {e}")
        if progress_callback:
            try:
                progress_callback(100, f"Failed: {e}")
            except Exception:
                pass
        return pd.DataFrame()

if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.zone-turf.fr/programmes/"
    
    df = scrape_zone_turf(url)
    
    print(f"\n[RESULT] Scraped {len(df)} rows")
    print(f"[INFO] Columns: {list(df.columns)}")
    
    # Save to SQLite database using separate table for flat races
    if not df.empty:
        conn = sqlite3.connect('flat_zone.db')
        df.to_sql('flat_races', conn, if_exists='append', index=False)
        conn.close()
        print("[INFO] Data saved to flat_zone.db (flat_races table)")
    
    # Display sample
    if not df.empty:
        print("\n[SAMPLE] First few rows:")
        print(df.head())        
