from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sqlite3
import time
import re
from model_functions import parse_performance_string, compute_d_perf, compute_slope, slope_to_label

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

def scrape_zone_turf_trot(url, progress_callback=None, cancel_check=None):
    """Scrape trotting race tables using pandas read_html with class and course ID filters
    Accepts optional progress_callback(percent:int, message:str)
    Accepts optional cancel_check callable that returns True if cancellation requested"""
    print(f"[INFO] Scraping Zone-Turf (Trotting) with pandas: {url}")
    if progress_callback:
        try:
            progress_callback(5, "Scraping trotting races from Zone-Turf...")
        except Exception:
            pass
    
    # Check for early cancellation
    if cancel_check and cancel_check():
        print("[INFO] Cancellation requested before trot scraping begins")
        return pd.DataFrame()
    
    try:
        # Read all tables from the page
        if progress_callback:
            try:
                progress_callback(10, "Loading page content...")
            except Exception:
                pass
        
        from model_functions import read_html_flexible
        # Read all tables from the page using robust reader to avoid encoding issues
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
                progress_callback(15, f"Found {len(course_tables)} trotting race(s)")
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
                    pct = 20 + int((course_idx / total) * 30)
                    progress_callback(pct, f"Processing race {course_id} ({course_idx+1}/{total})")
                except Exception:
                    pass
            
            # Extract race information before table
            race_id = prize_name = race_conditions = None
            
            # Find nomCourse div before this table
            nom_course = course_table.find_previous('div', class_='nomCourse')
            if nom_course:
                text = nom_course.get_text(strip=True)
                # Extract race ID (R2 Course N°2)
                race_match = re.search(r'(R\d+\s+Course\s+N°\d+)', text)
                if race_match:
                    race_id = race_match.group(1)
                # Extract prize name (everything after ':')
                if ':' in text:
                    prize_name = text.split(':', 1)[1].split('-')[1].strip()

            # Find numerical id of race in div with class 'bloc data course' before this table
            # Prefer extracting numeric id from the link href if available, otherwise parse text
            course_div = course_table.find_previous('div', class_='bloc data course')
            if course_div:
                id_course = None
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
                    
                    # Extract links and ferPlaque data from data rows only (skip header)
                    jockey_links = []
                    trainer_links = []
                    fer_plaque_data = []
                    horse_links = []
                    horse_names = []
                    
                    data_rows = course_table.find_all('tr')[1:]  # Skip header row
                    for row in data_rows:
                        jockey_link = trainer_link = fer_plaque = None
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
                        
                        # Find ferPlaque data in infosGen cells
                        infos_gen_cell = row.find('td', class_='infosGen')
                        horse_name = None
                        if infos_gen_cell:
                            fer_plaque_span = infos_gen_cell.find('span', class_='ferPlaque')
                            if fer_plaque_span:
                                fer_plaque = fer_plaque_span.get_text(strip=True)

                            # Find horse link data in infosGen cells
                            horse_anchor = infos_gen_cell.find('a', class_='link')
                            if horse_anchor and horse_anchor.get('href'):
                                horse_link = horse_anchor.get('href')
                            
                            # Extract horse name from the <b> tag inside infosGen
                            horse_name_tag = infos_gen_cell.find('b')
                            if horse_name_tag:
                                horse_name = horse_name_tag.get_text(strip=True)

                        # Find distance in the 4th 'td' element of the row with class 'hidden-smallDevice'
                        distance = None
                        td_cells = row.find_all('td')
                        if len(td_cells) >= 4:
                            distance_cell = td_cells[3]
                            if 'hidden-smallDevice' in distance_cell.get('class', []):
                                dist_text = distance_cell.get_text(strip=True)
                                # Extract distance value (e.g. "2 100m" -> "2100")
                                dist_match = re.search(r'([\d\s]+)m', dist_text)
                                if dist_match:
                                    # Remove spaces and convert to integer
                                    num_part = re.sub(r'\s+', '', dist_match.group(1))
                                    distance = int(num_part) if num_part.isdigit() else None
                        
                        jockey_links.append(jockey_link)
                        trainer_links.append(trainer_link)
                        fer_plaque_data.append(fer_plaque)
                        horse_links.append(horse_link)
                        horse_names.append(horse_name)
                    
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
                        
                        # Get the indices of rows that are NOT non_partant (keeping only valid horses)
                        valid_indices = df[~non_partant_mask].index.tolist()
                        
                        # Filter out NON PARTANT rows
                        df_clean = df[~non_partant_mask].copy()
                        
                        print(f"[INFO] Removed {non_partant_mask.sum()} NON PARTANT rows")
                        print(f"[INFO] Clean table shape: {df_clean.shape}")
                        
                        if not df_clean.empty:
                            # Horse names come from the first column of the dataframe (already extracted by pandas)
                            # We don't need to extract them separately from HTML
                            first_col = df_clean.columns[0]
                            second_col = df_clean.columns[1] if len(df_clean.columns) > 1 else None
                            
                            # Use valid_indices to slice the link data (not just length-based slicing)
                            # This ensures we're taking links that correspond to the horses, not NON PARTANT rows
                            if len(jockey_links) >= len(valid_indices):
                                df_clean['jockey_link'] = [jockey_links[i] for i in valid_indices]
                            if len(trainer_links) >= len(valid_indices):
                                df_clean['trainer_link'] = [trainer_links[i] for i in valid_indices]
                            if len(fer_plaque_data) >= len(valid_indices):
                                df_clean['Def.'] = [fer_plaque_data[i] for i in valid_indices]
                            if len(horse_names) >= len(valid_indices) and second_col:
                                # Replace the second column with clean horse names extracted from <b> tags
                                df_clean[second_col] = [horse_names[i] for i in valid_indices]
                            
                            # Rename the first column to 'Cheval' if it doesn't have that name already
                            #if first_col != 'Cheval':
                            #    df_clean.rename(columns={first_col: 'Cheval'}, inplace=True)
                            
                            # Extract music data for each row
                            if progress_callback:
                                try:
                                    progress_callback(50, "Fetching jockey and trainer performance records...")
                                except Exception:
                                    pass
                            
                            jockey_music = []
                            trainer_music = []
                            
                            for idx, row in df_clean.iterrows():
                                jockey_link = row.get('jockey_link')
                                trainer_link = row.get('trainer_link')
                                
                                print(f"[INFO] Fetching music for row {idx}...")
                                jockey_music.append(get_music_from_profile(jockey_link))
                                trainer_music.append(get_music_from_profile(trainer_link))
                                
                                # Update progress every few rows
                                if progress_callback and idx and (idx % 3 == 0):
                                    try:
                                        subpct = 50 + int((idx / len(df_clean)) * 20)
                                        progress_callback(subpct, f"Fetching records {idx+1}/{len(df_clean)}")
                                    except Exception:
                                        pass
                            
                            df_clean['jockey_music'] = jockey_music
                            df_clean['trainer_music'] = trainer_music
                            if len(horse_links) >= len(valid_indices):
                                df_clean['horse_link'] = [horse_links[i] for i in valid_indices]
                            
                            # Drop jockey and trainer link columns
                            #df_clean = df_clean.drop(columns=['jockey_link', 'trainer_link'], errors='ignore')
                            
                            # Add race information and metadata
                            df_clean['race_date'] = race_date
                            df_clean['HIPPODROME'] = track_name
                            df_clean['REF_COURSE'] = race_id
                            df_clean['prize_name'] = prize_name
                            df_clean['dist'] = distance
                            df_clean['race_conditions'] = race_conditions
                            df_clean['descriptif'] = ext_conditions
                            df_clean['q+'] = is_quinte_plus
                            df_clean['table_index'] = course_idx + 1
                            df_clean['course_id'] = course_id

                            # Create two new columns for Sex and Age from the SA column
                            if 'SA' in df_clean.columns:
                                try:
                                    sa_split = df_clean['SA'].str.extract(r'([A-Za-z]+)\s*(\d+)')
                                    df_clean['Sex'] = sa_split[0].map({'H':  0, 'M': 1, 'F': 2 })
                                    df_clean['Age'] = sa_split[1]
                                except Exception as e:
                                    print(f"[WARN] Failed to extract Sex/Age: {e}")
                            
                            if progress_callback:
                                try:
                                    pct = 20 + int(((course_idx + 1) / len(course_tables)) * 30)
                                    progress_callback(pct, f"Completed race {course_id} - {len(df_clean)} horses")
                                except Exception:
                                    pass
                            
                            all_data.append(df_clean)
            except Exception as e:
                print(f"[ERROR] Failed to process table {course_id}: {e}")
                continue
        
        # Combine all tables
        if all_data:
            if progress_callback:
                try:
                    progress_callback(70, "Combining race data...")
                except Exception:
                    pass
            
            final_df = pd.concat(all_data, ignore_index=True)
            
            if progress_callback:
                try:
                    progress_callback(75, "Cleaning and standardizing data...")
                except Exception:
                    pass
            
            # Clean up data
            # Extract distance from Cheval column (text in brackets)
            #if 'Cheval' in final_df.columns:
            #    final_df['distance_extracted'] = final_df['Cheval'].str.extract(r'\[(.*?)\]')[0]
            
            # Clean music columns - handle None values
            if 'jockey_music' in final_df.columns:
                final_df['jockey_music'] = final_df['jockey_music'].fillna('').astype(str).str.replace('Musique :', '', regex=False).str.strip()
                final_df['jockey_music'] = final_df['jockey_music'].replace('', None)
            if 'trainer_music' in final_df.columns:
                final_df['trainer_music'] = final_df['trainer_music'].fillna('').astype(str).str.replace('Musique :', '', regex=False).str.strip()
                final_df['trainer_music'] = final_df['trainer_music'].replace('', None)
            
            # Clean race_date to dd/mm/yyyy format
            if 'race_date' in final_df.columns:
                date_parts = final_df['race_date'].str.extract(r'(\d{1,2})\s+(\w+)\s+(\d{4})')
                month_map = {'janvier': '01', 'février': '02', 'fevrier': '02', 'mars': '03', 'avril': '04', 'mai': '05', 'juin': '06',
                           'juillet': '07', 'août': '08', 'aout': '08', 'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12', 'decembre': '12'}
                final_df['race_date'] = date_parts[0].str.zfill(2) + '/' + date_parts[1].map(month_map) + '/' + date_parts[2]
            
            # Extract Cote column using heuristic across candidate columns
            # Heuristic: parse numeric tokens (decimals/fractions) and prefer the column with most numeric tokens
            # Take the last numeric token as the latest odds
            try:
                candidates = ['Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13']
                
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
                # Fill missing with race median when possible
                if 'REF_COURSE' in final_df.columns:
                    final_df['Cote'] = final_df.groupby('REF_COURSE')['Cote'].transform(lambda g: g.fillna(g.median()))
                # round to 1 decimal
                final_df['Cote'] = np.round(final_df['Cote'], 1)
                
                try:
                    print(f"[INFO] Cote extraction counts: {used_counts}")
                except Exception:
                    pass
            except Exception as e:
                print(f"[WARN] Failed to extract Cote: {e}")
            
            # Add race_type column to identify this as trot racing
            final_df['race_type'] = 'trot'
            
            if progress_callback:
                try:
                    progress_callback(90, "Finalizing data...")
                except Exception:
                    pass
            
            print(f"[SUCCESS] Combined {len(all_data)} tables into DataFrame with {len(final_df)} rows")
            
            # Convert all column names to uppercase before returning
            final_df.columns = final_df.columns.str.upper()
            
            if progress_callback:
                try:
                    progress_callback(100, f"Successfully scraped {len(final_df)} trotting race entries")
                except Exception:
                    pass
            
            return final_df
        else:
            print("[WARN] No valid data found")
            if progress_callback:
                try:
                    progress_callback(100, "No trotting race data found on page")
                except Exception:
                    pass
            return pd.DataFrame()
            
    except Exception as e:
        print(f"[ERROR] Failed to scrape with pandas: {e}")
        if progress_callback:
            try:
                progress_callback(100, f"Error during scraping: {str(e)}")
            except Exception:
                pass
        return pd.DataFrame()

if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.zone-turf.fr/programmes/"
    
    df = scrape_zone_turf_trot(url)
    
    print(f"\n[RESULT] Scraped {len(df)} rows")
    print(f"[INFO] Columns: {list(df.columns)}")
    
    # Save to SQLite database using separate table for trot races
    if not df.empty:
        conn = sqlite3.connect('trot_zone.db')
        df.to_sql('trot_races', conn, if_exists='append', index=False)
        conn.close()
        print("[INFO] Data saved to trot_zone.db (trot_races table)")
    
    # Display sample
    if not df.empty:
        print("\n[SAMPLE] First few rows:")
        print(df.head())        
