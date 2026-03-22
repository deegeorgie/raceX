#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RaceX v1.0 - Advanced Betting Analysis for Horse Racing
French text and Unicode support enabled
"""

import sys
import sqlite3
from datetime import datetime, date

# PyQt5 is only required for the desktop GUI. Streamlit imports this module for
# shared scraping logic, so keep PyQt5 optional to avoid import-time failures.
try:
    from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
                                 QWidget, QPushButton, QLineEdit, QTableWidget,
                                 QTableWidgetItem, QTabWidget, QLabel, QProgressBar,
                                 QMessageBox, QComboBox, QScrollArea, QSlider, QDialog, QDialogButtonBox, QSpinBox, QFileDialog, QSplashScreen,
                                 QListWidget, QListWidgetItem, QDoubleSpinBox, QSizePolicy, QCheckBox, QGroupBox, QTextEdit, QAction, QDateEdit)
    from PyQt5.QtCore import QThread, pyqtSignal, Qt, QSettings, QDate, QDateTime
    from PyQt5.QtGui import QFont, QColor, QPixmap, QIcon
    PYQT_AVAILABLE = True
    _PYQT_IMPORT_ERROR = None
except Exception as _e:
    PYQT_AVAILABLE = False
    _PYQT_IMPORT_ERROR = _e

    def _pyqt_unavailable(*_args, **_kwargs):
        raise ImportError("PyQt5 is required for the desktop GUI but is not installed.")

    class _PyQtBase:
        def __init__(self, *_args, **_kwargs):
            _pyqt_unavailable()

    # Minimal stubs to allow module import when PyQt5 is missing.
    QApplication = QMainWindow = QVBoxLayout = QHBoxLayout = QWidget = QPushButton = QLineEdit = _PyQtBase
    QTableWidget = QTableWidgetItem = QTabWidget = QLabel = QProgressBar = QMessageBox = _PyQtBase
    QComboBox = QScrollArea = QSlider = QDialog = QDialogButtonBox = QSpinBox = QFileDialog = _PyQtBase
    QSplashScreen = QListWidget = QListWidgetItem = QDoubleSpinBox = QSizePolicy = QCheckBox = _PyQtBase
    QGroupBox = QTextEdit = QAction = QDateEdit = _PyQtBase
    QThread = Qt = QSettings = QDate = QDateTime = _PyQtBase

    def pyqtSignal(*_args, **_kwargs):
        return None

    QFont = QColor = QPixmap = QIcon = _PyQtBase
import pandas as pd
import numpy as np
from data_sources import data_source_manager
from heatmap_matplotlib_interactive import create_interactive_matplotlib_heatmap, create_heatmap_controls
from favorable_cordes import compute_favorable_corde_horses
from race_stats_function import compute_race_statistics
from meeting_cache import get_cached_meetings, cache_meetings
import matplotlib
import random
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from collections import Counter
import itertools
import re
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.pdfgen import canvas
import requests
from bs4 import BeautifulSoup
import urllib.parse
from pathlib import Path

# =====================================================
# UTILITY FUNCTIONS
# =====================================================

def get_downloads_folder():
    """Get the path to the Downloads folder for the current user."""
    try:
        downloads_path = Path.home() / "Downloads"
        downloads_path.mkdir(parents=True, exist_ok=True)
        return downloads_path
    except Exception as e:
        print(f"[WARNING] Could not access Downloads folder: {e}, using current directory")
        return Path.cwd()

# =====================================================
# DATE UTILITIES
# =====================================================

def convert_date_to_french_url(date_input):
    """
    Convert a date/datetime/str value to French URL format.
    Example: date(2026, 1, 14) -> 'mercredi-14-janvier-2026'
    Handles invalid dates gracefully.
    Supports non-accented month names for URL compatibility.
    """
    try:
        if not date_input:
            return None

        # Normalize to a date-like object
        if isinstance(date_input, datetime):
            d = date_input.date()
        elif isinstance(date_input, date):
            d = date_input
        elif isinstance(date_input, str):
            # Expect YYYY-MM-DD
            try:
                d = datetime.strptime(date_input, "%Y-%m-%d").date()
            except Exception:
                return None
        else:
            # Duck-typing: year(), month(), day()
            try:
                y = int(date_input.year())
                m = int(date_input.month())
                d_ = int(date_input.day())
                d = date(y, m, d_)
            except Exception:
                return None

        # French months (non-accented for URL compatibility)
        french_months = {
            1: 'janvier', 2: 'fevrier', 3: 'mars', 4: 'avril',
            5: 'mai', 6: 'juin', 7: 'juillet', 8: 'aout',
            9: 'septembre', 10: 'octobre', 11: 'novembre', 12: 'decembre'
        }

        french_days = {
            0: 'lundi', 1: 'mardi', 2: 'mercredi', 3: 'jeudi',
            4: 'vendredi', 5: 'samedi', 6: 'dimanche'
        }

        day_of_week = d.weekday()  # Mon=0 .. Sun=6
        day_name = french_days.get(day_of_week)
        if not day_name:
            print(f"[WARNING] Invalid day of week: {day_of_week}")
            return None

        day_num = d.day
        month_num = d.month
        if month_num < 1 or month_num > 12:
            print(f"[WARNING] Invalid month: {month_num}")
            return None

        month_name = french_months[month_num]
        year = d.year

        return f"{day_name}-{day_num:02d}-{month_name}-{year}"
    except Exception as e:
        print(f"[ERROR] convert_date_to_french_url failed: {e}")
        return None

# =====================================================
# MEETING URL SCRAPER
# =====================================================

def scrape_meeting_urls(date_qdate=None, force_refresh=False):
    """
    Scrape available meeting URLs from zone-turf.fr programmes.
    If date_qdate is provided, scrapes for that specific date.
    Otherwise, scrapes for today's meetings.
    Returns a dictionary with meeting names as keys and URLs as values.
    Caches results to avoid repeated downloads (24-hour TTL).
    """
    def _normalize_cache_date(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return value
        # Duck-typing: year(), month(), day()
        try:
            y = int(value.year())
            m = int(value.month())
            d = int(value.day())
            return date(y, m, d)
        except Exception:
            return str(value)

    try:
        cache_date = _normalize_cache_date(date_qdate)
        # Check cache first (unless force_refresh is True)
        if not force_refresh and cache_date:
            cached_meetings, is_fresh = get_cached_meetings(cache_date)
            if cached_meetings and is_fresh:
                print(f"[INFO] Using cached meetings for {date_qdate}")
                return cached_meetings
        
        # Determine URL based on date parameter
        if date_qdate:
            french_date = convert_date_to_french_url(date_qdate)
            if not french_date:
                print("[WARNING] Could not convert date to French format, using default URL")
                url = 'https://zone-turf.fr/programmes'
            else:
                url = f'https://zone-turf.fr/programmes/{french_date}/'
        else:
            url = 'https://zone-turf.fr/programmes'
        
        print(f"[INFO] Fetching meetings from: {url}")
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the div containing the meeting list
        meetings = {}
        
        # Look for all li elements with class "pmu"
        meeting_items = soup.find_all('li', class_='pmu')
        
        if not meeting_items:
            print(f"[WARNING] No meetings found for date: {date_qdate}")
            return {}
        
        for item in meeting_items:
            try:
                link = item.find('a', href=True)
                if not link:
                    continue
                    
                href = link.get('href')
                if not href:
                    continue
                
                # Extract meeting info from span elements
                date_span = item.find('span', class_='date')
                reunion_span = item.find('span', class_='reunion')
                
                if date_span and reunion_span:
                    time_text = date_span.get_text(strip=True)
                    
                    # Get the race number and location
                    lieu_span = reunion_span.find('span', class_='lieu')
                    if lieu_span:
                        location = lieu_span.get_text(strip=True)
                        # Get race number (text before the lieu span)
                        race_text = reunion_span.get_text(strip=True).replace(location, '').strip()
                        
                        # Create a descriptive label: "R1 Vincennes 13:15"
                        label = f"{race_text} {location} {time_text}"
                        
                        # Make absolute URL
                        if href.startswith('/'):
                            full_url = urllib.parse.urljoin('https://zone-turf.fr', href)
                        else:
                            full_url = href
                        
                        meetings[label] = full_url
            except Exception as e:
                print(f"[DEBUG] Error parsing meeting item: {e}")
                continue
        
        print(f"[INFO] Successfully scraped {len(meetings)} meetings")
        
        # Cache the results if we have a date_qdate
        if cache_date and meetings:
            cache_meetings(cache_date, meetings)
            print(f"[INFO] Cached {len(meetings)} meetings for {date_qdate}")
        
        return meetings
    except requests.exceptions.Timeout:
        print("[ERROR] Request timeout when scraping meetings (server not responding)")
        return {}
    except requests.exceptions.ConnectionError as e:
        print(f"[ERROR] Connection error when scraping meetings: {e}")
        return {}
    except Exception as e:
        print(f"[ERROR] Unexpected error scraping meeting URLs: {e}")
        return {}

# =====================================================
# BETTING ANALYSIS FUNCTIONS
# =====================================================

def get_pdf_header_footer():
    """Get standard header and footer for all PDF documents."""
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    import os
    
    header_style = ParagraphStyle(
        'HeaderStyle',
        fontSize=10,
        textColor=colors.HexColor('#1e3a8a'),
        spaceAfter=3,
        alignment=TA_CENTER
    )
    
    footer_style = ParagraphStyle(
        'FooterStyle',
        fontSize=7,
        textColor=colors.grey,
        spaceAfter=0,
        alignment=TA_CENTER
    )
    
    # Header: RaceX branding
    header_text = "RaceX v1.0 - Advanced Betting Analysis for Horse Racing"
    
    # Footer: contact information
    footer_text = "RaceX v1.0 | Developed by Georges BODIONG | deebodiong@gmail.com | +226 74 91 15 38 / 60 35 44 00"
    
    # Get icon path if available
    icon_path = None
    assets_path = os.path.join(os.path.dirname(__file__), 'assets', 'race.png')
    if os.path.exists(assets_path):
        icon_path = assets_path
    
    return header_text, footer_text, header_style, footer_style, icon_path

def analyze_fitness_if(race_df):
    """
    Rank horses by IF (fitness indicator - lower is better).
    IF likely represents some form of fitness index.
    """
    try:
        if 'IF' not in race_df.columns:
            return pd.DataFrame()
        
        # Lower IF = better fitness
        # Use uppercase column names (CHEVAL, COTE)
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        # Find horse number column (N°, N, Numero, etc.)
        num_col = None
        for col in race_df.columns:
            if col in ('N°', 'N', 'Numero', 'NUM', 'Num'):
                num_col = col
                break
        if not num_col:
            num_col = 'N'  # Fallback
        if num_col not in race_df.columns:
            race_df = race_df.copy()
            race_df['N'] = range(1, len(race_df) + 1)
            num_col = 'N'
        analysis = race_df[[cheval_col, num_col, cote_col, 'IF']].copy()
        analysis.columns = ['Cheval', 'N', 'Cote', 'IF']
        analysis = analysis.dropna(subset=['IF'])
        analysis['IF'] = pd.to_numeric(analysis['IF'], errors='coerce')
        analysis = analysis.dropna(subset=['IF'])
        analysis = analysis.sort_values('IF')
        # Round numeric results
        try:
            analysis['IF'] = pd.to_numeric(analysis['IF'], errors='coerce').round(2)
        except Exception:
            pass
        return analysis
    except Exception as e:
        print(f"Error in analyze_fitness_if: {e}")
        return pd.DataFrame()

def analyze_class_ic(race_df):
    """
    Rank horses by IC (higher is better).
    IC = Poids (weight) - Cote (odds), higher suggests good value with decent weight.
    """
    try:
        if 'IC' not in race_df.columns:
            return pd.DataFrame()
        
        # Higher IC = better
        # Use uppercase column names (CHEVAL, COTE)
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        # Find horse number column
        num_col = None
        for col in race_df.columns:
            if col in ('N°', 'N', 'Numero', 'NUM', 'Num'):
                num_col = col
                break
        if not num_col:
            num_col = 'N'
        if num_col not in race_df.columns:
            race_df = race_df.copy()
            race_df['N'] = range(1, len(race_df) + 1)
            num_col = 'N'
        analysis = race_df[[cheval_col, num_col, cote_col, 'IC']].copy()
        analysis.columns = ['Cheval', 'N', 'Cote', 'IC']
        analysis = analysis.dropna(subset=['IC'])
        analysis['IC'] = pd.to_numeric(analysis['IC'], errors='coerce')
        analysis = analysis.dropna(subset=['IC'])
        analysis = analysis.sort_values('IC', ascending=False)
        try:
            analysis['IC'] = pd.to_numeric(analysis['IC'], errors='coerce').round(2)
        except Exception:
            pass
        return analysis
    except Exception as e:
        print(f"Error in analyze_class_ic: {e}")
        return pd.DataFrame()

def analyze_success_coeff(race_df):
    """
    Rank horses by S_COEFF or s_COEFF (success coefficient - higher is better).
    Indicates past racing success.
    """
    try:
        s_col = 'S_COEFF' if 'S_COEFF' in race_df.columns else ('s_COEFF' if 's_COEFF' in race_df.columns else None)
        if not s_col:
            return pd.DataFrame()
        
        # Higher S_COEFF = better success
        # Use uppercase column names (CHEVAL, COTE)
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        # Find horse number column
        num_col = None
        for col in race_df.columns:
            if col in ('N°', 'N', 'Numero', 'NUM', 'Num'):
                num_col = col
                break
        if not num_col:
            num_col = 'N'
        if num_col not in race_df.columns:
            race_df = race_df.copy()
            race_df['N'] = range(1, len(race_df) + 1)
            num_col = 'N'
        analysis = race_df[[cheval_col, num_col, cote_col, s_col]].copy()
        analysis.columns = ['Cheval', 'N', 'Cote', s_col]
        analysis = analysis.dropna(subset=[s_col])
        analysis[s_col] = pd.to_numeric(analysis[s_col], errors='coerce')
        analysis = analysis.dropna(subset=[s_col])
        analysis = analysis.sort_values(s_col, ascending=False)
        try:
            analysis[s_col] = pd.to_numeric(analysis[s_col], errors='coerce').round(2)
        except Exception:
            pass
        return analysis
    except Exception as e:
        print(f"Error in analyze_success_coeff: {e}")
        return pd.DataFrame()

def analyze_weight_stability(race_df):
    """
    Rank horses by N_WEIGHT (weight change from previous race).
    Stable or lower weight can indicate better form.
    """
    try:
        # detect n_weight column (either uppercase or lowercase)
        n_weight_col = 'N_WEIGHT' if 'N_WEIGHT' in race_df.columns else ('n_weight' if 'n_weight' in race_df.columns else None)
        if not n_weight_col:
            return pd.DataFrame()

        # detect cheval and cote variants
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        if not cheval_col or not cote_col:
            return pd.DataFrame()

        # detect number column (N° or N)
        num_col = 'N°' if 'N°' in race_df.columns else ('N' if 'N' in race_df.columns else None)

        # detect IC if present
        ic_col = 'IC' if 'IC' in race_df.columns else None

        cols = [cheval_col]
        if num_col:
            cols.append(num_col)
        cols.append(cote_col)
        cols.append(n_weight_col)
        if ic_col:
            cols.append(ic_col)

        analysis = race_df[cols].copy()

        # build canonical column names corresponding to selected columns
        canonical = ['Cheval']
        if num_col:
            canonical.append('N°')
        canonical.append('Cote')
        canonical.append('n_weight')
        if ic_col:
            canonical.append('IC')

        analysis.columns = canonical

        analysis = analysis.dropna(subset=['n_weight'])
        analysis['n_weight'] = pd.to_numeric(analysis['n_weight'], errors='coerce')
        analysis = analysis.dropna(subset=['n_weight'])

        # Sort by weight change (stable = close to 0, lower is surprise factor)
        analysis['Weight_Stability'] = abs(analysis['n_weight'])
        analysis = analysis.sort_values('Weight_Stability')
        try:
            analysis['n_weight'] = pd.to_numeric(analysis['n_weight'], errors='coerce').round(2)
            analysis['Weight_Stability'] = pd.to_numeric(analysis['Weight_Stability'], errors='coerce').round(2)
        except Exception:
            pass
        return analysis
    except Exception as e:
        print(f"Error in analyze_weight_stability: {e}")
        return pd.DataFrame()

def analyze_light_weight_surprise(race_df):
    """
    Rank horses carrying significantly less weight than others.
    Le poids leger peut etre un facteur de surprise pour gagner/placer.
    """
    try:
        # detect poids column
        poids_col = 'POIDS' if 'POIDS' in race_df.columns else ('Poids' if 'Poids' in race_df.columns else None)
        if not poids_col:
            return pd.DataFrame()

        # detect cheval, cote and n_weight variants
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        n_weight_col = 'N_WEIGHT' if 'N_WEIGHT' in race_df.columns else ('n_weight' if 'n_weight' in race_df.columns else None)

        if not cheval_col or not cote_col:
            return pd.DataFrame()

        # detect number column - handle various column name variants
        num_col = None
        for col in race_df.columns:
            if col in ('N°', 'N', 'Numero', 'NUM', 'Num'):
                num_col = col
                break
        
        cols = [cheval_col]
        if num_col:
            cols.append(num_col)
        cols.append(cote_col)
        cols.append(poids_col)
        if n_weight_col:
            cols.append(n_weight_col)

        analysis = race_df[cols].copy()

        # canonical names
        canonical = ['Cheval']
        if num_col:
            canonical.append('N')
        canonical.append('Cote')
        canonical.append('Poids')
        if n_weight_col:
            canonical.append('n_weight')

        analysis.columns = canonical

        analysis = analysis.dropna(subset=['Poids'])
        analysis['Poids'] = pd.to_numeric(analysis['Poids'], errors='coerce')
        analysis = analysis.dropna(subset=['Poids'])

        # Sort by weight (lighter is surprise)
        analysis = analysis.sort_values('Poids')
        try:
            analysis['Poids'] = pd.to_numeric(analysis['Poids'], errors='coerce').round(2)
            if 'n_weight' in analysis.columns:
                analysis['n_weight'] = pd.to_numeric(analysis['n_weight'], errors='coerce').round(2)
        except Exception:
            pass
        return analysis
    except Exception as e:
        print(f"Error in analyze_light_weight_surprise: {e}")
        return pd.DataFrame()


# =====================================================
# TROTTING-SPECIFIC ANALYSIS FUNCTIONS
# =====================================================

def analyze_trotting_fitness(race_df):
    """
    Analyze fitness for trotting races using FA/FM metrics.
    FA (Fitness Attelé) and FM (Fitness Monté) are computed from 'DERNIÈRES PERF.'
    LOWER values = BETTER fitness (1=excellent, 10=disqualified).
    Values computed via compute_d_perf() function.
    """
    try:
        from model_functions import compute_d_perf
        
        # Determine which fitness column exists (FA for attelé, FM for monté)
        fa_col = 'FA' if 'FA' in race_df.columns else None
        fm_col = 'FM' if 'FM' in race_df.columns else None
        perf_col = 'DERNIÈRES PERF.' if 'DERNIÈRES PERF.' in race_df.columns else None
        
        # Use whichever fitness column is available, or compute from DERNIÈRES PERF.
        fitness_col = fa_col if fa_col else fm_col
        disc = 'a' if fa_col else 'm'  # 'a' for attelé, 'm' for monté
        
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        num_col = 'N°' if 'N°' in race_df.columns else ('N' if 'N' in race_df.columns else None)
        
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        
        # Build analysis dataframe
        analysis_data = []
        for idx, row in race_df.iterrows():
            try:
                horse_num = row.get(num_col) if num_col else None
                horse_name = row.get(cheval_col)
                odds = row.get(cote_col)
                
                # Get fitness value from column if it exists
                if fitness_col and fitness_col in race_df.columns:
                    fitness_val = row.get(fitness_col)
                    if pd.notna(fitness_val):
                        fitness_val = pd.to_numeric(fitness_val, errors='coerce')
                        # Only include if valid and positive (0 means missing)
                        if pd.notna(fitness_val) and fitness_val > 0:
                            analysis_data.append({
                                'N°': horse_num,
                                'Cheval': horse_name,
                                'Cote': odds,
                                'Fitness': fitness_val
                            })
                # Otherwise compute from DERNIÈRES PERF.
                elif perf_col and perf_col in race_df.columns:
                    perf_str = row.get(perf_col)
                    if pd.notna(perf_str):
                        d_perf = compute_d_perf(str(perf_str))
                        fitness_val = d_perf.get(disc, 0)
                        if fitness_val and fitness_val > 0:
                            analysis_data.append({
                                'N°': horse_num,
                                'Cheval': horse_name,
                                'Cote': odds,
                                'Fitness': fitness_val
                            })
            except Exception as e:
                print(f"[DEBUG] Error processing row for fitness: {e}")
                continue
        
        if not analysis_data:
            return pd.DataFrame()
        
        analysis = pd.DataFrame(analysis_data)
        
        # Sort by fitness ASCENDING (lower = better)
        analysis = analysis.sort_values('Fitness', ascending=True)
        try:
            analysis['Fitness'] = pd.to_numeric(analysis['Fitness'], errors='coerce').round(2)
        except:
            pass
        return analysis
    except Exception as e:
        print(f"Error in analyze_trotting_fitness: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def analyze_trotting_performance(race_df):
    """
    Analyze success coefficient for trotting races.
    S_COEFF is computed from 'DERNIÈRES PERF.' column using success_coefficient function.
    HIGHER values = BETTER performance (higher % of good placements).
    """
    try:
        from model_functions import success_coefficient
        
        perf_col = 'DERNIÈRES PERF.' if 'DERNIÈRES PERF.' in race_df.columns else None
        rec_col = 'REC.' if 'REC.' in race_df.columns else None
        
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        num_col = 'N°' if 'N°' in race_df.columns else ('N' if 'N' in race_df.columns else None)
        
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        
        # Check if S_COEFF already computed in dataframe
        s_coeff_col = 'S_COEFF_norm' if 'S_COEFF_norm' in race_df.columns else ('S_COEFF' if 'S_COEFF' in race_df.columns else None)
        
        analysis_data = []
        for idx, row in race_df.iterrows():
            try:
                horse_num = row.get(num_col) if num_col else None
                horse_name = row.get(cheval_col)
                odds = row.get(cote_col)
                
                # Try to get S_COEFF from existing column first
                s_coeff = None
                if s_coeff_col and s_coeff_col in race_df.columns:
                    s_coeff = row.get(s_coeff_col)
                    if pd.notna(s_coeff):
                        s_coeff = pd.to_numeric(s_coeff, errors='coerce')
                
                # Otherwise compute from DERNIÈRES PERF.
                if pd.isna(s_coeff) and perf_col and perf_col in race_df.columns:
                    perf_str = row.get(perf_col)
                    if pd.notna(perf_str):
                        # Compute S_COEFF for attelé (discipline 'a')
                        s_coeff = success_coefficient(str(perf_str), 'a')
                
                # Only include if we have a valid S_COEFF value
                if pd.notna(s_coeff) and s_coeff > 0:
                    rec_val = row.get(rec_col) if rec_col and rec_col in race_df.columns else None
                    analysis_data.append({
                        'N°': horse_num,
                        'Cheval': horse_name,
                        'Cote': odds,
                        'S_COEFF': s_coeff,
                        'Recent': rec_val if rec_val else ''
                    })
            except Exception as e:
                print(f"[DEBUG] Error processing row for S_COEFF: {e}")
                continue
        
        if not analysis_data:
            return pd.DataFrame()
        
        analysis = pd.DataFrame(analysis_data)
        
        # Sort by S_COEFF DESCENDING (higher = better)
        analysis = analysis.sort_values('S_COEFF', ascending=False)
        try:
            analysis['S_COEFF'] = pd.to_numeric(analysis['S_COEFF'], errors='coerce').round(2)
        except:
            pass
        
        # Drop Recent column if empty
        if 'Recent' in analysis.columns and analysis['Recent'].isna().all():
            analysis = analysis.drop('Recent', axis=1)
        
        return analysis
    except Exception as e:
        print(f"Error in analyze_trotting_performance: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def analyze_trotting_trend(race_df):
    """
    Analyze trend and form trajectory using parse_performance_string.
    Extracts:
    - Recent average rank (last 3 races)
    - Trend slope (improving/declining/stable)
    - Number of races analyzed
    - Time-decay weighted average (recent races weighted more)
    
    LOWER avg_rank = BETTER form (1 is best).
    NEGATIVE slope = IMPROVING trend (ranks getting better/lower).
    POSITIVE slope = DECLINING trend (ranks getting worse/higher).
    Slope thresholds: < -0.3 = Improving, > 0.3 = Declining, else = Stable.
    """
    try:
        from model_functions import parse_performance_string
        
        perf_col = 'DERNIÈRES PERF.' if 'DERNIÈRES PERF.' in race_df.columns else None
        if not perf_col:
            return pd.DataFrame()
        
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        num_col = 'N°' if 'N°' in race_df.columns else ('N' if 'N' in race_df.columns else None)
        
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        
        analysis_data = []
        for idx, row in race_df.iterrows():
            try:
                horse_num = row.get(num_col) if num_col else None
                horse_name = row.get(cheval_col)
                # Append horse number to name for clarity
                if horse_num and pd.notna(horse_num):
                    horse_name = f"{horse_name} (N°{horse_num})"
                odds = row.get(cote_col)
                
                perf_str = row.get(perf_col)
                if pd.isna(perf_str):
                    continue
                
                # Parse performance string
                perf_metrics = parse_performance_string(str(perf_str))
                
                # Extract useful metrics
                if perf_metrics['recent_avg_rank'] is not None:
                    analysis_data.append({
                        'N°': horse_num,
                        'Cheval': horse_name,
                        'Cote': odds,
                        'Races': perf_metrics['num_races'],
                        'Recent_Avg': perf_metrics['recent_avg_rank'],
                        'Trend': perf_metrics['trend_label'],
                        'Slope': perf_metrics['trend_rank_slope'],
                        'Decay_Avg': perf_metrics['time_decay_avg_rank']
                    })
            except Exception as e:
                print(f"[DEBUG] Error processing row for trend: {e}")
                continue
        
        if not analysis_data:
            return pd.DataFrame()
        
        analysis = pd.DataFrame(analysis_data)
        
        # Sort by Slope ascending (negative = improving, lower is better)
        # Then by Recent_Avg as secondary sort
        analysis = analysis.sort_values(['Slope', 'Recent_Avg'], ascending=[True, True])
        try:
            analysis['Recent_Avg'] = pd.to_numeric(analysis['Recent_Avg'], errors='coerce').round(2)
            analysis['Decay_Avg'] = pd.to_numeric(analysis['Decay_Avg'], errors='coerce').round(2)
            analysis['Slope'] = pd.to_numeric(analysis['Slope'], errors='coerce').round(2)
            analysis['Races'] = pd.to_numeric(analysis['Races'], errors='coerce').astype(int)
        except:
            pass
        
        return analysis
    except Exception as e:
        print(f"Error in analyze_trotting_trend: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def analyze_trotting_shoeing(race_df):
    """
    Analyze shoeing strategy for trotting races.
    Shoeing affects grip and performance. Aggressive shoeing may indicate driver confidence.
    """
    try:
        # Prefer pre-parsed shoeing features if present (added by compute_trotting_metrics)
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        num_col = 'N°' if 'N°' in race_df.columns else ('N' if 'N' in race_df.columns else None)

        if not cheval_col or not cote_col:
            return pd.DataFrame()

        # If `shoeing_aggressiveness` exists, use it and other parsed features
        if 'shoeing_aggressiveness' in race_df.columns:
            cols = [cheval_col]
            if num_col:
                cols.append(num_col)
            cols.append(cote_col)
            feat_cols = [c for c in ['shoeing_aggressiveness', 'bare_front', 'bare_back', 'plate_front', 'plate_back', 'fully_bare', 'fully_plated', 'any_bare', 'any_plate'] if c in race_df.columns]
            cols.extend(feat_cols)

            analysis = race_df[cols].copy()
            canonical = ['Cheval']
            if num_col:
                canonical.append('N°')
            canonical.append('Cote')
            canonical.extend([c.replace('_', ' ').title() for c in feat_cols])
            analysis.columns = canonical

            # Normalize/ensure numeric
            if 'Shoeing Aggressiveness' in analysis.columns:
                analysis['Shoeing Aggressiveness'] = pd.to_numeric(analysis['Shoeing Aggressiveness'], errors='coerce')
            # Sort by aggressiveness descending
            if 'Shoeing Aggressiveness' in analysis.columns:
                analysis = analysis.sort_values('Shoeing Aggressiveness', ascending=False)
            return analysis.reset_index(drop=True)

        # Fallback: parse DEF. column on-the-fly
        def_col = 'DEF.' if 'DEF.' in race_df.columns else None
        if not def_col:
            return pd.DataFrame()

        cols = [cheval_col]
        if num_col:
            cols.append(num_col)
        cols.extend([cote_col, def_col])
        analysis = race_df[cols].copy()
        analysis.columns = ['Cheval'] + (['N°'] if num_col else []) + ['Cote', 'DEF']

        from model_functions import parse_shoeing_features
        shoe_feats = analysis['DEF'].apply(lambda x: parse_shoeing_features(x))
        shoe_df = pd.DataFrame(list(shoe_feats))
        combined = pd.concat([analysis.drop(columns=['DEF']), shoe_df], axis=1)

        # Rename and format
        if 'shoeing_aggressiveness' in combined.columns:
            combined = combined.rename(columns={'shoeing_aggressiveness': 'Shoeing Aggressiveness'})
            combined['Shoeing Aggressiveness'] = pd.to_numeric(combined['Shoeing Aggressiveness'], errors='coerce')
            combined = combined.sort_values('Shoeing Aggressiveness', ascending=False)

        return combined.reset_index(drop=True)
    except Exception as e:
        print(f"Error in analyze_trotting_shoeing: {e}")
        return pd.DataFrame()


def analyze_trotting_disqualification_risk(race_df):
    """
    Analyze disqualification risk for trotting races.
    Displays disqualification metrics that should already be computed in the dataframe.
    """
    try:
        # Minimal debug: report presence of disqualification-related columns
        disq_cols = [c for c in race_df.columns if 'disq' in c.lower() or 'dq' in c.lower() or 'disqual' in c.lower()]
        for candidate in ('disq_harness_rate', 'disq_mounted_rate', 'recent_disq', 'recent_dq'):
            if candidate in race_df.columns and candidate not in disq_cols:
                disq_cols.append(candidate)
        print(f"[DEBUG] analyze_trotting_disqualification_risk: found disq_cols={disq_cols}")
        if not disq_cols:
            return pd.DataFrame()

        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
        num_col = 'N°' if 'N°' in race_df.columns else ('N' if 'N' in race_df.columns else None)

        if not cheval_col:
            return pd.DataFrame()

        cols = [cheval_col]
        if num_col:
            cols.append(num_col)
        if cote_col:
            cols.append(cote_col)
        cols.extend(disq_cols)

        analysis = race_df[cols].copy()
        # Canonical column names for display
        canonical = ['Cheval']
        if num_col:
            canonical.append('N°')
        if cote_col:
            canonical.append('Cote')
        # Add disq field names normalized
        canonical.extend([f"raw_{c}" for c in disq_cols])
        analysis.columns = canonical
        
        # Convert numeric columns
        for c in analysis.columns:
            if c.startswith('raw_'):
                analysis[c] = pd.to_numeric(analysis[c], errors='coerce')
                analysis[c] = analysis[c].round(3)

        # Build a combined disqualification risk score (0..100 where higher means riskier)
        # Heuristic weights: recent count (if present) 0.6, mean rate 0.4. If only rates, average them.
        # Detect recent count column
        recent_col = None
        for c in analysis.columns:
            if 'recent' in c.lower() or 'count' in c.lower():
                recent_col = c
                break

        rate_cols = [c for c in analysis.columns if c.startswith('raw_') and c != recent_col]

        # Normalize each metric to 0..1 (higher = worse)
        norm = pd.DataFrame(index=analysis.index)
        for c in rate_cols:
            ser = analysis[c].copy()
            ser = pd.to_numeric(ser, errors='coerce')
            if ser.dropna().empty:
                norm[c] = 0.0
                continue
            mn = ser.min()
            mx = ser.max()
            if pd.isna(mn) or pd.isna(mx) or mx == mn:
                norm[c] = ser.fillna(ser.median()).apply(lambda x: 0.5)
            else:
                norm[c] = ((ser - mn) / (mx - mn)).fillna(0.5)

        recent_norm = None
        if recent_col is not None:
            ser = pd.to_numeric(analysis[recent_col], errors='coerce')
            if ser.dropna().empty:
                recent_norm = pd.Series(0.0, index=analysis.index)
            else:
                mn = ser.min()
                mx = ser.max()
                if pd.isna(mn) or pd.isna(mx) or mx == mn:
                    recent_norm = ser.fillna(ser.median()).apply(lambda x: 0.5)
                else:
                    recent_norm = ((ser - mn) / (mx - mn)).fillna(0.5)

        # Combine into final score
        if recent_norm is not None and not norm.empty:
            # both present
            mean_rate = norm.mean(axis=1)
            score = 0.6 * recent_norm + 0.4 * mean_rate
        elif not norm.empty:
            score = norm.mean(axis=1)
        elif recent_norm is not None:
            score = recent_norm
        else:
            score = pd.Series(0.0, index=analysis.index)

        analysis['DQ_Risk'] = (score * 100).round(2)

        # Prepare display columns
        display_cols = ['Cheval']
        if num_col:
            display_cols.append('N°')
        if cote_col:
            display_cols.append('Cote')
        # Include all raw disq columns in display
        display_cols.extend(rate_cols)  # Add all raw_* columns
        # include recent and mean rate if present
        if recent_col is not None:
            display_cols.append(recent_col)
        if not norm.empty:
            display_cols.append('Mean_Disq_Rate')
            analysis['Mean_Disq_Rate'] = norm.mean(axis=1).round(3)
        display_cols.append('DQ_Risk')

        result = analysis[display_cols].copy()
        # Sort by risk ascending (lower = safer)
        result = result.sort_values('DQ_Risk', ascending=True)
        return result
    except Exception as e:
        print(f"Error in analyze_trotting_disqualification_risk: {e}")
        return pd.DataFrame()


def generate_trotting_prognosis(race_df, max_len=8):
    """
    Generate trotting prognosis by analyzing all 7 metrics with weighted voting:
    - S_COEFF (Success Coefficient) - 20% weight
    - Fitness (FA/FM) - 15% weight
    - Slope (Form Trajectory) - 15% weight
    - DQ_Risk (Disqualification Risk) - 20% weight
    - COTE (Odds) - 15% weight
    - REC (Recent Races) - 10% weight
    - Shoeing_Agg (Shoeing Aggressiveness) - 5% weight
    
    Returns horse numbers sorted by weighted confidence score.
    Each horse's score = sum of weights for metrics where it appears in top-half.
    """
    try:
        if race_df is None or race_df.empty:
            return []
        
        # Get ranked DataFrames for all 7 metrics with their weights
        metric_rankings = [
            (analyze_trotting_performance(race_df), 0.20, "S_COEFF"),      # 20%
            (analyze_trotting_fitness(race_df), 0.15, "Fitness"),          # 15%
            (analyze_trotting_trend(race_df), 0.15, "Slope"),              # 15%
            (analyze_trotting_disqualification_risk(race_df), 0.20, "DQ_Risk"),  # 20%
            (analyze_trotting_shoeing(race_df), 0.05, "Shoeing"),          # 5%
        ]
        
        # Helper to extract top-half set and rank mapping
        def top_half_and_ranks(df):
            if df is None or df.empty:
                return set(), {}
            n = max(1, int(np.ceil(len(df) / 2)))
            top = df.head(n)
            # Try to find horse number column
            num_col = None
            for col in ['N°', 'N', 'Numero']:
                if col in top.columns:
                    num_col = col
                    break
            if not num_col:
                return set(), {}
            
            nums = []
            ranks = {}
            for i, raw in enumerate(top[num_col].tolist()):
                if raw is None:
                    continue
                val = str(raw).strip()
                if val.endswith('.0'):
                    val = val[:-2]
                if not val or val.lower() in ('nan', 'none') or val == '0':
                    continue
                if not val.isdigit():
                    cleaned = ''.join(ch for ch in val if ch.isdigit())
                    if not cleaned or cleaned == '0':
                        continue
                    val = cleaned
                nums.append(val)
                ranks[val] = i
            return set(nums), ranks
        
        # Extract all horse numbers from race
        all_nums = set()
        num_col_name = None
        for col in ['N°', 'N', 'Numero']:
            if col in race_df.columns:
                num_col_name = col
                for raw in race_df[col].tolist():
                    if raw is None:
                        continue
                    v = str(raw).strip()
                    if v.endswith('.0'):
                        v = v[:-2]
                    if not v or v.lower() in ('nan', 'none') or v == '0':
                        continue
                    if not v.isdigit():
                        v_clean = ''.join(ch for ch in v if ch.isdigit())
                        if not v_clean or v_clean == '0':
                            continue
                        v = v_clean
                    all_nums.add(v)
                break
        
        # Compute weighted confidence scores for each horse
        confidence_scores = {}
        best_ranks = {}
        
        for metric_df, weight, metric_name in metric_rankings:
            top_set, top_ranks = top_half_and_ranks(metric_df)
            for horse_num in top_set:
                if horse_num not in confidence_scores:
                    confidence_scores[horse_num] = 0.0
                    best_ranks[horse_num] = {}
                confidence_scores[horse_num] += weight
                best_ranks[horse_num][metric_name] = top_ranks.get(horse_num, 999)
        
        # Add COTE ranking (lower odds = better)
        try:
            cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
            if cote_col and num_col_name:
                cote_vals = pd.to_numeric(race_df[cote_col], errors='coerce')
                cote_df = pd.DataFrame({
                    'N°': race_df[num_col_name],
                    'Cote': cote_vals
                }).dropna(subset=['Cote']).sort_values('Cote', ascending=True).reset_index(drop=True)
                
                cote_set, cote_ranks = top_half_and_ranks(cote_df)
                for horse_num in cote_set:
                    if horse_num not in confidence_scores:
                        confidence_scores[horse_num] = 0.0
                        best_ranks[horse_num] = {}
                    confidence_scores[horse_num] += 0.15
                    best_ranks[horse_num]['COTE'] = cote_ranks.get(horse_num, 999)
        except Exception:
            pass
        
        # Add REC ranking (lower recent = better rested)
        try:
            rec_col = 'REC.' if 'REC.' in race_df.columns else ('Rec' if 'Rec' in race_df.columns else None)
            if rec_col and num_col_name:
                rec_vals = pd.to_numeric(race_df[rec_col], errors='coerce')
                rec_df = pd.DataFrame({
                    'N°': race_df[num_col_name],
                    'REC': rec_vals
                }).dropna(subset=['REC']).sort_values('REC', ascending=True).reset_index(drop=True)
                
                rec_set, rec_ranks = top_half_and_ranks(rec_df)
                for horse_num in rec_set:
                    if horse_num not in confidence_scores:
                        confidence_scores[horse_num] = 0.0
                        best_ranks[horse_num] = {}
                    confidence_scores[horse_num] += 0.10
                    best_ranks[horse_num]['REC'] = rec_ranks.get(horse_num, 999)
        except Exception:
            pass
        
        # Sort by confidence score (descending), then by average rank position
        def score_key(num):
            score = confidence_scores.get(num, 0)
            avg_rank = sum(best_ranks.get(num, {}).values()) / max(1, len(best_ranks.get(num, {}))) if best_ranks.get(num) else 999
            try:
                horse_num = int(num) if num.isdigit() else num
            except:
                horse_num = num
            return (-score, avg_rank, horse_num)
        
        result = sorted(confidence_scores.keys(), key=score_key)
        return result[:max_len]
    except Exception as e:
        print(f"Error generating trotting prognosis: {e}")
        import traceback
        traceback.print_exc()
        return []


def analyze_trotting_summary_prognosis(race_df, composite_df=None):
    """
    Build a comprehensive Summary & Prognosis tab for trotting races.
    Returns a DataFrame with summary insights, predictions, and exclusions.
    """
    try:
        if race_df.empty or composite_df is None or composite_df.empty:
            return pd.DataFrame()
        
        # Helper to find exact column name (case-insensitive)
        def find_col(df, *candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            return None
        
        cheval_col = find_col(race_df, 'CHEVAL', 'Cheval', 'Horse')
        num_col = find_col(race_df, 'N°', 'N', 'Numero', 'NUM')
        cote_col = find_col(composite_df, 'COTE', 'Cote')
        comp_col = find_col(composite_df, 'Composite', 'composite')
        
        if not (cheval_col and comp_col):
            return pd.DataFrame()
        
        result = []
        
        # Section 1: SUMMARY (top 3 contenders + race overview)
        sorted_comp = composite_df.nlargest(3, comp_col).copy()
        
        for rank, (idx, horse) in enumerate(sorted_comp.iterrows(), 1):
            comp_score = horse.get(comp_col, 0)
            odds = horse.get(cote_col, '-')
            cheval = horse.get(cheval_col, '?')
            h_num = horse.get(num_col, '-') if num_col else '-'
            # Append horse number to name for clarity
            if h_num and h_num != '-':
                cheval = f"{cheval} (N°{h_num})"
            
            # Determine verdict based on score and odds
            if comp_score >= 0.75:
                verdict = "⭐ Strong Contender"
            elif comp_score >= 0.6:
                verdict = "✓ Good Chance"
            else:
                verdict = "▪ Moderate Chance"
            
            result.append({
                'Position': f"#{rank}",
                'Horse': cheval,
                'N°': h_num if h_num != '-' else '',
                'Composite': round(comp_score, 3),
                'Odds': f"{odds}" if pd.notna(odds) else '-',
                'Status': verdict,
                'Type': 'SUMMARY'
            })
        
        # Section 2: PROGNOSIS (predictions using trotting-specific multi-source analysis)
        # Display 8 horses if race has > 10 starters, otherwise use half the field (min 3)
        race_size = len(race_df)
        if race_size > 10:
            prognosis_count = 8
        else:
            prognosis_count = max(3, int(np.ceil(race_size / 2)))
        prog_horses = generate_trotting_prognosis(race_df, max_len=prognosis_count)
        
        for rank, horse_num in enumerate(prog_horses, 1):
            # Find horse in composite_df
            match = None
            for idx, row in composite_df.iterrows():
                row_num = row.get(num_col, '?')
                if row_num and str(row_num).strip().endswith('.0'):
                    row_num = str(row_num)[:-2]
                if row_num and str(row_num).strip() == str(horse_num).strip():
                    match = row
                    break
            
            if match is None:
                continue
            
            comp_score = match.get(comp_col, 0)
            cheval = match.get(cheval_col, '?')
            h_num = str(horse_num)
            # Append horse number to name for clarity
            if h_num and h_num != '-':
                cheval = f"{cheval} (N°{h_num})"
            odds = match.get(cote_col, '-')
            
            # Confidence level based on composite score
            if comp_score >= 0.8:
                confidence = "Very High"
                emoji = "🌟"
            elif comp_score >= 0.65:
                confidence = "High"
                emoji = "✨"
            elif comp_score >= 0.5:
                confidence = "Moderate"
                emoji = "▪"
            else:
                confidence = "Low"
                emoji = "◯"
            
            result.append({
                'Position': f"{emoji}",
                'Horse': cheval,
                'N°': h_num if h_num != '-' else '',
                'Composite': round(comp_score, 3),
                'Odds': f"{odds}" if pd.notna(odds) else '-',
                'Status': confidence,
                'Type': 'PROGNOSIS'
            })
        
        # Section 3: EXCLUSIVES (horses to avoid/consider)
        # Lowest scoring + highest disqualification risk
        excl_df = composite_df.nsmallest(3, comp_col).copy()
        
        for idx, horse in excl_df.iterrows():
            comp_score = horse.get(comp_col, 0)
            cheval = horse.get(cheval_col, '?')
            h_num = horse.get(num_col, '-') if num_col else '-'
            # Append horse number to name for clarity
            if h_num and h_num != '-':
                cheval = f"{cheval} (N°{h_num})"
            odds = horse.get(cote_col, '-')
            
            reason = "Low Composite Score"
            if comp_score == 0:
                reason = "⚠️ Likely Disqualified"
            
            result.append({
                'Position': '❌',
                'Horse': cheval,
                'N°': h_num if h_num != '-' else '',
                'Composite': round(comp_score, 3),
                'Odds': f"{odds}" if pd.notna(odds) else '-',
                'Status': reason,
                'Type': 'EXCLUSIVES'
            })
        
        if not result:
            return pd.DataFrame()
        
        summary_df = pd.DataFrame(result)
        return summary_df
    
    except Exception as e:
        print(f"[ERROR] analyze_trotting_summary_prognosis: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def compute_trotting_summary_horses(composite_df, max_len=None):
    """
    Extract horse numbers (N°) from the top-half of trotting composite score rankings.
    Returns a list of N° values sorted by descending composite score (best first).
    Similar to flat race `compute_summary_horses()` but optimized for trotting.
    """
    try:
        if composite_df is None or composite_df.empty:
            return []
        
        # Sort by Composite score descending (best first)
        sorted_df = composite_df.sort_values('Composite', ascending=False)
        n = max(1, int(np.ceil(len(sorted_df) / 2)))
        
        # Get the cutoff score (the score of the n-th horse)
        cutoff_score = sorted_df.iloc[n-1]['Composite']
        
        # Include ALL horses with score >= cutoff_score (handles ties fairly)
        top_half = sorted_df[sorted_df['Composite'] >= cutoff_score]
        
        # Extract N° values, sanitized
        result = []
        for col in ['N°', 'N', 'Numero']:
            if col in top_half.columns:
                for raw in top_half[col].tolist():
                    if raw is None:
                        continue
                    v = str(raw).strip()
                    if v.endswith('.0'):
                        v = v[:-2]
                    if not v or v.lower() in ('nan', 'none'):
                        continue
                    if not v.isdigit():
                        v_clean = ''.join(ch for ch in v if ch.isdigit())
                        if not v_clean:
                            continue
                        v = v_clean
                    if v == '0':
                        continue
                    if v not in result:
                        result.append(v)
        return result
    except Exception as e:
        print(f"Error computing trotting summary horses: {e}")
        return []


def generate_trotting_bets(race_df, composite_df, base_horses=None, desired_size=5, max_combos=100, manual_base=''):
    """
    Generate betting combinations for trotting races.
    Combines trotting prognosis, summary horses, and multi-source priority ranking.
    
    Args:
        race_df: DataFrame with trotting race data
        composite_df: DataFrame with composite scores (sorted by ranking)
        base_horses: List of mandatory horse numbers (optional)
        desired_size: Horses per combination (default 5)
        max_combos: Maximum combinations to generate
        manual_base: Manual comma/semicolon-separated horse string (overrides base_horses)
    
    Returns:
        List of tuples (combinations) or empty list if generation failed
    """
    try:
        import random
        
        # Build BASE (mandatory horses)
        if manual_base and manual_base.strip():
            parts = [p.strip() for p in manual_base.replace(';', ',').split(',') if p.strip()]
            base = []
            for p in parts:
                if p.endswith('.0'):
                    p = p[:-2]
                if p.isdigit() and p != '0':
                    base.append(p)
        elif base_horses:
            base = [str(h) for h in base_horses if str(h).isdigit() and str(h) != '0']
        else:
            # Use top 2 from prognosis as default base
            prog = generate_trotting_prognosis(race_df, max_len=3)
            base = prog[:2] if len(prog) >= 2 else prog
        
        # Build ASSOCIATES (optional pool)
        associates = []
        if composite_df is not None and not composite_df.empty:
            associates = compute_trotting_summary_horses(composite_df)
        
        # Remove base from associates
        associates = [a for a in associates if a not in base]
        
        # Generate combinations using reducing_system
        combos = []
        if len(base) >= desired_size:
            # Base already complete
            combos = [tuple(base[:desired_size])]
        else:
            # Need to fill from associates
            combos = reducing_system(base, associates, desired_size)
        
        # If still empty, try alternative: take first N from composite ranking
        if not combos and composite_df is not None and not composite_df.empty:
            top_horses = compute_trotting_summary_horses(composite_df, max_len=desired_size)
            if top_horses:
                combos = [tuple(top_horses[:desired_size])]
        
        # Deduplicate
        seen = set()
        unique = []
        for c in combos:
            canon = tuple(sorted(c))
            if canon not in seen:
                seen.add(canon)
                unique.append(tuple(c))
        
        return unique[:max_combos]
    except Exception as e:
        print(f"Error generating trotting bets: {e}")
        import traceback
        traceback.print_exc()
        return []


def analyze_odds_divergence(race_df, composite_df):
    """
    Identify outsiders: horses with high composite scores but high odds (undervalued).
    Calculates divergence between odds rank and composite rank.
    
    Higher divergence = better outsider potential (high composite, low odds preference).
    """
    try:
        if composite_df is None or composite_df.empty:
            return pd.DataFrame()
        
        # Rank by composite score (1 = best)
        composite_ranked = composite_df.copy()
        composite_ranked['composite_rank'] = composite_ranked['Composite'].rank(method='min', ascending=False)
        
        # Rank by odds (1 = lowest odds = most favored)
        # Handle COTE/Cote variants
        cote_col = 'COTE' if 'COTE' in composite_ranked.columns else ('Cote' if 'Cote' in composite_ranked.columns else None)
        if not cote_col:
            return pd.DataFrame()
        
        composite_ranked['odds_rank'] = pd.to_numeric(composite_ranked[cote_col], errors='coerce').rank(method='min', ascending=True)
        
        # Calculate divergence (positive = outsider: good composite but high odds)
        composite_ranked['divergence'] = composite_ranked['odds_rank'] - composite_ranked['composite_rank']
        
        # Build result table
        cheval_col = 'CHEVAL' if 'CHEVAL' in composite_ranked.columns else ('Cheval' if 'Cheval' in composite_ranked.columns else None)
        # Find horse number column - N° is the correct name
        num_col = None
        for col in ['N°', 'N', 'Numero', 'NUM', 'Num']:
            if col in composite_ranked.columns:
                num_col = col
                print(f"[DEBUG] analyze_odds_divergence: Found horse number column: {repr(col)}")
                break
        
        if not cheval_col or not num_col:
            print(f"[DEBUG] analyze_odds_divergence: Missing columns. cheval_col={cheval_col}, num_col={num_col}")
            print(f"[DEBUG] Available columns: {list(composite_ranked.columns)}")
            return pd.DataFrame()
        
        result = composite_ranked[[num_col, cheval_col, cote_col, 'Composite', 'composite_rank', 'odds_rank', 'divergence']].copy()
        result.columns = ['N', 'Cheval', 'Cote', 'Composite', 'Rank Composite', 'Rank Odds', 'Divergence']
        
        # Sort by divergence descending (highest first)
        result = result.sort_values('Divergence', ascending=False)
        
        # Filter to show only horses with positive divergence (outsiders)
        result = result[result['Divergence'] > 0]
        
        # Round numeric columns
        for col in ['Cote', 'Composite', 'Divergence']:
            try:
                result[col] = pd.to_numeric(result[col], errors='coerce').round(2)
            except:
                pass
        
        for col in ['Rank Composite', 'Rank Odds']:
            try:
                result[col] = result[col].astype(int)
            except:
                pass
        
        return result
    except Exception as e:
        print(f"Error in analyze_odds_divergence: {e}")
        return pd.DataFrame()


def analyze_consistency_score(race_df, composite_df):
    """
    Score horses on how many favorable conditions they meet.
    High consistency = multiple favorable factors = potential upset winner.
    
    Factors considered:
    - In top half of composite ranking (1 point)
    - In favorable cordes (1 point)
    - Light weight (lower Poids) (1 point)
    - Good fitness (lower IF) (1 point)
    - Good success coefficient (higher S_COEFF) (1 point)
    - Stable or improving weight (lower n_weight) (1 point)
    
    Returns DataFrame sorted by consistency score descending.
    """
    try:
        if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
            return pd.DataFrame()
        
        # Initialize consistency score
        consistency_data = composite_df.copy()
        consistency_data['consistency_score'] = 0
        
        # Get column variants
        cheval_col = 'CHEVAL' if 'CHEVAL' in consistency_data.columns else ('Cheval' if 'Cheval' in consistency_data.columns else None)
        num_col = 'N°' if 'N°' in consistency_data.columns else 'N'
        cote_col = 'COTE' if 'COTE' in consistency_data.columns else ('Cote' if 'Cote' in consistency_data.columns else None)
        
        if not cheval_col or not cote_col:
            return pd.DataFrame()
        
        # Factor 1: Top half of composite ranking
        median_composite = consistency_data['Composite'].median()
        consistency_data.loc[consistency_data['Composite'] >= median_composite, 'consistency_score'] += 1
        
        # Factor 2: Light weight (lower Poids)
        if 'Poids' in consistency_data.columns:
            poids_col_data = pd.to_numeric(consistency_data['Poids'], errors='coerce')
            poids_median = poids_col_data.median()
            consistency_data.loc[poids_col_data <= poids_median, 'consistency_score'] += 1
        
        # Factor 3: Good fitness (lower IF)
        if 'IF' in consistency_data.columns:
            if_col_data = pd.to_numeric(consistency_data['IF'], errors='coerce')
            if_median = if_col_data.median()
            consistency_data.loc[if_col_data <= if_median, 'consistency_score'] += 1
        
        # Factor 4: Good success coefficient (higher S_COEFF)
        s_coeff_col = 'S_COEFF' if 'S_COEFF' in consistency_data.columns else ('s_COEFF' if 's_COEFF' in consistency_data.columns else None)
        if s_coeff_col:
            s_coeff_data = pd.to_numeric(consistency_data[s_coeff_col], errors='coerce')
            s_coeff_median = s_coeff_data.median()
            consistency_data.loc[s_coeff_data >= s_coeff_median, 'consistency_score'] += 1
        
        # Factor 5: Stable/improving weight (lower n_weight absolute value)
        n_weight_col = 'N_WEIGHT' if 'N_WEIGHT' in consistency_data.columns else ('n_weight' if 'n_weight' in consistency_data.columns else None)
        if n_weight_col:
            n_weight_data = pd.to_numeric(consistency_data[n_weight_col], errors='coerce')
            # Use absolute value to measure stability (close to 0 = stable)
            n_weight_stability = n_weight_data.abs()
            n_weight_stability_median = n_weight_stability.median()
            consistency_data.loc[n_weight_stability <= n_weight_stability_median, 'consistency_score'] += 1
        
        # Factor 6: Class indicator (IC) - higher is better
        if 'IC' in consistency_data.columns:
            ic_col_data = pd.to_numeric(consistency_data['IC'], errors='coerce')
            ic_median = ic_col_data.median()
            consistency_data.loc[ic_col_data >= ic_median, 'consistency_score'] += 1
        
        # Build result table
        # Find horse number column - N° is the correct name
        num_col_actual = None
        for col in ['N°', 'N', 'Numero', 'NUM', 'Num']:
            if col in consistency_data.columns:
                num_col_actual = col
                print(f"[DEBUG] analyze_consistency_score: Found horse number column: {repr(col)}")
                break
        
        if not num_col_actual:
            print(f"[DEBUG] analyze_consistency_score: Missing horse number column")
            print(f"[DEBUG] Available columns: {list(consistency_data.columns)}")
            return pd.DataFrame()
        
        result = consistency_data[[num_col_actual, cheval_col, cote_col, 'Composite', 'consistency_score']].copy()
        result.columns = ['N', 'Cheval', 'Cote', 'Composite', 'Consistance']
        
        # Sort by consistency score descending
        result = result.sort_values('Consistance', ascending=False)
        
        # Round numeric columns
        try:
            result['Cote'] = pd.to_numeric(result['Cote'], errors='coerce').round(2)
            result['Composite'] = pd.to_numeric(result['Composite'], errors='coerce').round(2)
            result['Consistance'] = result['Consistance'].astype(int)
        except:
            pass
        
        return result
    except Exception as e:
        print(f"Error in analyze_consistency_score: {e}")
        return pd.DataFrame()


def analyze_underperforming_favorites(race_df, composite_df):
    """
    Identify market favorites with low composite scores (bottom 5).
    These are horses the market favors (low odds < 8) but our model ranks poorly.
    
    Useful for:
    - Identifying potentially rigged races
    - Finding overvalued betting opportunities
    - Spotting anomalies in market consensus vs fundamental metrics
    """
    try:
        if composite_df is None or composite_df.empty:
            return pd.DataFrame()
        
        # Get odds column
        cote_col = 'COTE' if 'COTE' in composite_df.columns else ('Cote' if 'Cote' in composite_df.columns else None)
        if not cote_col:
            return pd.DataFrame()
        
        # Identify favorites (odds < 10)
        composite_data = composite_df.copy()
        composite_data['cote_numeric'] = pd.to_numeric(composite_data[cote_col], errors='coerce')
        favorites = composite_data[composite_data['cote_numeric'] < 10].copy()
        
        if favorites.empty:
            return pd.DataFrame()
        
        # Find bottom 5 by composite score
        bottom_5_threshold = composite_data['Composite'].nsmallest(5).max()
        underperformers = favorites[favorites['Composite'] <= bottom_5_threshold].copy()
        
        if underperformers.empty:
            return pd.DataFrame()
        
        # Build result table
        cheval_col = 'CHEVAL' if 'CHEVAL' in underperformers.columns else ('Cheval' if 'Cheval' in underperformers.columns else None)
        # Find horse number column - N° is the correct name
        num_col_actual = None
        for col in ['N°', 'N', 'Numero', 'NUM', 'Num']:
            if col in underperformers.columns:
                num_col_actual = col
                print(f"[DEBUG] analyze_underperforming_favorites: Found horse number column: {repr(col)}")
                break
        
        if not cheval_col or not num_col_actual:
            print(f"[DEBUG] analyze_underperforming_favorites: Missing columns. cheval_col={cheval_col}, num_col={num_col_actual}")
            print(f"[DEBUG] Available columns: {list(underperformers.columns)}")
            return pd.DataFrame()
        
        result = underperformers[[num_col_actual, cheval_col, cote_col, 'Composite']].copy()
        result.columns = ['N', 'Cheval', 'Cote', 'Composite']
        
        # Add rank column
        result['Rang Composite'] = composite_data['Composite'].rank(method='min', ascending=True).reindex(result.index).astype(int)
        
        # Sort by composite score (worst first)
        result = result.sort_values('Composite', ascending=True)
        
        # Round numeric columns
        try:
            result['Cote'] = pd.to_numeric(result['Cote'], errors='coerce').round(2)
            result['Composite'] = result['Composite'].round(2)
        except:
            pass
        
        return result
    except Exception as e:
        print(f"Error in analyze_underperforming_favorites: {e}")
        return pd.DataFrame()


def compute_summary_horses(composite_df, max_len=None):
    """
    Extract horse numbers (N°) from the first-half of composite score rankings.
    Returns a list of N° values sorted by descending composite score (best first).
    
    Note: If multiple horses tie at the cutoff score, ALL tied horses are included
    to avoid arbitrarily excluding horses with equal strength.
    """
    try:
        if composite_df is None or composite_df.empty:
            return []
        
        # Sort by Composite score descending (best first)
        sorted_df = composite_df.sort_values('Composite', ascending=False)
        n = max(1, int(np.ceil(len(sorted_df) / 2)))
        
        # Get the cutoff score (the score of the n-th horse)
        cutoff_score = sorted_df.iloc[n-1]['Composite']
        
        # Include ALL horses with score >= cutoff_score (handles ties fairly)
        top_half = sorted_df[sorted_df['Composite'] >= cutoff_score]
        
        # Extract N° values, sanitized - prioritize N° over other variants
        result = []
        for col in ['N°', 'N', 'Numero']:
            if col in top_half.columns:
                for raw in top_half[col].tolist():
                    if raw is None:
                        continue
                    v = str(raw).strip()
                    if v.endswith('.0'):
                        v = v[:-2]
                    if not v or v.lower() in ('nan', 'none'):
                        continue
                    if not v.isdigit():
                        v_clean = ''.join(ch for ch in v if ch.isdigit())
                        if not v_clean:
                            continue
                        v = v_clean
                    if v == '0':
                        continue
                    if v not in result:
                        result.append(v)
        return result
    except Exception as e:
        print(f"Error computing summary horses: {e}")
        import traceback
        traceback.print_exc()
        return []


def sanitize_horse_list(items):
    """
    Ensure list of horse identifiers contains only non-zero numeric strings.
    Preserves order and removes duplicates.
    """
    if not items:
        return []
    out = []
    seen = set()
    for raw in items:
        try:
            if raw is None:
                continue
            v = str(raw).strip()
            if v.endswith('.0'):
                v = v[:-2]
            if not v or v.lower() in ('nan', 'none'):
                continue
            # keep only digits
            if not v.isdigit():
                v_clean = ''.join(ch for ch in v if ch.isdigit())
                if not v_clean:
                    continue
                v = v_clean
            if v == '0':
                continue
            if v not in seen:
                seen.add(v)
                out.append(v)
        except Exception:
            continue
    return out


def compute_prognosis(race_df, max_len=8):
    """
    Compute an automated prognosis list (up to max_len) of horse numbers ('N°') based on:
    - First half of rankings from fitness (IF), class (IC), and success coefficient (S_COEFF).
    - Recurring horses present in all three top-halves (high priority).
    - Horses present in two of three top-halves (next priority).
    - Surprise duos/individuals appearing only in a single category (included afterward).

    Returns an ordered list of up to max_len values (as strings) representing 'N°'.
    """
    try:
        if race_df is None or race_df.empty:
            return []

        # Get the three ranked DataFrames
        f_df = analyze_fitness_if(race_df)
        c_df = analyze_class_ic(race_df)
        s_df = analyze_success_coeff(race_df)

        # helper to extract top-half set and rank mapping (sanitizes numbers)
        def top_half_and_ranks(df):
            if df is None or df.empty:
                return set(), {}
            n = max(1, int(np.ceil(len(df) / 2)))
            top = df.head(n)
            # preference column for number: try 'N°' then 'N'
            num_col = 'N°' if 'N°' in top.columns else ('N' if 'N' in top.columns else None)
            nums = []
            ranks = {}
            if num_col:
                for i, raw in enumerate(top[num_col].tolist()):
                    val = '' if raw is None else str(raw).strip()
                    # normalize floats like '1.0' -> '1'
                    if val.endswith('.0'):
                        val = val[:-2]
                    # skip empty, nan, none, or '0'
                    if not val or val.lower() in ('nan', 'none'):
                        continue
                    if not val.isdigit():
                        # allow simple numeric-like with leading zeros removed
                        cleaned = ''.join(ch for ch in val if ch.isdigit())
                        if cleaned == '' or cleaned == '0':
                            continue
                        val = cleaned
                    if val == '0':
                        continue
                    nums.append(val)
                    ranks[val] = i
            return set(nums), ranks

        s1, r1 = top_half_and_ranks(f_df)
        s2, r2 = top_half_and_ranks(c_df)
        s3, r3 = top_half_and_ranks(s_df)

        # All candidate horses present anywhere in the race (sanitized)
        all_nums = set()
        for col in ['N°', 'N', 'Numero']:
            if col in race_df.columns:
                for raw in race_df[col].tolist():
                    if raw is None:
                        continue
                    v = str(raw).strip()
                    if v.endswith('.0'):
                        v = v[:-2]
                    if not v or v.lower() in ('nan', 'none'):
                        continue
                    # keep only numeric part
                    if not v.isdigit():
                        v_clean = ''.join(ch for ch in v if ch.isdigit())
                        if not v_clean:
                            continue
                        v = v_clean
                    if v == '0':
                        continue
                    all_nums.add(v)

        # Count presence across the three top-half sets
        presence = {}
        rank_sum = {}
        for num in all_nums:
            cnt = 0
            rs = 0
            if num in s1:
                cnt += 1
                rs += r1.get(num, 0)
            if num in s2:
                cnt += 1
                rs += r2.get(num, 0)
            if num in s3:
                cnt += 1
                rs += r3.get(num, 0)
            presence[num] = cnt
            rank_sum[num] = rs

        # Priority buckets
        p3 = [n for n, v in presence.items() if v == 3]
        p2 = [n for n, v in presence.items() if v == 2]
        p1 = [n for n, v in presence.items() if v == 1]

        # Sort each bucket by best (lowest) rank_sum then numeric N° if tie
        def sort_bucket(lst):
            try:
                return sorted(lst, key=lambda x: (rank_sum.get(x, 999), int(x) if x.isdigit() else x))
            except Exception:
                return sorted(lst, key=lambda x: rank_sum.get(x, 999))

        result = []
        for bucket in (sort_bucket(p3) + sort_bucket(p2)):
            for n in bucket:
                if n not in result:
                    result.append(n)
                if len(result) >= max_len:
                    return result[:max_len]

        # Surprise duos: pairs that appear together in exactly one of the rankings' top-halves
        from itertools import combinations
        pair_counts = {}
        for s in (s1, s2, s3):
            for a, b in combinations(sorted(list(s)), 2):
                pair = tuple(sorted((a, b)))
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

        surprise_pairs = [p for p, c in pair_counts.items() if c == 1]
        # sort surprise pairs by summed rank in the ranking where they occur (approx via rank_sum)
        surprise_pairs = sorted(surprise_pairs, key=lambda pr: (rank_sum.get(pr[0], 999) + rank_sum.get(pr[1], 999)))

        for a, b in surprise_pairs:
            for n in (a, b):
                if n not in result:
                    result.append(n)
                if len(result) >= max_len:
                    return result[:max_len]

        # Fill with single-category surprises (best ranked within p1)
        for n in sort_bucket(p1):
            if n not in result:
                result.append(n)
            if len(result) >= max_len:
                break

        return result[:max_len]
    except Exception as e:
        print(f"Error computing prognosis: {e}")
        return []


def compute_trending_horses(race_df, top_n=3, min_tokens=2):
    """
    Detect trending horses by parsing odds-evolution strings from specific columns.
    Looks for space-separated odds like "112.0 23.1 17.7" or "11 11".
    Returns a list of formatted strings like 'HorseName (inite?last, -XX%)'
    Criteria: must have at least two tokens in evolution, initial rank not among early favorites.
    """
    try:
        if race_df is None or race_df.empty:
            print("[DEBUG trending] race_df is None or empty")
            return []

        # Focus on likely columns for odds evolution. Try post-rename names first,
        # then fall back to original Unnamed columns and any column that contains 'Unnamed'.
        priority_cols = ['PMU', 'PMU_FR', 'Unnamed: 13']
        candidate_cols = [c for c in priority_cols if c in race_df.columns]
        # fallback to other variants
        if not candidate_cols:
            for alt in ['Unnamed: 13', 'Unnamed: 12', 'Unnamed: 11']:
                if alt in race_df.columns and alt not in candidate_cols:
                    candidate_cols.append(alt)
        # also include any column whose name contains 'unnamed' (case-insensitive)
        if not candidate_cols:
            candidate_cols = [c for c in race_df.columns if 'unnamed' in str(c).lower()]

        print(f"[DEBUG trending] Available evolution columns (after fallbacks): {candidate_cols}")

        evol_map = {}  # idx -> (cheval, [floats])

        for idx, row in race_df.iterrows():
            best_tokens = []
            # Prefer uppercase CHEVAL, fallback to Cheval, else use index
            cheval = None
            for cand in ('CHEVAL', 'Cheval'):
                try:
                    val = row.get(cand)
                    if pd.notna(val):
                        cheval = val
                        break
                except Exception:
                    continue
            if cheval is None:
                cheval = str(idx)
            
            # Try candidate columns in order
            for col in candidate_cols:
                try:
                    val = row.get(col)
                    if pd.isna(val):
                        continue
                    s = str(val).strip()
                    if not s:
                        continue
                    # Parse space-separated numeric tokens like "112.0 23.1 17.7" or "11 11"
                    tokens = s.split()
                    # Extract floats from each token
                    tokens_f = []
                    for t in tokens:
                        match = re.match(r'^(\d+\.?\d*)$', t.strip())
                        if match:
                            tokens_f.append(float(match.group(1)))
                    
                    if len(tokens_f) >= min_tokens:
                        best_tokens = tokens_f
                        print(f"[DEBUG trending] {cheval} ({col}): {tokens_f}")
                        break  # Use first column with enough tokens
                except Exception as e:
                    continue

            # If we didn't find evolution in candidate cols, try scanning all string columns for tokens
            if not best_tokens:
                for col in race_df.columns:
                    try:
                        if col in candidate_cols:
                            continue
                        val = row.get(col)
                        if pd.isna(val):
                            continue
                        s = str(val).strip()
                        if not s:
                            continue
                        tokens = s.split()
                        tokens_f = []
                        for t in tokens:
                            m = re.match(r'^(\d+\.?\d*)$', t.strip())
                            if m:
                                tokens_f.append(float(m.group(1)))
                        if len(tokens_f) >= min_tokens:
                            best_tokens = tokens_f
                            print(f"[DEBUG trending] Fallback {cheval} ({col}): {tokens_f}")
                            break
                    except Exception:
                        continue

            if best_tokens:
                evol_map[idx] = (cheval, best_tokens)

        print(f"[DEBUG trending] Found {len(evol_map)} horses with evolution data")
        
        if not evol_map:
            return []

        # build list of initial odds to compute initial ranks
        idxs = list(evol_map.keys())
        initial_odds = [evol_map[i][1][0] for i in idxs]

        results = []
        for idx in idxs:
            cheval, tokens = evol_map[idx]
            init = tokens[0]
            last = tokens[-1]
            abs_drop = init - last
            pct_drop = (abs_drop / init) if init and init > 0 else 0.0
            # initial rank: 1 + number of horses with lower (better) initial odds
            rank = int(sum(1 for v in initial_odds if v < init)) + 1
            results.append({'idx': idx, 'cheval': cheval, 'init': init, 'last': last, 'abs_drop': abs_drop, 'pct_drop': pct_drop, 'rank': rank})
            print(f"[DEBUG trending] {cheval}: rank={rank}, init={init:.1f}, last={last:.1f}, drop={abs_drop:.1f} ({pct_drop*100:.0f}%)")

        n = len(results)
        threshold_rank = 3 if n > 5 else max(2, int(np.ceil(n / 3)))
        print(f"[DEBUG trending] threshold_rank={threshold_rank}, total horses={n}")

        # Select horses that were not initial favorites (rank > threshold_rank) and had positive drop
        candidates = [r for r in results if r['rank'] > threshold_rank and r['abs_drop'] > 0]
        print(f"[DEBUG trending] Candidates after filter: {len(candidates)}")
        for c in candidates:
            print(f"  {c['cheval']}: rank={c['rank']}, drop={c['abs_drop']:.1f}")
        
        # sort by percent drop then absolute drop
        candidates.sort(key=lambda r: (r['pct_drop'], r['abs_drop']), reverse=True)
        top = candidates[:top_n]
        formatted = [f"{t['cheval']} ({t['init']:.1f}→{t['last']:.1f}, -{t['pct_drop']*100:.0f}%)" for t in top]
        print(f"[DEBUG trending] Returning {len(formatted)} trending horses: {formatted}")
        return formatted
    except Exception as e:
        print(f"Error computing trending horses: {e}")
        import traceback
        traceback.print_exc()
        return []


def reducing_system(base, associates, combination_size):
    """
    Generate reduced combinations keeping `base` mandatory and selecting the remaining
    members from `associates` to reach `combination_size`.
    base and associates are lists of strings (horse numbers).
    Returns list of tuples.
    """
    base = tuple(base)
    needed = combination_size - len(base)
    if needed < 0:
        return []
    if needed == 0:
        return [base]
    reduced = []
    for combo in itertools.combinations(associates, needed):
        reduced.append(base + combo)
    return reduced


def greedy_reducer(base, associates, combination_size, max_combinations=50):
    """
    Greedy reducer that selects combinations to maximize coverage of (combination_size-1)-subsets.
    Returns up to max_combinations tuples.
    """
    all_combos = reducing_system(base, associates, combination_size)
    selected = []
    coverage = set()
    for combo in all_combos:
        subsets = set(itertools.combinations(combo, combination_size - 1))
        if len(subsets - coverage) > 0:
            selected.append(combo)
            coverage |= subsets
        if len(selected) >= max_combinations:
            break
    return selected


def compute_corde_score(race_df):
    """
    Compute a normalized 'Corde' score (0-1) representing starting post advantage.
    Higher values = more advantageous starting position.

    Heuristics used:
    - Detect a post number column among common names ("N°", "N", "Poste", "POSTE", "Corde", "post").
    - If autostart is detected (columns like 'autostart', 'AUTO', 'Autostart'), prefer center positions.
    - For flat races (default), lower post numbers are generally better; effect increases for shorter distances.
    - If distance is present, adjust strength: short distances (<1600m) amplify post importance;
      very long distances (>3000m) reduce its importance.

    Returns a pandas Series indexed like race_df with values in [0,1]. If unable to compute, returns Series of 0.5.
    """
    try:
        # Detect post column among common variants (prefer uppercase CORDE)
        post_col = None
        for cand in ('CORDE','Corde','corde','N\u00b0','N','Poste','POSTE','Post'):
            if cand in race_df.columns:
                post_col = cand
                break
        if post_col is None:
            return pd.Series(0.5, index=race_df.index)

        # Extract numeric portion if column contains non-numeric characters
        try:
            posts = pd.to_numeric(race_df[post_col].astype(str).str.extract(r"(\d+)")[0], errors='coerce')
        except Exception:
            posts = pd.to_numeric(race_df[post_col], errors='coerce')
        if posts.dropna().empty:
            return pd.Series(0.5, index=race_df.index)

        max_post = int(np.nanmax(posts.fillna(0))) if not posts.dropna().empty else 1
        max_post = max(1, max_post)

        # detect distance to scale importance (meters)
        dist_cols = ['distance', 'DISTANCE', 'dist', 'DIST']
        race_distance = None
        for c in dist_cols:
            if c in race_df.columns:
                try:
                    race_distance = float(str(race_df[c].iloc[0]).replace('m','').strip())
                except:
                    race_distance = None
                break

        # distance-based multiplier (shorter -> amplify advantage; longer -> reduce)
        mult = 1.0
        if race_distance is not None:
            try:
                if race_distance <= 1200:
                    mult = 1.30
                elif race_distance <= 1600:
                    mult = 1.15
                elif race_distance <= 2400:
                    mult = 1.00
                elif race_distance <= 3000:
                    mult = 0.80
                else:
                    mult = 0.60
            except:
                mult = 1.0

        # Compute base score where lower post number is better (1 is best)
        # raw between 0..1 (1 best): raw = 1 - (post-1)/(max_post-1)
        score = pd.Series(0.5, index=race_df.index, dtype=float)
        denom = max(1.0, max_post - 1.0)
        for idx, p in posts.items():
            try:
                pval = float(p)
                if np.isnan(pval) or pval <= 0:
                    score.at[idx] = 0.5
                    continue

                raw = 1.0 - ((pval - 1.0) / denom)
                # bring toward extremes depending on distance multiplier
                sc = 0.5 + (raw - 0.5) * mult
                sc = max(0.0, min(1.0, sc))
                score.at[idx] = sc
            except Exception:
                score.at[idx] = 0.5

        # round to 2 decimals for consistency
        try:
            score = score.round(2)
        except Exception:
            pass
        return score
    except Exception as e:
        print(f"Error computing corde score: {e}")
        return pd.Series(0.5, index=race_df.index)


def compute_age_score(race_df):
    """Compute normalized age score (0-1) favoring 5-6 year olds and scaled by distance."""
    try:
        col = 'Age' if 'Age' in race_df.columns else ('AGE' if 'AGE' in race_df.columns else None)
        if col is None:
            return pd.Series(0.5, index=race_df.index)

        ages = pd.to_numeric(race_df[col], errors='coerce')
        if ages.dropna().empty:
            return pd.Series(0.5, index=race_df.index)

        ideal = 5.5
        # use observed spread for normalization fallback
        max_range = max(1.0, float(ages.dropna().max() - ages.dropna().min()))
        raw = 1.0 - (ages.subtract(ideal).abs() / max_range)
        raw = raw.clip(0.0, 1.0).fillna(0.5)

        # distance scaling
        dist = None
        for dcol in ['distance','DISTANCE','dist','DIST']:
            if dcol in race_df.columns:
                try:
                    dist = float(str(race_df[dcol].iloc[0]).replace('m','').strip())
                except Exception:
                    dist = None
                break

        mult = 1.0
        if dist is not None:
            try:
                if dist <= 1200:
                    mult = 1.30
                elif dist <= 1600:
                    mult = 1.15
                elif dist <= 2400:
                    mult = 1.00
                elif dist <= 3000:
                    mult = 0.80
                else:
                    mult = 0.60
            except Exception:
                mult = 1.0

        age_score = (0.5 + (raw - 0.5) * mult).clip(0.0, 1.0)
        try:
            age_score = age_score.round(2)
        except Exception:
            pass
        return age_score
    except Exception as e:
        print(f"Error computing age score: {e}")
        return pd.Series(0.5, index=race_df.index)


def compute_sex_score(race_df):
    """Compute normalized sex score (0-1) based on gender advantages in racing.
    
    In French horse racing (flat/trot):
    - Males generally have a slight physical advantage in longer distances
    - Females (mares/fillies) can be competitive, especially at moderate distances
    - Score: Males = 0.55, Females = 0.50, Unknown = 0.50
    - Adjusted based on race distance when available
    """
    try:
        # Find sex column (Sex, Sexe, SEXE)
        sex_col = None
        for col in ['Sex', 'Sexe', 'SEXE']:
            if col in race_df.columns:
                sex_col = col
                break
        
        if sex_col is None:
            return pd.Series(0.5, index=race_df.index)
        
        # Extract sex values and normalize (M/Mele/Male = 1, F/Femelle/Female = 0)
        sex_vals = race_df[sex_col].astype(str).str.upper().str.strip()
        
        # Map to scores
        sex_score = pd.Series(0.5, index=race_df.index, dtype=float)
        male_mask = sex_vals.str.contains('^M|^MALE', regex=True, na=False)
        female_mask = sex_vals.str.contains('^F|^FEMALE', regex=True, na=False)
        
        # Base scores: males slightly favored
        sex_score[male_mask] = 0.55
        sex_score[female_mask] = 0.50
        
        # Distance adjustment: females are more competitive at shorter/moderate distances
        dist = None
        for dcol in ['distance', 'DISTANCE', 'dist', 'DIST']:
            if dcol in race_df.columns:
                try:
                    dist = float(str(race_df[dcol].iloc[0]).replace('m', '').strip())
                except Exception:
                    dist = None
                break
        
        if dist is not None:
            try:
                if dist <= 1200:
                    # Short races: females favored equally
                    sex_score[female_mask] = 0.52
                elif dist <= 1600:
                    # Medium distance: females slightly favored
                    sex_score[female_mask] = 0.51
                elif dist <= 2400:
                    # Long distance: males maintain advantage
                    sex_score[male_mask] = 0.56
                else:
                    # Very long: male advantage increases
                    sex_score[male_mask] = 0.58
            except Exception:
                pass
        
        try:
            sex_score = sex_score.round(2)
        except Exception:
            pass
        
        return sex_score
    except Exception as e:
        print(f"Error computing sex score: {e}")
        return pd.Series(0.5, index=race_df.index)


class SettingsDialog(QDialog):
    """Dialog to edit metric weights. Sliders arranged horizontally to save space."""
    def __init__(self, parent=None, metric_weights=None):
        super().__init__(parent)
        self.setWindowTitle("Metric Weights")
        self.setMinimumWidth(600)
        self.metric_weights = dict(metric_weights or {})

        layout = QVBoxLayout(self)

        # Horizontal slider row inside a scroll area if needed
        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(6,6,6,6)
        row_layout.setSpacing(12)

        self.sliders = {}
        self.labels = {}

        for metric, val in self.metric_weights.items():
            col = QVBoxLayout()
            mlabel = QLabel(metric)
            mlabel.setAlignment(Qt.AlignCenter)
            slider = QSlider()
            slider.setOrientation(Qt.Vertical)
            slider.setRange(0,100)
            slider.setValue(int(val*100))
            slider.setFixedHeight(160)
            pct_label = QLabel(f"{int(val*100)}%")
            pct_label.setAlignment(Qt.AlignCenter)
            col.addWidget(mlabel)
            col.addWidget(slider)
            col.addWidget(pct_label)
            row_layout.addLayout(col)

            self.sliders[metric] = slider
            self.labels[metric] = pct_label

            slider.valueChanged.connect(lambda v, m=metric: self._on_slider_change(m, v))

        layout.addWidget(row_widget)

        # Reset button + Dialog buttons
        button_layout = QHBoxLayout()
        
        reset_btn = QPushButton("Reset to Defaults")
        reset_btn.setToolTip("Reset all weights to their default values")
        reset_btn.clicked.connect(self._reset_to_defaults)
        button_layout.addWidget(reset_btn)
        
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        button_layout.addWidget(buttons)
        
        layout.addLayout(button_layout)

    def _on_slider_change(self, metric, value):
        """Handle slider change with proportional redistribution.
        When a metric weight increases, other weights shrink proportionally to maintain 100% total.
        When a metric weight decreases, other weights grow proportionally.
        """
        try:
            new_val = float(value) / 100.0
            old_val = self.metric_weights.get(metric, new_val)
            delta = new_val - old_val
            
            # Identify other metrics to rebalance
            other_metrics = [m for m in self.metric_weights.keys() if m != metric]
            
            if other_metrics and abs(delta) > 0.0001:
                # Sum of weights that will be redistributed
                old_sum_others = sum(self.metric_weights.get(m, 0) for m in other_metrics)
                
                if old_sum_others > 0:
                    # Calculate proportional scale factor
                    # If increasing metric: other_metrics must shrink to (old_sum_others - delta)
                    # If decreasing metric: other_metrics must grow to (old_sum_others - delta)
                    new_sum_others = old_sum_others - delta
                    if new_sum_others > 0:
                        scale = new_sum_others / old_sum_others
                    else:
                        scale = 0.0
                    
                    # Update other metrics proportionally (block signals to avoid cascading)
                    for m in other_metrics:
                        new_weight = self.metric_weights[m] * scale
                        self.metric_weights[m] = max(new_weight, 0.0)
                        
                        # Update slider and label without triggering change events
                        self.sliders[m].blockSignals(True)
                        self.sliders[m].setValue(int(self.metric_weights[m] * 100))
                        self.labels[m].setText(f"{int(self.metric_weights[m] * 100)}%")
                        self.sliders[m].blockSignals(False)
            
            # Update the changed metric
            self.metric_weights[metric] = new_val
            self.labels[metric].setText(f"{int(value)}%")
            
        except Exception as e:
            print(f"Error in slider change handler: {e}")

    def _reset_to_defaults(self):
        """Reset all metric weights to their default values."""
        # Default percentages: IC(20%), S_COEFF(20%), IF(15%), n_weight(10%), COTE(20%), Corde(10%), VALEUR(5%)
        default_perc = {'IC': 20, 'S_COEFF': 20, 'IF': 15, 'n_weight': 10, 'COTE': 20, 'Corde': 10, 'VALEUR': 5}
        
        # Block signals to prevent proportional rebalancing during reset
        for metric in self.metric_weights.keys():
            self.sliders[metric].blockSignals(True)
        
        try:
            # Update weights and UI elements
            for metric in self.metric_weights.keys():
                default_val = default_perc.get(metric, 10)
                self.metric_weights[metric] = float(default_val) / 100.0
                self.sliders[metric].setValue(default_val)
                self.labels[metric].setText(f"{default_val}%")
        finally:
            # Re-enable signals
            for metric in self.metric_weights.keys():
                self.sliders[metric].blockSignals(False)

    def get_weights(self):
        return dict(self.metric_weights)


# Global cache for normalization parameters to improve performance
_normalization_cache = {}

def normalize_composite_columns(df):
    """
    Pre-normalize columns used in Composite score computation.
    Normalizes PER RACE (based on REF_COURSE/ID_COURSE) to avoid cross-race contamination.
    Returns DataFrame with normalized columns added as _norm_<metric> columns.
    
    Performance: Groups by race first, then normalizes within each race group.
    This ensures horses are ranked relative to their own race, not globally.
    """
    global _normalization_cache
    
    if df.empty:
        return df
    
    result = df.copy()
    
    # Identify race column (REF_COURSE or ID_COURSE)
    race_col = None
    if 'REF_COURSE' in df.columns:
        race_col = 'REF_COURSE'
    elif 'ID_COURSE' in df.columns:
        race_col = 'ID_COURSE'
    
    # If no race column, normalize globally as fallback
    if race_col is None:
        race_groups = [(None, df)]
    else:
        race_groups = df.groupby(race_col, sort=False)
    
    # Build list of columns to normalize
    normalize_map = {
        'IF': ['IF', 'If'],
        'IC': ['IC', 'Ic'],
        'S_COEFF': ['S_COEFF', 's_COEFF'],
        'n_weight': ['n_weight', 'N_WEIGHT'],
        'POIDS': ['POIDS', 'Poids', 'weight'],
        'VALEUR': ['VALEUR', 'Valeur'],
        'COTE': ['COTE', 'Cote'],
        # NOTE: Corde is NOT included here because it's computed separately via compute_corde_score()
        # which already properly handles the logic: lower post number = better = higher score
    }
    
    # Initialize normalized columns with default 0.5
    for metric in normalize_map.keys():
        result[f'_norm_{metric}'] = 0.5
    # Initialize Corde normalization (computed separately)
    result['_norm_Corde'] = 0.5
    
    # Normalize each race separately
    for race_key, race_df in race_groups:
        indices = race_df.index
        
        for metric, col_variants in normalize_map.items():
            col = next((c for c in col_variants if c in race_df.columns), None)
            
            if col is None:
                continue
            
            try:
                # Convert to numeric
                if metric == 'Experience':
                    series = race_df[col].fillna("").astype(str).str.len().astype(float)
                else:
                    series = pd.to_numeric(race_df[col], errors='coerce')
                
                # Handle VALEUR, n_weight, and POIDS with bell-curve normalization (middle values are optimal)
                # Higher VALEUR = more weight (bad), Lower VALEUR = inexperience (bad), Middle = optimal
                # Extreme n_weight changes (very high or very low) = risky, Middle = optimal
                # Extreme POIDS (too light or too heavy) = suboptimal, Middle = optimal
                if metric in ['VALEUR', 'n_weight', 'POIDS']:
                    if not series.dropna().empty:
                        # Use a Gaussian-like bell curve: penalize both low and high values
                        # Normalize to [0, 1] first, then apply Gaussian
                        smin = series.min()
                        smax = series.max()
                        if not pd.isna(smin) and not pd.isna(smax) and smax != smin:
                            normalized_range = (series - smin) / (smax - smin)
                            # Apply Gaussian: e^(-(x-0.5)^2 / 0.08) to reward middle values
                            normalized = np.exp(-(normalized_range - 0.5) ** 2 / 0.08)
                        else:
                            normalized = pd.Series(0.5, index=series.index)
                    else:
                        normalized = pd.Series(0.5, index=series.index)
                # Invert if lower is better (lower odds = better confidence)
                elif metric in ['IF', 'COTE']:
                    series = -series
                    
                    # Min-max normalize within this race
                    if not series.dropna().empty:
                        smin = series.min()
                        smax = series.max()
                        if not pd.isna(smin) and not pd.isna(smax) and smax != smin:
                            normalized = (series - smin) / (smax - smin)
                        else:
                            normalized = pd.Series(0.5, index=series.index)
                    else:
                        normalized = pd.Series(0.5, index=series.index)
                else:
                    # Standard min-max normalize for other metrics
                    if not series.dropna().empty:
                        smin = series.min()
                        smax = series.max()
                        if not pd.isna(smin) and not pd.isna(smax) and smax != smin:
                            normalized = (series - smin) / (smax - smin)
                        else:
                            normalized = pd.Series(0.5, index=series.index)
                    else:
                        normalized = pd.Series(0.5, index=series.index)
                
                # Assign back to result dataframe using race indices
                result.loc[indices, f'_norm_{metric}'] = normalized.fillna(0.5).round(2).values
            except Exception:
                result.loc[indices, f'_norm_{metric}'] = 0.5

        # Compute Corde normalization per race (if possible)
        try:
            corde_vals = compute_corde_score(race_df)
            result.loc[indices, '_norm_Corde'] = corde_vals.fillna(0.5).values
        except Exception:
            result.loc[indices, '_norm_Corde'] = 0.5
    
    return result


def compute_composite_score(race_df, weights=None):
    """Compute a composite score (0-1) from selected metrics.
    We normalize metrics to 0-1 where higher is better and apply weights.
    
    For trotting races, includes DQ_Risk (inverted: lower risk = higher score).
    
    Performance optimization: Uses pre-normalized columns if available (_norm_* columns),
    otherwise computes normalization on-the-fly.
    """
    try:
        # Check if this is a trotting race by presence of trotting-specific columns
        is_trot = 'DQ_Risk' in race_df.columns or 'disq_count' in race_df.columns
        
        # Check if pre-normalized columns are available (from normalize_composite_columns)
        has_prenorm = all(f'_norm_{m}' in race_df.columns for m in ['IC', 'S_COEFF', 'IF', 'n_weight', 'VALEUR'])
        
        # Set default weights if not provided
        if weights is None:
            if is_trot:
                # Trotting: S_COEFF(20%), COTE(15%), DQ_Risk(20%), Fitness(15%), Slope(15%), REC(10%), Shoeing(5%)
                weights = {'S_COEFF':0.20, 'COTE':0.15, 'DQ_Risk_Inverted':0.20, 'Fitness':0.15, 'Slope':0.15, 'REC':0.10, 'Shoeing_Agg':0.05}
            else:
                # Flat: traditional weights
                weights = {'IC':0.20, 'S_COEFF':0.20, 'IF':0.15, 'n_weight':0.10, 'COTE':0.20, 'Corde':0.10, 'VALEUR':0.05}
        
        if has_prenorm:
            # Fast path: use pre-normalized columns
            norm = pd.DataFrame(index=race_df.index)
            norm['IC'] = race_df['_norm_IC']
            norm['S_COEFF'] = race_df['_norm_S_COEFF']
            norm['IF'] = race_df['_norm_IF']
            norm['n_weight'] = race_df['_norm_n_weight']
            norm['VALEUR'] = race_df['_norm_VALEUR']
            norm['Corde'] = race_df.get('_norm_Corde', pd.Series(0.5, index=race_df.index))
            norm['COTE'] = race_df.get('_norm_COTE', pd.Series(0.5, index=race_df.index))
        else:
            # Slow path: compute normalization on-the-fly (original logic)
            metrics = {}
            # prefer column variants
            metrics['IF'] = 'IF' if 'IF' in race_df.columns else ('If' if 'If' in race_df.columns else None)
            metrics['IC'] = 'IC' if 'IC' in race_df.columns else ('Ic' if 'Ic' in race_df.columns else None)
            metrics['S_COEFF'] = 'S_COEFF' if 'S_COEFF' in race_df.columns else ('s_COEFF' if 's_COEFF' in race_df.columns else None)
            metrics['n_weight'] = 'n_weight' if 'n_weight' in race_df.columns else ('N_WEIGHT' if 'N_WEIGHT' in race_df.columns else None)
            metrics['VALEUR'] = 'VALEUR' if 'VALEUR' in race_df.columns else ('Valeur' if 'Valeur' in race_df.columns else None)
            metrics['Corde'] = 'Corde' if 'Corde' in race_df.columns else ('CORDE' if 'CORDE' in race_df.columns else None)
            metrics['COTE'] = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)

            # Build norm table
            norm = pd.DataFrame(index=race_df.index)

        # Ensure Corde is present in norm when computable (FLAT RACES ONLY - trotting has no CORDE)
        if not is_trot and 'Corde' not in norm.columns:
            try:
                corde_series = compute_corde_score(race_df)
                if corde_series is not None:
                    norm['Corde'] = corde_series.fillna(0.5)
            except Exception:
                pass
        
        # Normalize S_COEFF and COTE for trotting (these are the main metrics available)
        if is_trot:
            # S_COEFF: higher is better
            if 'S_COEFF' in race_df.columns:
                try:
                    s_vals = pd.to_numeric(race_df['S_COEFF'], errors='coerce')
                    if not s_vals.dropna().empty:
                        smin, smax = s_vals.min(), s_vals.max()
                        if smax > smin:
                            norm['S_COEFF'] = ((s_vals - smin) / (smax - smin)).fillna(0.5)
                        else:
                            norm['S_COEFF'] = 0.5
                except Exception:
                    norm['S_COEFF'] = 0.5
            
            # COTE: lower is better (inverted)
            cote_col = 'COTE' if 'COTE' in race_df.columns else ('Cote' if 'Cote' in race_df.columns else None)
            if cote_col:
                try:
                    cote_vals = pd.to_numeric(race_df[cote_col], errors='coerce')
                    if not cote_vals.dropna().empty:
                        cote_inv = -cote_vals  # invert: lower odds = better
                        cmin, cmax = cote_inv.min(), cote_inv.max()
                        if cmax > cmin:
                            norm['COTE'] = ((cote_inv - cmin) / (cmax - cmin)).fillna(0.5)
                        else:
                            norm['COTE'] = 0.5
                except Exception:
                    norm['COTE'] = 0.5
            
            # Fitness (FA/FM): lower is better (inverted)
            fa_col = 'FA' if 'FA' in race_df.columns else None
            fm_col = 'FM' if 'FM' in race_df.columns else None
            fitness_col = fa_col if fa_col else fm_col
            if fitness_col:
                try:
                    fit_vals = pd.to_numeric(race_df[fitness_col], errors='coerce')
                    if not fit_vals.dropna().empty:
                        fit_inv = -fit_vals  # invert: lower fitness (better horses) = higher score
                        fmin, fmax = fit_inv.min(), fit_inv.max()
                        if fmax > fmin:
                            norm['Fitness'] = ((fit_inv - fmin) / (fmax - fmin)).fillna(0.5)
                        else:
                            norm['Fitness'] = 0.5
                except Exception:
                    norm['Fitness'] = 0.5
            
            # Recent Count (REC.): lower is better (inverted) - fewer recent races = more rest
            if 'REC.' in race_df.columns:
                try:
                    rec_vals = pd.to_numeric(race_df['REC.'], errors='coerce')
                    if not rec_vals.dropna().empty:
                        rec_inv = -rec_vals  # invert: lower recent count = better rested
                        rmin, rmax = rec_inv.min(), rec_inv.max()
                        if rmax > rmin:
                            norm['REC'] = ((rec_inv - rmin) / (rmax - rmin)).fillna(0.5)
                        else:
                            norm['REC'] = 0.5
                except Exception:
                    norm['REC'] = 0.5
            
            # Trend Slope (form trajectory): lower/negative is better (improving form)
            if 'trend_rank_slope' in race_df.columns:
                try:
                    slope_vals = pd.to_numeric(race_df['trend_rank_slope'], errors='coerce')
                    if not slope_vals.dropna().empty:
                        slope_inv = -slope_vals  # invert: declining slope (negative) = improving = better
                        slmin, slmax = slope_inv.min(), slope_inv.max()
                        if slmax > slmin:
                            norm['Slope'] = ((slope_inv - slmin) / (slmax - slmin)).fillna(0.5)
                        else:
                            norm['Slope'] = 0.5
                except Exception:
                    norm['Slope'] = 0.5
            
            # Shoeing Aggressiveness: higher is better (aggressive strategy = driver confidence)
            if 'shoeing_aggressiveness' in race_df.columns:
                try:
                    shoe_vals = pd.to_numeric(race_df['shoeing_aggressiveness'], errors='coerce')
                    if not shoe_vals.dropna().empty:
                        shmin, shmax = shoe_vals.min(), shoe_vals.max()
                        if shmax > shmin:
                            norm['Shoeing_Agg'] = ((shoe_vals - shmin) / (shmax - shmin)).fillna(0.5)
                        else:
                            norm['Shoeing_Agg'] = 0.5
                except Exception:
                    norm['Shoeing_Agg'] = 0.5
        
        # Add DQ_Risk (inverted) for trotting races: lower risk = higher score
        if is_trot and 'DQ_Risk' in race_df.columns:
            try:
                dq_risk_vals = pd.to_numeric(race_df['DQ_Risk'], errors='coerce').fillna(50.0)
                # Invert: 0 risk -> 1.0 score, 100 risk -> 0.0 score
                norm['DQ_Risk_Inverted'] = (100.0 - dq_risk_vals) / 100.0
                norm['DQ_Risk_Inverted'] = norm['DQ_Risk_Inverted'].clip(0, 1)
            except Exception:
                norm['DQ_Risk_Inverted'] = 0.5
        
        if norm.empty:
            return pd.DataFrame()

        # Round normalized metrics to 2 decimals for consistency
        try:
            norm = norm.round(2)
        except Exception:
            pass

        # Apply weights (ignore metrics not present)
        total_weight = sum(weights[k] for k in weights if k in norm.columns)
        if total_weight == 0:
            total_weight = 1.0
        
        # DEBUG: Log which metrics are being used and their weights
        metrics_in_norm = [k for k in weights if k in norm.columns]
        print(f"[DEBUG] compute_composite_score: is_trot={is_trot}, metrics_used={metrics_in_norm}")
        print(f"[DEBUG] weights being applied: {' | '.join(f'{k}={weights[k]}' for k in metrics_in_norm)}")
        
        weighted = pd.Series(0.0, index=norm.index)
        for k, w in weights.items():
            if k in norm.columns:
                weighted += norm[k] * w

        weighted = weighted / total_weight
        try:
            weighted = weighted.round(2)
        except Exception:
            pass
        
        # DEBUG: Show first few weighted scores and all horses' scores
        if len(weighted) > 0:
            print(f"[DEBUG] Weighted composite scores (first 5): {weighted.head().values}")
            print(f"[DEBUG] Composite score stats: min={weighted.min():.2f}, max={weighted.max():.2f}, mean={weighted.mean():.2f}, std={weighted.std():.4f}")
            if weighted.std() < 0.01:
                print(f"[WARNING] Very low standard deviation in composite scores! All scores are too similar.")
                print(f"[DEBUG] Full score distribution: {weighted.value_counts().to_dict()}")
        
        result = race_df.copy()
        # Ensure canonical 'Cheval' column exists for downstream code (heatmap, tables)
        cheval_col = 'CHEVAL' if 'CHEVAL' in race_df.columns else ('Cheval' if 'Cheval' in race_df.columns else None)
        if cheval_col:
            try:
                result['Cheval'] = result[cheval_col].astype(str)
            except Exception:
                result['Cheval'] = result[cheval_col].apply(lambda x: str(x) if pd.notna(x) else "")
        else:
            # Fallback: create Cheval from index
            result['Cheval'] = result.index.astype(str)
        result['Composite'] = weighted
        # expose computed normalized Corde/Age/Sex scores if available for downstream display/heatmap
        try:
            if 'Corde' in norm.columns:
                result['Corde'] = norm['Corde'].values
        except Exception:
            pass
        try:
            if 'Age' in norm.columns:
                result['AgeScore'] = norm['Age'].values
                try:
                    result['AgeScore'] = pd.to_numeric(result['AgeScore'], errors='coerce').round(2)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if 'Sex' in norm.columns:
                result['SexScore'] = norm['Sex'].values
                try:
                    result['SexScore'] = pd.to_numeric(result['SexScore'], errors='coerce').round(2)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if 'Experience' in norm.columns:
                result['Experience'] = norm['Experience'].values
                try:
                    result['Experience'] = pd.to_numeric(result['Experience'], errors='coerce').round(2)
                except Exception:
                    pass
        except Exception:
            pass

        # Ensure COTE/Cote column is included (handle case variations)
        if 'COTE' not in result.columns and 'Cote' not in result.columns:
            # Try to find and add COTE from original data
            if 'COTE' in race_df.columns:
                result['COTE'] = race_df['COTE']
            elif 'Cote' in race_df.columns:
                result['Cote'] = race_df['Cote']

        # Add individual metric columns from norm table (for heatmap)
        for metric in ['IF', 'IC', 'S_COEFF', 'n_weight', 'VALEUR']:
            if metric in norm.columns:
                result[metric] = norm[metric].values
        
        # Return limited columns for ranking
        cols = [c for c in ['Cheval', 'N°', 'COTE', 'Cote', 'Composite'] if c in result.columns]
        cols += [c for c in ['IF','IC','S_COEFF','n_weight','VALEUR','Poids','POIDS','Corde','AgeScore','SexScore','Experience','DQ_Risk','Fitness','Slope','REC','Shoeing_Agg'] if c in result.columns]
        return result[cols].sort_values('Composite', ascending=False)
    except Exception as e:
        print(f"Error computing composite score: {e}")
        return pd.DataFrame()

def create_heatmap_canvas(norm_df, composite_scores=None):
    """Create a matplotlib FigureCanvas heatmap from a normalized DataFrame (horses x metrics).
    
    Args:
        norm_df: DataFrame with horses as index and metrics as columns (values 0-1)
        composite_scores: Series with horse names as index and composite scores (optional, for sorting)
    """
    try:
        if norm_df.empty:
            fig = plt.figure(figsize=(4,3))
            return FigureCanvas(fig)

        # Sort by composite score if provided
        if composite_scores is not None:
            # Get horses in composite order
            sorted_horses = composite_scores.index.tolist()
            norm_df = norm_df.loc[[h for h in sorted_horses if h in norm_df.index]]

        # Dynamic sizing: taller for more horses
        num_horses = len(norm_df.index)
        fig_height = max(3, 0.5 * num_horses)
        fig, ax = plt.subplots(figsize=(8, fig_height))
        
        # Create heatmap with RdYlGn colormap
        im = ax.imshow(norm_df.values, aspect='auto', cmap='RdYlGn', vmin=0, vmax=1)
        
        # Configure axes
        ax.set_xticks(range(len(norm_df.columns)))
        ax.set_xticklabels(norm_df.columns, rotation=45, ha='right', fontsize=10, fontweight='bold')
        ax.set_yticks(range(len(norm_df.index)))
        ax.set_yticklabels(norm_df.index, fontsize=9, fontweight='bold')
        
        # Add gridlines for clarity
        ax.set_xticks([x - 0.5 for x in range(1, len(norm_df.columns))], minor=True)
        ax.set_yticks([y - 0.5 for y in range(1, len(norm_df.index))], minor=True)
        ax.grid(which='minor', color='white', linestyle='-', linewidth=1.5)
        
        # Add text annotations (normalized values)
        for i in range(len(norm_df.index)):
            for j in range(len(norm_df.columns)):
                val = norm_df.values[i, j]
                # Color text based on cell value for contrast
                text_color = 'white' if val < 0.5 else 'black'
                ax.text(j, i, f'{val:.2f}', ha='center', va='center', 
                       color=text_color, fontsize=8, fontweight='bold')
        
        # Enhanced colorbar
        cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cbar.set_label('Strength (0=Low, 1=High)', fontsize=10, fontweight='bold')
        
        # Title and layout
        ax.set_title('Heatmap des performances des chevaux\n(triee par score composite)', 
                    fontsize=12, fontweight='bold', pad=15)
        fig.tight_layout()
        
        canvas = FigureCanvas(fig)
        try:
            plt.close(fig)
        except Exception:
            pass
        # Wrap canvas in a container widget for consistent parenting
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(canvas)
        return container
    except Exception as e:
        print(f"Error creating heatmap: {e}")
        fig = plt.figure(figsize=(4,3))
        return FigureCanvas(fig)


def create_interactive_heatmap_widget(norm_df, composite_scores=None, parent=None, on_metric_toggle=None):
    """Wrapper that uses the Matplotlib-based interactive heatmap."""
    return create_interactive_matplotlib_heatmap(norm_df, composite_scores, parent, on_metric_toggle=on_metric_toggle)

class ScraperThread(QThread):
    finished = pyqtSignal(pd.DataFrame, str, str, str)  # df, race_type, source, custom_table
    progress = pyqtSignal(int, str)  # percent, message
    error = pyqtSignal(str)
    
    def __init__(self, url, race_type, source='zone-turf', custom_table=None):
        super().__init__()
        self.url = url
        self.race_type = race_type
        self.source = source
        self.custom_table = custom_table
        self.is_cancelled = False

    def emit_progress(self, percent, message):
        try:
            self.progress.emit(int(percent), str(message))
        except Exception:
            pass
    
    def get_cancel_check(self):
        """Return a callable that checks if cancellation was requested."""
        return lambda: self.is_cancelled
    
    def cancel(self):
        """Signal the thread to stop processing."""
        self.is_cancelled = True
    
    def run(self):
        try:
            # Pass both progress_callback and cancel_check to allow stopping during download
            df = data_source_manager.scrape_races(
                self.url, 
                self.race_type, 
                self.source, 
                progress_callback=self.emit_progress,
                cancel_check=self.get_cancel_check()
            )
            if not self.is_cancelled:
                self.finished.emit(df, self.race_type, self.source, self.custom_table)
            else:
                self.error.emit("Téléchargement annulé par l'utilisateur")
        except Exception as e:
            if not self.is_cancelled:
                self.error.emit(str(e))


class MeetingLoaderThread(QThread):
    """Thread to load available meetings without blocking the UI."""
    finished = pyqtSignal(dict)  # meetings dictionary
    error = pyqtSignal(str)
    
    def __init__(self, date_qdate=None):
        super().__init__()
        self.date_qdate = date_qdate
        self.is_cancelled = False
    
    def cancel(self):
        """Signal the thread to stop processing."""
        self.is_cancelled = True
    
    def run(self):
        print(f"[DEBUG] MeetingLoaderThread.run() started")
        try:
            # Check for early cancellation
            if self.is_cancelled:
                print("[DEBUG] Thread cancelled before starting")
                return
            
            print(f"[DEBUG] Scraping meetings for date: {self.date_qdate}")
            meetings = scrape_meeting_urls(self.date_qdate)
            print(f"[DEBUG] Scraping complete, got {len(meetings)} meetings")
            
            # Check again before emitting
            if not self.is_cancelled:
                print(f"[DEBUG] Emitting finished signal with {len(meetings)} meetings")
                self.finished.emit(meetings)
                print("[DEBUG] Signal emitted")
            else:
                print("[DEBUG] Thread was cancelled before emitting")
        except Exception as e:
            print(f"[DEBUG] Thread exception: {e}")
            import traceback
            traceback.print_exc()
            if not self.is_cancelled:
                self.error.emit(str(e))


# =====================================================
# ANALYSIS WINDOW CLASSES
# =====================================================

class BaseAnalysisWindow(QMainWindow):
    """Base class for race analysis windows (flat and trotting)"""
    
    def __init__(self, filtered_df, race_id_text, date_text, race_type='flat', parent_app=None):
        super().__init__()
        self.filtered_df = filtered_df
        self.race_id_text = race_id_text
        self.date_text = date_text
        self.race_type = race_type
        self.parent_app = parent_app
        
        self.setWindowTitle(f"[TARGET] Analyse - {race_type.capitalize()} ({race_id_text})")
        self.setGeometry(100, 100, 1300, 850)
        
        # Apply professional stylesheet to window
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f7fa;
            }
            QTabWidget::pane {
                border: 1px solid #d5dce0;
                border-radius: 4px;
            }
            QTabBar::tab {
                background-color: #e8eef5;
                border: 1px solid #d5dce0;
                padding: 6px 12px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                color: #2c3e50;
            }
            QTabBar::tab:selected {
                background-color: #ffffff;
                border-bottom: 2px solid #3498db;
                color: #2c3e50;
                font-weight: bold;
            }
            QTabBar::tab:hover:!selected {
                background-color: #d8e0eb;
            }
            QLabel {
                color: #2c3e50;
            }
        """)
        
        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_widget.setStyleSheet("background-color: #f5f7fa;")
        layout = QVBoxLayout(main_widget)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)
        
        # Add title with better styling
        title = QLabel(f"Analyse de Course - {race_id_text} ({date_text})")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setStyleSheet("""
            color: #2c3e50;
            padding: 8px 12px;
            background-color: #ffffff;
            border-radius: 4px;
            border-left: 4px solid #3498db;
        """)
        layout.addWidget(title)
        
        # Create tabs for different analyses
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)
        
        # Add analysis tabs
        self.setup_analysis_tabs()
        
        # Show window
        self.show()
    
    def setup_analysis_tabs(self):
        """Override in subclasses to add specific analysis tabs"""
        # Default: add a generic analysis tab
        analysis_widget = QWidget()
        analysis_layout = QVBoxLayout()
        analysis_layout.addWidget(QLabel("Analysis data will appear here"))
        analysis_widget.setLayout(analysis_layout)
        self.tab_widget.addTab(analysis_widget, "Analysis")


class TrottingAnalysisWindow(BaseAnalysisWindow):
    """Specialized analysis window for trotting races"""
    
    def __init__(self, filtered_df, race_id_text, date_text, parent_app=None):
        super().__init__(filtered_df, race_id_text, date_text, race_type='trot', parent_app=parent_app)
        self.setWindowTitle(f"[TROTTING] Analyse - {race_id_text}")
    
    def setup_analysis_tabs(self):
        """Setup trotting-specific analysis tabs"""
        try:
            # Compute trotting composite score first
            composite_df = self.compute_trotting_composite()
            if composite_df is not None and not composite_df.empty:
                composite_widget = self.create_analysis_tab(composite_df, "Composite Score")
                self.tab_widget.addTab(composite_widget, "📊 Composite")
                # Trotting-specific heatmap (next to composite)
                heatmap_widget = self.display_heatmap()
                if heatmap_widget is not None:
                    try:
                        self.tab_widget.addTab(heatmap_widget, "🔥 Heatmap")
                    except Exception:
                        # fallback: ignore heatmap if widget incompatible
                        pass
                # Summary / Prognosis / Exclusives tab with special formatting
                try:
                    summary_df = analyze_trotting_summary_prognosis(self.filtered_df, composite_df)
                    summary_widget = self.create_summary_tab(summary_df)
                    self.tab_widget.addTab(summary_widget, "✨ Summary & Prognosis")
                except Exception:
                    pass
            
            # Trotting Fitness Tab (FA/FM instead of IF)
            fitness_df = analyze_trotting_fitness(self.filtered_df)
            fitness_widget = self.create_analysis_tab(fitness_df, "Fitness")
            self.tab_widget.addTab(fitness_widget, "💪 Fitness (FA/FM)")
            
            # Trotting Performance Tab (S_COEFF instead of IC)
            performance_df = analyze_trotting_performance(self.filtered_df)
            performance_widget = self.create_analysis_tab(performance_df, "Performance")
            self.tab_widget.addTab(performance_widget, "⚡ Performance (S_COEFF)")
            
            # Trotting Trend Tab (form trajectory & recent performance)
            trend_df = analyze_trotting_trend(self.filtered_df)
            trend_widget = self.create_analysis_tab(trend_df, "Trend")
            self.tab_widget.addTab(trend_widget, "📈 Form Trend")
            
            # Trotting Shoeing Tab (driver confidence indicator)
            shoeing_df = analyze_trotting_shoeing(self.filtered_df)
            shoeing_widget = self.create_analysis_tab(shoeing_df, "Shoeing")
            self.tab_widget.addTab(shoeing_widget, "👟 Shoeing Strategy")
            
            # Disqualification Risk Tab (trotting-specific risk)
            disq_df = analyze_trotting_disqualification_risk(self.filtered_df)
            disq_widget = self.create_analysis_tab(disq_df, "Disq Risk")
            self.tab_widget.addTab(disq_widget, "⚠️ Disq Risk")
            
        except Exception as e:
            print(f"[ERROR] Failed to setup trotting analysis tabs: {e}")
            import traceback
            traceback.print_exc()
    
    def compute_trotting_composite(self):
        """
        Compute trotting-specific composite score for each horse.
        Uses the main compute_composite_score function with trotting weights:
        S_COEFF(20%), COTE(15%), DQ_Risk(20%), Fitness(15%), Slope(15%), REC(10%), Shoeing(5%)
        Returns DataFrame with N°, CHEVAL, Composite Score, and ranking.
        """
        try:
            if self.filtered_df.empty:
                print("[DEBUG] Cannot compute composite: filtered_df is empty")
                return pd.DataFrame()
            
            # Make a copy to avoid modifying original
            composite_df = self.filtered_df.copy()
            
            # Use the main compute_composite_score function with trotting weights
            result_df = compute_composite_score(composite_df, weights=None)  # None triggers trotting path
            
            if result_df is None or result_df.empty:
                print("[ERROR] compute_composite_score returned None or empty DataFrame")
                return pd.DataFrame()
            
            # Check if Composite column exists
            if 'Composite' not in result_df.columns:
                print(f"[ERROR] Composite column not found. Available columns: {result_df.columns.tolist()}")
                return pd.DataFrame()
            
            # Select key columns for display
            # Find the horse number and horse name columns (they may have variations)
            num_col = None
            for col in ['N°', 'N', 'Numero']:
                if col in result_df.columns:
                    num_col = col
                    break
            
            cheval_col = None
            for col in ['CHEVAL', 'Cheval', 'cheval', 'Horse']:
                if col in result_df.columns:
                    cheval_col = col
                    break
            
            display_cols = []
            if num_col:
                display_cols.append(num_col)
            if cheval_col:
                display_cols.append(cheval_col)
            if 'Composite' not in display_cols:
                display_cols.append('Composite')
            
            # Add optional columns if they exist
            optional_cols = ['FA', 'FM', 'S_COEFF', 'REC.', 'DEF.', 'COTE', 'Cote']
            for col in optional_cols:
                if col in result_df.columns:
                    display_cols.append(col)
            
            # Filter to columns that exist
            available_cols = [col for col in display_cols if col in result_df.columns]
            result_df = result_df[available_cols].copy()
            
            # Add rank column (1 = best, higher composite is better)
            if not result_df.empty:
                result_df['Rank'] = result_df['Composite'].rank(method='min', ascending=False).astype(int)
                # Move Rank column to position 3 (after CHEVAL, before Composite)
                cols = result_df.columns.tolist()
                if 'Rank' in cols:
                    cols.remove('Rank')
                    cols.insert(3, 'Rank')
                    result_df = result_df[cols]
            
            # Sort by composite score descending (higher is better)
            result_df = result_df.sort_values('Composite', ascending=False, na_position='last')
            
            print(f"[DEBUG] Computed trotting composite scores for {len(result_df)} horses")
            return result_df
            
        except Exception as e:
            print(f"[ERROR] Failed to compute trotting composite score: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def create_analysis_tab(self, analysis_df, title):
        """Create a tab with analysis data in table format with professional styling"""
        widget = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(8, 8, 8, 8)
        
        if analysis_df is None or analysis_df.empty:
            empty_label = QLabel(f"No {title} data available")
            empty_label.setStyleSheet("color: #7f8c8d; font-style: italic; padding: 20px;")
            layout.addWidget(empty_label)
        else:
            # Create table with professional styling
            table = QTableWidget()
            table.setStyleSheet("""
                QTableWidget {
                    background-color: #ffffff;
                    gridline-color: #d5dce0;
                    border: 1px solid #bfc9d4;
                    border-radius: 4px;
                }
                QHeaderView::section {
                    background-color: #34495e;
                    color: #ffffff;
                    padding: 8px;
                    border: none;
                    font-weight: bold;
                    font-size: 11px;
                }
                QTableWidget::item {
                    padding: 6px;
                    border-bottom: 1px solid #ecf0f1;
                }
                QTableWidget::item:alternate-background-color {
                    background-color: #f8f9fa;
                }
            """)
            table.setAlternatingRowColors(True)
            table.setRowCount(len(analysis_df))
            table.setColumnCount(len(analysis_df.columns))
            table.setHorizontalHeaderLabels(analysis_df.columns.tolist())
            
            # Fill table with data and smart formatting
            for row in range(len(analysis_df)):
                for col in range(len(analysis_df.columns)):
                    value = analysis_df.iloc[row, col]
                    item_text = str(value) if not pd.isna(value) else "-"
                    item = QTableWidgetItem(item_text)
                    
                    # Center align numeric columns
                    try:
                        float(str(value).replace(',', '.'))
                        item.setTextAlignment(Qt.AlignCenter)
                    except:
                        pass
                    
                    table.setItem(row, col, item)
            
            # Auto-resize columns but with minimum widths
            table.resizeColumnsToContents()
            for col in range(table.columnCount()):
                if table.columnWidth(col) < 60:
                    table.setColumnWidth(col, 80)
            
            layout.addWidget(table)
        
        widget.setLayout(layout)
        return widget
    
    def create_summary_tab(self, analysis_df):
        """Create a formatted Summary & Prognosis tab with visual sections"""
        widget = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)
        
        if analysis_df is None or analysis_df.empty:
            layout.addWidget(QLabel("No summary data available"))
        else:
            # Group by Type and display sections
            for section_type in ['SUMMARY', 'PROGNOSIS', 'EXCLUSIVES']:
                section_data = analysis_df[analysis_df['Type'] == section_type]
                
                if not section_data.empty:
                    # Section title with background
                    section_title = QLabel()
                    if section_type == 'SUMMARY':
                        title_text = "⭐ SUMMARY - Top Contenders"
                        bg_color = "#e8f4f8"
                    elif section_type == 'PROGNOSIS':
                        title_text = "🔮 PROGNOSIS - Race Predictions"
                        bg_color = "#fff9e6"
                    else:
                        title_text = "❌ EXCLUSIVES - Horses to Avoid"
                        bg_color = "#ffe8e8"
                    
                    section_title.setText(title_text)
                    section_title.setStyleSheet(f"""
                        background-color: {bg_color};
                        padding: 10px 12px;
                        border-radius: 4px;
                        font-weight: bold;
                        font-size: 12px;
                    """)
                    layout.addWidget(section_title)
                    
                    # Table for section
                    table = QTableWidget()
                    table.setStyleSheet("""
                        QTableWidget {
                            background-color: #ffffff;
                            gridline-color: #f0f0f0;
                            border: 1px solid #ddd;
                        }
                        QHeaderView::section {
                            background-color: #f5f5f5;
                            color: #2c3e50;
                            padding: 6px;
                            font-weight: bold;
                            font-size: 10px;
                            border: none;
                        }
                        QTableWidget::item {
                            padding: 6px;
                        }
                    """)
                    table.setRowCount(len(section_data))
                    table.setColumnCount(len(section_data.columns) - 1)  # Exclude Type column
                    
                    # Headers without Type column
                    headers = [col for col in section_data.columns if col != 'Type']
                    table.setHorizontalHeaderLabels(headers)
                    
                    # Fill data
                    for row, (idx, data_row) in enumerate(section_data.iterrows()):
                        for col, header in enumerate(headers):
                            value = data_row[header]
                            item_text = str(value) if not pd.isna(value) else "-"
                            item = QTableWidgetItem(item_text)
                            item.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
                            table.setItem(row, col, item)
                    
                    table.resizeColumnsToContents()
                    layout.addWidget(table)
            
            layout.addStretch()
        
        widget.setLayout(layout)
        return widget

    def display_heatmap(self):
        """
        Prepare trotting-specific normalized metrics and return an interactive heatmap widget.
        Metrics included (if present): FA, FM, REC., avg_rank, recent_avg_rank,
        trend_rank_slope, S_COEFF, COTE, disq_harness_rate/disq_mounted_rate.
        Values are normalized to 0..1 where higher = better.
        """
        try:
            from pandas import DataFrame
            # Build composite scores mapping if available
            composite_df = self.compute_trotting_composite()
            composite_scores = None
            if composite_df is not None and not composite_df.empty and 'CHEVAL' in composite_df.columns:
                # composite_df index might not be horse names; ensure index is horse name
                if 'CHEVAL' in composite_df.columns:
                    composite_scores = composite_df.set_index('CHEVAL')['Composite']
                elif 'CHEVAL' in self.filtered_df.columns:
                    composite_scores = composite_df.set_index(self.filtered_df['CHEVAL'])['Composite']

            df = self.filtered_df.copy()
            if df is None or df.empty:
                return QWidget()

            # Determine horse name column
            horse_col = None
            for c in ('CHEVAL', 'Cheval', 'NAME', 'Nom'):
                if c in df.columns:
                    horse_col = c
                    break
            if horse_col is None:
                df['CHEVAL'] = df.index.astype(str)
                horse_col = 'CHEVAL'

            metrics = []
            candidate_cols = ['FA', 'FM', 'REC.', 'REC', 'avg_rank', 'recent_avg_rank', 'recent_avg', 'trend_rank_slope', 'trend_slope', 'S_COEFF', 's_COEFF', 'COTE', 'Cote', 'disq_harness_rate', 'disq_mounted_rate']
            for col in candidate_cols:
                if col in df.columns and col not in metrics:  # Check for duplicates
                    metrics.append(col)

            # If no explicit candidate columns found, search columns by keyword patterns
            if not metrics:
                patterns = ['fa', 'fm', 'rec', 'avg_rank', 'recent', 'trend', 's_coeff', 'cote', 'disq', 'dq']
                for c in df.columns:
                    low = c.lower()
                    for p in patterns:
                        if p in low and c not in metrics:  # Ensure no duplicates
                            metrics.append(c)
                            break

            print(f"[DEBUG] display_heatmap: horse_col={horse_col}, found_metrics={metrics}")
            if not metrics:
                print("[DEBUG] No metrics found for heatmap; available columns:\n", list(df.columns))
                return QWidget()

            # Build matrix: rows=horse names, cols=metrics
            mat = {}
            for col in metrics:
                # Show sample raw values for debugging
                try:
                    sample_vals = df[col].astype(str).head(5).tolist()
                except Exception:
                    sample_vals = []
                print(f"[DEBUG] Metric candidate '{col}' sample values: {sample_vals}")

                # Robust cleaning FIRST: handle common string formats like '1,23', '3.4%', or embedded text
                try:
                    col_data = df[col]
                    print(f"[DEBUG] Column '{col}' dtype={col_data.dtype}, first 3 raw values: {col_data.head(3).tolist()}")
                    # Convert to string, replace comma decimal separators, extract first numeric token
                    tmp = col_data.astype(str).str.strip().str.replace(',', '.')
                    print(f"[DEBUG] After string conversion & replace, first 3 values: {tmp.head(3).tolist()}")
                    # Extract numeric part (handles percentages, trailing text)
                    nums = tmp.str.extract(r'(-?\d+\.?\d*)', expand=False)
                    print(f"[DEBUG] After regex extract, first 3 values: {nums.head(3).tolist()}")
                    ser = pd.to_numeric(nums, errors='coerce')
                    print(f"[DEBUG] After pd.to_numeric for '{col}': non-null count={ser.count()}, min={ser.min()}, max={ser.max()}")
                except Exception as e:
                    print(f"[DEBUG] Robust parsing failed for '{col}': {e}")
                    import traceback
                    traceback.print_exc()
                    ser = pd.to_numeric(df[col], errors='coerce')
                    print(f"[DEBUG] After fallback direct conversion '{col}': non-null count={ser.count()}, min={ser.min()}, max={ser.max()}")
                
                # **CRITICAL FIX**: Set the index to horse names so DataFrame alignment works
                ser.index = df[horse_col].astype(str)
                mat[col] = ser

            mat_df = pd.DataFrame(mat)  # All Series already have the correct index (horse names)

            # **FILTER OUT EMPTY COLUMNS**: Remove any metric with zero non-null values
            mat_df = mat_df.dropna(axis=1, how='all')  # Drop columns that are all NaN
            print(f"[DEBUG] After removing all-NaN columns: {mat_df.columns.tolist()}")
            
            # **HEATMAP ONLY**: Append horse numbers to index for display
            # Build horse_name -> horse_num mapping BEFORE processing
            num_col_for_heatmap = 'N°' if 'N°' in df.columns else ('N' if 'N' in df.columns else None)
            horse_num_map = {}
            if num_col_for_heatmap:
                for _, row in df.iterrows():
                    h_name = str(row.get(horse_col, '')).strip()
                    h_num = row.get(num_col_for_heatmap)
                    if h_name and h_num and pd.notna(h_num):
                        horse_num_map[h_name] = str(h_num)
            
            # Now append numbers to index
            if horse_num_map:
                new_index = [f"{name} (N°{horse_num_map.get(name, name)})" if name in horse_num_map else name 
                            for name in mat_df.index]
                mat_df.index = new_index

            # Debug: Print mat_df structure
            print(f"[DEBUG] mat_df shape: {mat_df.shape}")
            print(f"[DEBUG] mat_df columns: {mat_df.columns.tolist()}")
            print(f"[DEBUG] mat_df dtypes:\n{mat_df.dtypes}")
            print(f"[DEBUG] mat_df sample (first 3 rows, first 3 cols):\n{mat_df.iloc[:3, :3] if mat_df.shape[1] >= 3 else mat_df.head(3)}")
            print(f"[DEBUG] mat_df non-null counts:\n{mat_df.count()}")

            # Normalize columns to 0..1 with higher better
            norm_df = DataFrame(index=mat_df.index)
            for col in mat_df.columns:
                col_series = mat_df[col].copy()
                print(f"[DEBUG] Processing column '{col}': type={col_series.dtype}, non-null count before={col_series.count()}, values: {col_series.head(3).tolist()}")
                # Define whether higher is better
                higher_better = True
                if col in ('FA', 'FM', 'REC.', 'avg_rank', 'recent_avg_rank'):
                    higher_better = False
                if col in ('trend_rank_slope',):
                    # negative slope is better -> invert sign
                    col_series = -col_series
                    higher_better = True
                if col in ('disq_harness_rate', 'disq_mounted_rate'):
                    higher_better = False

                # Convert to numeric and fillna with median
                # At this point col_series should be numeric or NaN
                if col_series.dropna().empty:
                    print(f"[DEBUG] Heatmap metric '{col}' has no numeric values after parsing")
                    norm_df[col] = 0.5
                    continue
                mn = col_series.min()
                mx = col_series.max()
                # Log range for debugging
                try:
                    print(f"[DEBUG] Heatmap metric '{col}': count={col_series.count()}, min={mn}, max={mx}")
                except Exception:
                    pass
                if pd.isna(mn) or pd.isna(mx) or mx == mn:
                    # No variation -> use neutral 0.5 but keep consistent numeric dtype
                    norm = pd.Series(0.5, index=col_series.index)
                else:
                    norm = (col_series - mn) / (mx - mn)
                # If lower is better, invert
                if not higher_better:
                    norm = 1.0 - norm
                # Fill NaNs with 0.5
                norm = norm.fillna(0.5)
                norm_df[col] = norm
                print(f"[DEBUG] Normalized metric '{col}': sample values={norm.head(5).tolist()}")

            # Use create_interactive_heatmap_widget to build the widget
            heatmap_widget = create_interactive_heatmap_widget(norm_df, composite_scores, parent=self)
            return heatmap_widget
        except Exception as e:
            print(f"[ERROR] Failed to build trotting heatmap: {e}")
            import traceback
            traceback.print_exc()
            return QWidget()


class BetGeneratorWindow(QMainWindow):
    """Standalone window for bet generation with independent logic."""
    
    def __init__(self, parent=None, current_composite_df=None, current_filtered_df=None, last_filtered_df=None, parent_app=None):
        super().__init__(parent)
        self.setWindowTitle("[RACEX] Advanced Bet Generator")
        self.setGeometry(200, 200, 1400, 800)
        self.current_composite_df = current_composite_df if current_composite_df is not None else pd.DataFrame()
        self.current_filtered_df = current_filtered_df if current_filtered_df is not None else pd.DataFrame()
        self.last_filtered_df = last_filtered_df if last_filtered_df is not None else pd.DataFrame()
        self.parent_app = parent_app  # Reference to main app for method access
        
        # Stylesheets
        self.INPUT_STYLESHEET = """
            QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {
                background-color: #f5f5f5;
                color: #2c3e50;
                border: 1px solid #bdc3c7;
                padding: 4px 6px;
                border-radius: 4px;
            }
        """
        self.TABLE_STYLESHEET = """
            QTableWidget {
                background-color: #ffffff;
                gridline-color: #e0e0e0;
                border: 1px solid #d0d0d0;
                border-radius: 4px;
            }
        """
        self.BUTTON_STYLESHEET = """
            QPushButton {
                background-color: #27ae60;
                color: white;
                padding: 8px 12px;
                border: none;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2ecc71;
            }
        """
        
        self.setup_ui()
    
    def setup_ui(self):
        """Create the UI layout."""
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)
        
        # Title
        title = QLabel("Advanced Bet Generator")
        title.setStyleSheet("font-size: 14px; font-weight: bold; color: #2c3e50;")
        layout.addWidget(title)
        
        # Main strategies displayed in 3-column grid
        strategies_container = QWidget()
        strategies_layout = QHBoxLayout(strategies_container)
        strategies_layout.setContentsMargins(0, 0, 0, 0)
        strategies_layout.setSpacing(12)
        
        # Column 1: Classic
        classic_group = QGroupBox("📊 Classic (RaceX)")
        classic_group.setStyleSheet("QGroupBox { font-weight: bold; color: #2c3e50; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 3px; }")
        classic_layout = QVBoxLayout(classic_group)
        classic_layout.addWidget(self.create_classic_tab_content())
        strategies_layout.addWidget(classic_group, 1)
        
        # Column 2: Manual Bets
        manual_group = QGroupBox("🎲 Manual Bets")
        manual_group.setStyleSheet("QGroupBox { font-weight: bold; color: #2c3e50; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 3px; }")
        manual_layout = QVBoxLayout(manual_group)
        manual_layout.addWidget(self.create_outsiders_tab_content())
        strategies_layout.addWidget(manual_group, 1)
        
        # Column 3: Smart Mix
        smart_group = QGroupBox("🎭 Smart Mix")
        smart_group.setStyleSheet("QGroupBox { font-weight: bold; color: #2c3e50; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 3px; }")
        smart_layout = QVBoxLayout(smart_group)
        smart_layout.addWidget(self.create_smart_mix_tab_content())
        strategies_layout.addWidget(smart_group, 1)
        
        layout.addWidget(strategies_container)
        
        # Results area
        layout.addWidget(QLabel("Generated Combinations:"))
        self.results_table = QTableWidget()
        self.results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.results_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.results_table.setAlternatingRowColors(True)
        layout.addWidget(self.results_table)
        
        # Status and buttons
        bottom_layout = QHBoxLayout()
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("color: #27ae60; font-weight: bold;")
        bottom_layout.addWidget(self.status_label)
        bottom_layout.addStretch()
        
        export_btn = QPushButton("Export PDF")
        export_btn.setStyleSheet(self.BUTTON_STYLESHEET)
        export_btn.clicked.connect(self.export_results_pdf)
        bottom_layout.addWidget(export_btn)
        
        layout.addLayout(bottom_layout)
        
        # Status bar
        self.statusBar().showMessage("Ready")
    
    def create_classic_tab_content(self):
        """Create controls for classic RaceX bet generation."""
        controls = QWidget()
        controls_layout = QVBoxLayout(controls)
        controls_layout.setContentsMargins(6, 6, 6, 6)
        
        controls_layout.addWidget(QLabel("Mandatory Horses:"))
        self.classic_mandatory_spin = QSpinBox()
        self.classic_mandatory_spin.setRange(0, 8)
        self.classic_mandatory_spin.setValue(2)
        self.classic_mandatory_spin.setMaximumWidth(80)
        self.classic_mandatory_spin.setStyleSheet(self.INPUT_STYLESHEET)
        controls_layout.addWidget(self.classic_mandatory_spin)
        
        controls_layout.addWidget(QLabel("Horses per Combo:"))
        self.classic_bet_size = QSpinBox()
        self.classic_bet_size.setRange(1, 8)
        self.classic_bet_size.setValue(5)
        self.classic_bet_size.setMaximumWidth(80)
        self.classic_bet_size.setStyleSheet(self.INPUT_STYLESHEET)
        controls_layout.addWidget(self.classic_bet_size)
        
        controls_layout.addWidget(QLabel("Horses Source:"))
        self.classic_assoc_source = QComboBox()
        self.classic_assoc_source.addItems(["Summary", "Predictions+Summary", "All"])
        self.classic_assoc_source.setStyleSheet(self.INPUT_STYLESHEET)
        controls_layout.addWidget(self.classic_assoc_source)
        
        controls_layout.addWidget(QLabel("Max Combos:"))
        self.classic_max_combos = QSpinBox()
        self.classic_max_combos.setRange(1, 500)
        self.classic_max_combos.setValue(50)
        self.classic_max_combos.setMaximumWidth(120)
        self.classic_max_combos.setStyleSheet(self.INPUT_STYLESHEET)
        controls_layout.addWidget(self.classic_max_combos)
        
        btn = QPushButton("Generate")
        btn.setStyleSheet(self.BUTTON_STYLESHEET)
        btn.clicked.connect(self.generate_classic_bets)
        controls_layout.addWidget(btn)
        
        self.classic_method_label = QLabel("Ready")
        self.classic_method_label.setStyleSheet("color: #95a5a6; font-size: 10px; font-style: italic;")
        controls_layout.addWidget(self.classic_method_label)
        
        controls_layout.addStretch()
        return controls
    
    def create_outsiders_tab_content(self):
        """Create controls for manual bet generation."""
        controls = QWidget()
        ctrl_layout = QVBoxLayout(controls)
        ctrl_layout.setContentsMargins(6, 6, 6, 6)
        
        ctrl_layout.addWidget(QLabel("Must-Have Horses:"))
        self.manual_must_have = QLineEdit()
        self.manual_must_have.setPlaceholderText("Ex: 2,5,7")
        self.manual_must_have.setToolTip("Horses that MUST appear in every combination")
        self.manual_must_have.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.manual_must_have)
        
        ctrl_layout.addWidget(QLabel("Optional Pool:"))
        self.manual_optional = QLineEdit()
        self.manual_optional.setPlaceholderText("Ex: 1,3,4,6,8,9 or leave empty for all")
        self.manual_optional.setToolTip("Horses to choose from for remaining slots")
        self.manual_optional.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.manual_optional)
        
        ctrl_layout.addWidget(QLabel("Exclude Horses:"))
        self.manual_exclude = QLineEdit()
        self.manual_exclude.setPlaceholderText("Ex: 10,11,12")
        self.manual_exclude.setToolTip("Horses to exclude from combinations")
        self.manual_exclude.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.manual_exclude)
        
        ctrl_layout.addWidget(QLabel("Horses per Combo:"))
        self.manual_bet_size = QSpinBox()
        self.manual_bet_size.setRange(1, 8)
        self.manual_bet_size.setValue(5)
        self.manual_bet_size.setMaximumWidth(80)
        self.manual_bet_size.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.manual_bet_size)
        
        ctrl_layout.addWidget(QLabel("Strategy:"))
        self.manual_strategy = QComboBox()
        self.manual_strategy.addItems(["Systematic", "Random", "Sorted by Score"])
        self.manual_strategy.setToolTip("Systematic: enumerate all; Random: random selection; Sorted: best composite first")
        self.manual_strategy.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.manual_strategy)
        
        ctrl_layout.addWidget(QLabel("Max Combos:"))
        self.manual_max_combos = QSpinBox()
        self.manual_max_combos.setRange(1, 500)
        self.manual_max_combos.setValue(25)
        self.manual_max_combos.setMaximumWidth(120)
        self.manual_max_combos.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.manual_max_combos)
        
        btn = QPushButton("Generate")
        btn.setStyleSheet(self.BUTTON_STYLESHEET)
        btn.clicked.connect(self.generate_manual_bets)
        ctrl_layout.addWidget(btn)
        
        self.manual_method_label = QLabel("Ready")
        self.manual_method_label.setStyleSheet("color: #95a5a6; font-size: 10px; font-style: italic;")
        ctrl_layout.addWidget(self.manual_method_label)
        
        ctrl_layout.addStretch()
        return controls
    
    
    def create_smart_mix_tab_content(self):
        """Create controls for combined strategy bets."""
        controls = QWidget()
        ctrl_layout = QVBoxLayout(controls)
        ctrl_layout.setContentsMargins(6, 6, 6, 6)
        
        ctrl_layout.addWidget(QLabel("Mandatory Outsiders:"))
        self.smart_mandatory_spin = QSpinBox()
        self.smart_mandatory_spin.setRange(1, 5)
        self.smart_mandatory_spin.setValue(2)
        self.smart_mandatory_spin.setMaximumWidth(80)
        self.smart_mandatory_spin.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.smart_mandatory_spin)
        
        ctrl_layout.addWidget(QLabel("Horses per Combo:"))
        self.smart_bet_size_spin = QSpinBox()
        self.smart_bet_size_spin.setRange(1, 8)
        self.smart_bet_size_spin.setValue(5)
        self.smart_bet_size_spin.setMaximumWidth(80)
        self.smart_bet_size_spin.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.smart_bet_size_spin)
        
        ctrl_layout.addWidget(QLabel("Max Combos:"))
        self.smart_max_combos_spin = QSpinBox()
        self.smart_max_combos_spin.setRange(1, 500)
        self.smart_max_combos_spin.setValue(10)
        self.smart_max_combos_spin.setMaximumWidth(120)
        self.smart_max_combos_spin.setStyleSheet(self.INPUT_STYLESHEET)
        ctrl_layout.addWidget(self.smart_max_combos_spin)
        
        btn = QPushButton("Generate")
        btn.setStyleSheet(self.BUTTON_STYLESHEET)
        btn.clicked.connect(self.generate_smart_mix_bets)
        ctrl_layout.addWidget(btn)
        ctrl_layout.addStretch()
        
        return controls
    
    def generate_classic_bets(self):
        """Generate bets using classic RaceX logic."""
        self.statusBar().showMessage("Generating classic bets...")
        self.status_label.setText("Generating...")
        QApplication.processEvents()
        
        try:
            print(f"[DEBUG] generate_classic_bets: parent_app={self.parent_app}, has generate_bets={hasattr(self.parent_app, 'generate_bets') if self.parent_app else False}")
            if self.parent_app is not None and hasattr(self.parent_app, 'generate_bets'):
                # Call parent app's generate_bets logic
                print(f"[DEBUG] Calling parent_app.generate_bets()...")
                self.parent_app.generate_bets()
                # Copy results from parent's bets_table to this window's results_table
                if hasattr(self.parent_app, 'bets_table'):
                    print(f"[DEBUG] Parent bets_table exists, calling copy_table_data()...")
                    self.copy_table_data(self.parent_app.bets_table, self.results_table)
                    self.status_label.setText(f"Generated {self.results_table.rowCount()} combinations")
                    self.statusBar().showMessage(f"Classic bets generated: {self.results_table.rowCount()} combinations")
                    print(f"[DEBUG] Copy complete. Results table now has {self.results_table.rowCount()} rows")
                else:
                    print(f"[DEBUG] Parent has no bets_table")
                    self.status_label.setText("Error: Parent bets_table not available")
            else:
                print(f"[DEBUG] Parent app not available or no generate_bets method")
                self.status_label.setText("Parent app not available")
                self.statusBar().showMessage("Error: Parent app not available")
        except Exception as e:
            print(f"[ERROR] generate_classic_bets failed: {e}")
            import traceback
            traceback.print_exc()
            self.status_label.setText(f"Error: {str(e)[:50]}")
            self.statusBar().showMessage(f"Error generating bets: {e}")
    
    def generate_manual_bets(self):
        """Generate bets from manually specified horses."""
        self.statusBar().showMessage("Generating manual bets...")
        self.status_label.setText("Generating...")
        QApplication.processEvents()
        
        try:
            # Parse user inputs
            must_have_str = self.manual_must_have.text().strip()
            optional_str = self.manual_optional.text().strip()
            exclude_str = self.manual_exclude.text().strip()
            combo_size = self.manual_bet_size.value()
            strategy = self.manual_strategy.currentText()
            max_combos = self.manual_max_combos.value()
            
            # Parse horse numbers from strings
            def parse_horse_numbers(s):
                """Parse comma-separated horse numbers."""
                if not s:
                    return []
                return [h.strip() for h in s.split(',') if h.strip()]
            
            must_have = parse_horse_numbers(must_have_str)
            optional = parse_horse_numbers(optional_str)
            exclude = set(parse_horse_numbers(exclude_str))
            
            # Extract all available horse numbers from the race
            available_horses = set()
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                for col in ['N°', 'N', 'Numero', 'N?']:
                    if col in df.columns:
                        for raw in df[col].tolist():
                            if raw is None:
                                continue
                            v = str(raw).strip()
                            if v.endswith('.0'):
                                v = v[:-2]
                            if not v or v.lower() in ('nan', 'none', ''):
                                continue
                            if not v.isdigit():
                                v_clean = ''.join(ch for ch in v if ch.isdigit())
                                if not v_clean:
                                    continue
                                v = v_clean
                            if v == '0':
                                continue
                            available_horses.add(v)
                        break
            
            if not available_horses:
                self.status_label.setText("Error: Could not extract available horses from race")
                self.statusBar().showMessage("Error: No horses found in race")
                return
            
            # Validate must-have horses
            invalid_must_have = [h for h in must_have if h not in available_horses]
            if invalid_must_have:
                self.status_label.setText(f"Error: Invalid must-have horses: {', '.join(invalid_must_have)}")
                self.statusBar().showMessage(f"Error: Horses not in race: {', '.join(invalid_must_have)}")
                return
            
            # Validate optional horses (if specified)
            if optional_str:
                invalid_optional = [h for h in optional if h not in available_horses]
                if invalid_optional:
                    self.status_label.setText(f"Error: Invalid optional horses: {', '.join(invalid_optional)}")
                    self.statusBar().showMessage(f"Error: Horses not in race: {', '.join(invalid_optional)}")
                    return
            
            # Validate exclude horses (warn about invalid ones)
            invalid_exclude = [h for h in exclude if h not in available_horses]
            if invalid_exclude:
                # Show warning but don't fail - excluded horses that don't exist are harmless
                print(f"[WARNING] Excluded horses not in race (ignoring): {', '.join(invalid_exclude)}")
            
            # Validate must-have count
            if len(must_have) > combo_size:
                self.status_label.setText(f"Error: Must-have count ({len(must_have)}) exceeds combo size ({combo_size})")
                self.statusBar().showMessage("Error: Too many must-have horses")
                return
            
            # If no optional pool specified, use all horses except must-have and excluded
            if not optional:
                optional = list(available_horses - set(must_have) - exclude)
            
            # Remove excluded horses from optional pool
            optional = [h for h in optional if h not in exclude and h not in must_have]
            
            # Need to fill (combo_size - len(must_have)) slots from optional pool
            slots_to_fill = combo_size - len(must_have)
            
            if len(optional) < slots_to_fill:
                self.status_label.setText(f"Error: Not enough optional horses ({len(optional)}) for {slots_to_fill} slots")
                self.statusBar().showMessage("Error: Insufficient horses in optional pool")
                return
            
            # Generate combinations based on strategy
            from itertools import combinations
            combos = []
            
            if strategy == "Systematic":
                # Enumerate all possible combinations of optional horses
                for opt_combo in combinations(optional, slots_to_fill):
                    full_combo = tuple(sorted(must_have, key=lambda x: int(x))) + opt_combo
                    combos.append(full_combo)
                    if len(combos) >= max_combos:
                        break
                        
            elif strategy == "Random":
                # Random selection
                import random
                for _ in range(max_combos):
                    selected_optional = random.sample(optional, slots_to_fill)
                    full_combo = tuple(sorted(must_have, key=lambda x: int(x))) + tuple(sorted(selected_optional, key=lambda x: int(x)))
                    if full_combo not in combos:
                        combos.append(full_combo)
                        
            elif strategy == "Sorted by Score":
                # Sort optional pool by composite score (descending) and generate from top horses
                # Build score map if composite_df available
                score_map = {}
                if hasattr(self, 'current_composite_df') and self.current_composite_df is not None and not self.current_composite_df.empty:
                    for _, row in self.current_composite_df.iterrows():
                        # Try to extract horse number from row (various column names)
                        horse_num = None
                        for col in ['N°', 'N', 'Numero', 'N?']:
                            if col in self.current_composite_df.columns:
                                h = str(row[col]).strip()
                                if h.isdigit() or (h and h.isdigit()):
                                    horse_num = h
                                    break
                        if horse_num and 'Composite' in self.current_composite_df.columns:
                            score_map[horse_num] = float(row['Composite'])
                
                # Sort optional by score
                optional_sorted = sorted(optional, key=lambda x: score_map.get(x, 0), reverse=True)
                
                # Generate combos from sorted optional pool
                for opt_combo in combinations(optional_sorted, slots_to_fill):
                    full_combo = tuple(sorted(must_have, key=lambda x: int(x))) + opt_combo
                    combos.append(full_combo)
                    if len(combos) >= max_combos:
                        break
            
            # Display results
            if combos:
                self.results_table.clear()
                self.results_table.setColumnCount(len(combos[0]))
                self.results_table.setRowCount(len(combos))
                headers = [f"H{i+1}" for i in range(len(combos[0]))]
                self.results_table.setHorizontalHeaderLabels(headers)
                
                for i, combo in enumerate(combos):
                    for j, horse in enumerate(combo):
                        item = QTableWidgetItem(str(horse))
                        item.setTextAlignment(Qt.AlignCenter)
                        self.results_table.setItem(i, j, item)
                
                self.manual_method_label.setText(f"Generated {len(combos)} combinations ({strategy})")
                self.status_label.setText(f"Generated {len(combos)} combinations")
                self.statusBar().showMessage(f"Manual bets generated: {len(combos)} combinations")
            else:
                self.status_label.setText("Could not generate combinations")
                self.statusBar().showMessage("No combinations generated")
                
        except Exception as e:
            print(f"[ERROR] generate_manual_bets failed: {e}")
            import traceback
            traceback.print_exc()
            self.status_label.setText(f"Error: {str(e)[:50]}")
            self.statusBar().showMessage(f"Error generating manual bets: {e}")
    
    
    def generate_smart_mix_bets(self):
        """Generate smart mix bets."""
        self.statusBar().showMessage("Generating smart mix bets...")
        self.status_label.setText("Generating...")
        QApplication.processEvents()
        
        try:
            # Get parameters
            mandatory_outsiders = self.smart_mandatory_spin.value()
            bet_size = self.smart_bet_size_spin.value()
            max_combos = self.smart_max_combos_spin.value()
            
            # Extract outsiders from parent app
            outsiders = []
            if hasattr(self, 'parent_app') and self.parent_app:
                if hasattr(self.parent_app, 'outsiders_divergence_table'):
                    for r in range(self.parent_app.outsiders_divergence_table.rowCount()):
                        if self.parent_app.outsiders_divergence_table.item(r, 0):
                            horse_num = self.parent_app.outsiders_divergence_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                outsiders.append(horse_num)
            
            # Extract underperforming favorites to exclude
            underperforming = set()
            if hasattr(self, 'parent_app') and self.parent_app:
                if hasattr(self.parent_app, 'underperforming_favorites_table'):
                    for r in range(self.parent_app.underperforming_favorites_table.rowCount()):
                        if self.parent_app.underperforming_favorites_table.item(r, 0):
                            horse_num = self.parent_app.underperforming_favorites_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                underperforming.add(horse_num)
            
            # Get all available horses (exclude underperforming)
            all_horses = []
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                for col in ['N°', 'N', 'Numero']:
                    if col in df.columns:
                        for raw in df[col].tolist():
                            if raw is None:
                                continue
                            v = str(raw).strip()
                            if v.endswith('.0'):
                                v = v[:-2]
                            if not v or v.lower() in ('nan', 'none'):
                                continue
                            if not v.isdigit():
                                v_clean = ''.join(ch for ch in v if ch.isdigit())
                                if not v_clean:
                                    continue
                                v = v_clean
                            if v == '0':
                                continue
                            if v not in all_horses and v not in underperforming:
                                all_horses.append(v)
                        break
            
            if not outsiders or not all_horses:
                self.status_label.setText("Insufficient data for smart mix")
                self.statusBar().showMessage("Cannot generate smart mix: missing data")
                return
            
            # Generate combinations with mandatory outsiders
            from itertools import combinations
            combos = []
            
            # Ensure we have enough horses
            additional_needed = bet_size - mandatory_outsiders
            if additional_needed < 0:
                self.status_label.setText("Bet size smaller than mandatory outsiders")
                return
            
            # Get other horses (outsiders + regular)
            other_horses = list(set(outsiders + all_horses))
            
            # Generate combinations with mandatory outsiders
            for outsider_combo in combinations(outsiders, min(mandatory_outsiders, len(outsiders))):
                if additional_needed == 0:
                    combos.append(outsider_combo)
                else:
                    # Add additional horses
                    remaining = [h for h in other_horses if h not in outsider_combo]
                    for extra_combo in combinations(remaining, min(additional_needed, len(remaining))):
                        full_combo = tuple(sorted(set(outsider_combo + extra_combo)))
                        if full_combo not in combos:
                            combos.append(full_combo)
                
                if len(combos) >= max_combos:
                    break
            
            # Display results
            if combos:
                self.results_table.clear()
                self.results_table.setColumnCount(bet_size)
                self.results_table.setRowCount(len(combos))
                headers = [f"H{i+1}" for i in range(bet_size)]
                self.results_table.setHorizontalHeaderLabels(headers)
                
                for i, combo in enumerate(combos):
                    for j in range(bet_size):
                        val = combo[j] if j < len(combo) else ''
                        item = QTableWidgetItem(str(val))
                        self.results_table.setItem(i, j, item)
                
                self.status_label.setText(f"Generated {len(combos)} combinations")
                self.statusBar().showMessage(f"Smart mix bets generated: {len(combos)} combinations")
            else:
                self.status_label.setText("Could not generate combinations")
                self.statusBar().showMessage("No combinations generated")
                
        except Exception as e:
            print(f"[ERROR] generate_smart_mix_bets failed: {e}")
            import traceback
            traceback.print_exc()
            self.status_label.setText(f"Error: {str(e)[:50]}")
            self.statusBar().showMessage(f"Error generating smart mix bets: {e}")
    
    def copy_table_data(self, source_table, dest_table):
        """Copy data from source table to destination table."""
        try:
            src_rows = source_table.rowCount()
            src_cols = source_table.columnCount()
            print(f"[DEBUG] copy_table_data: Source has {src_rows} rows x {src_cols} columns")
            
            dest_table.setColumnCount(src_cols)
            dest_table.setRowCount(src_rows)
            dest_table.setHorizontalHeaderLabels([source_table.horizontalHeaderItem(i).text() if source_table.horizontalHeaderItem(i) else f"H{i+1}" for i in range(src_cols)])
            
            for i in range(src_rows):
                for j in range(src_cols):
                    item = source_table.item(i, j)
                    if item:
                        dest_table.setItem(i, j, QTableWidgetItem(item.text()))
            
            print(f"[DEBUG] copy_table_data: Destination now has {dest_table.rowCount()} rows x {dest_table.columnCount()} columns")
        except Exception as e:
            print(f"[ERROR] copy_table_data failed: {e}")
            import traceback
            traceback.print_exc()
    
    def _get_pdf_header_footer(self):
        """Get standard header and footer for PDF documents (wrapper around module function)."""
        return get_pdf_header_footer()
    
    def export_results_pdf(self):
        """Export results table to a properly formatted PDF with UTF-8 encoding, matching save_combinations_only style."""
        self.statusBar().showMessage("Exporting PDF...")
        QApplication.processEvents()
        
        if self.results_table.rowCount() == 0:
            QMessageBox.warning(self, "No Data", "No combinations to export. Generate bets first.")
            return
        
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER
            
            # Extract table data
            rows = []
            for row in range(self.results_table.rowCount()):
                row_data = []
                for col in range(self.results_table.columnCount()):
                    item = self.results_table.item(row, col)
                    row_data.append(item.text() if item else "")
                rows.append(row_data)
            
            # Get race metadata
            ref_course = ''
            race_date = ''
            
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                ref_course = str(df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in df.columns else ''
                race_date = str(df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in df.columns else ''
            
            # Generate filename
            default_filename = "combinations.pdf"
            if race_date and ref_course:
                date_clean = race_date.replace('/', '').replace('-', '')
                default_filename = f"{date_clean}_{ref_course}_combinations.pdf"
            elif race_date:
                date_clean = race_date.replace('/', '').replace('-', '')
                default_filename = f"{date_clean}_combinations.pdf"
            
            # Save to Downloads folder
            downloads_folder = get_downloads_folder()
            path = str(downloads_folder / default_filename)
            
            # Create PDF document
            doc = SimpleDocTemplate(path, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=100, bottomMargin=50)
            story = []
            styles = getSampleStyleSheet()
            
            # Get header and footer
            header_text_racex, footer_text_racex, header_style, footer_style, _ = get_pdf_header_footer()
            
            # Add RaceX header
            story.append(Paragraph(header_text_racex, header_style))
            story.append(Spacer(1, 12))
            
            # Custom styles (matching save_combinations_only)
            title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=14, spaceAfter=30, alignment=TA_CENTER)
            
            # Header with race info
            header_text = f"Combinations Generated - {ref_course}"
            story.append(Paragraph(header_text, title_style))
            story.append(Paragraph(f"Date: {race_date}", styles['Normal']))
            story.append(Spacer(1, 12))
            
            # Get method info if available
            method_text = ''
            if hasattr(self, 'manual_method_label'):
                method_text = self.manual_method_label.text().strip()
            
            if method_text:
                story.append(Paragraph(f"<b>Method:</b> {method_text}", styles['Normal']))
                story.append(Spacer(1, 12))
            
            # Create combinations table in 3-column layout (matching save_combinations_only)
            num_combos = len(rows)
            combos_per_col = (num_combos + 2) // 3  # Distribute combinations across 3 columns
            
            # Build table data for 3-column layout
            combo_data = []
            
            # Create header row with column labels
            header_cols = ["Combinations (1)", "Combinations (2)", "Combinations (3)"]
            combo_data.append(header_cols)
            
            # Fill rows with combinations
            for row_idx in range(combos_per_col):
                row_items = []
                
                # Column 1
                if row_idx < len(rows):
                    col1_text = " - ".join([str(x) for x in rows[row_idx] if x])
                    row_items.append(col1_text)
                else:
                    row_items.append("")
                
                # Column 2
                if row_idx + combos_per_col < len(rows):
                    col2_text = " - ".join([str(x) for x in rows[row_idx + combos_per_col] if x])
                    row_items.append(col2_text)
                else:
                    row_items.append("")
                
                # Column 3
                if row_idx + 2 * combos_per_col < len(rows):
                    col3_text = " - ".join([str(x) for x in rows[row_idx + 2 * combos_per_col] if x])
                    row_items.append(col3_text)
                else:
                    row_items.append("")
                
                combo_data.append(row_items)
            
            combo_table = Table(combo_data, colWidths=[1.8*inch, 1.8*inch, 1.8*inch])
            combo_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightcyan),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
                ('TOPPADDING', (0, 1), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 8)
            ]))
            story.append(combo_table)
            
            # Add summary
            story.append(Spacer(1, 12))
            story.append(Paragraph(f"<b>Total combinations:</b> {num_combos}", styles['Normal']))
            
            # Add RaceX footer
            story.append(Spacer(1, 24))
            story.append(Paragraph(footer_text_racex, footer_style))
            
            # Build PDF
            doc.build(story)
            
            QMessageBox.information(self, "Success", f"PDF saved successfully to:\n{path}")
            self.statusBar().showMessage(f"PDF exported: {path}")
            print(f"[DEBUG] PDF exported to {path}")
            
        except ImportError:
            QMessageBox.critical(self, "Error", "ReportLab package is required. Install with: pip install reportlab")
        except Exception as e:
            print(f"[ERROR] export_results_pdf failed: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Export Failed", f"Error exporting PDF:\n{str(e)}")
            self.statusBar().showMessage(f"Export failed: {e}")


class RaceScraperApp(QMainWindow):
    # Global stylesheet for tables with UTF-8/Unicode support
    TABLE_STYLESHEET = """
        QTableWidget {
            background-color: #ffffff;
            gridline-color: #e0e0e0;
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            font-family: 'Segoe UI', Arial, 'DejaVu Sans', sans-serif;
            font-size: 10pt;
        }
        QTableWidget::item {
            padding: 4px;
            border-bottom: 1px solid #f0f0f0;
            font-family: 'Segoe UI', Arial, 'DejaVu Sans', sans-serif;
        }
        QTableWidget::item:selected {
            background-color: #3498db;
            color: white;
        }
        QTableWidget::item:hover {
            background-color: #ecf0f1;
        }
        QHeaderView::section {
            background-color: #2c3e50;
            color: white;
            padding: 6px;
            border: none;
            font-weight: bold;
            font-size: 11pt;
        }
        QHeaderView::section:hover {
            background-color: #34495e;
        }
    """
    
    # Global stylesheet for input widgets with Unicode support
    INPUT_STYLESHEET = """
        QLineEdit {
            background-color: #ffffff;
            border: 2px solid #bdc3c7;
            border-radius: 4px;
            padding: 6px;
            font-size: 10pt;
            font-family: 'Segoe UI', Arial, 'DejaVu Sans', sans-serif;
            selection-background-color: #3498db;
        }
        QLineEdit:focus {
            border: 2px solid #3498db;
            background-color: #ecf0f1;
        }
        QLineEdit:hover {
            border: 2px solid #95a5a6;
        }
        QComboBox {
            background-color: #ffffff;
            border: 2px solid #bdc3c7;
            border-radius: 4px;
            padding: 6px;
            font-size: 10pt;
            selection-background-color: #3498db;
        }
        QComboBox:focus {
            border: 2px solid #3498db;
            background-color: #ecf0f1;
        }
        QComboBox:hover {
            border: 2px solid #95a5a6;
        }
        QComboBox::drop-down {
            border: none;
            background: transparent;
            width: 20px;
        }
        QComboBox::down-arrow {
            image: url(noimg);
            width: 12px;
            height: 12px;
        }
        QComboBox QAbstractItemView {
            background-color: #ffffff;
            border: 1px solid #bdc3c7;
            selection-background-color: #3498db;
            padding: 4px;
        }
    """

    # Compact input variant for sidebars
    COMPACT_INPUT_STYLESHEET = """
        QLineEdit, QComboBox {
            background-color: #ffffff;
            border: 1px solid #c7ccd1;
            border-radius: 3px;
            padding: 4px;
            font-size: 9pt;
        }
        QLineEdit:focus, QComboBox:focus { border: 1px solid #5dade2; background-color: #f6f8fa; }
    """

    # Simple dark theme stylesheet
    DARK_STYLESHEET = """
        QWidget { background-color: #121212; color: #e0e0e0; }
        QLineEdit, QComboBox, QListWidget, QTableWidget { background-color: #1e1e1e; color: #e0e0e0; border: 1px solid #333; }
        QHeaderView::section { background-color: #1f1f1f; color: #e0e0e0; }
        QPushButton { background-color: #2d6a86; color: white; }
        QPushButton:hover { background-color: #24586f; }
    """
    # More comprehensive dark theme (extended) with table/header/tooltip improvements
    DARK_STYLESHEET_EXTENDED = """
        QWidget { background-color: #0f1113; color: #e6eef6; font-family: 'Segoe UI', Arial; }
        QToolTip { background-color: #2b2b2b; color: #f0f0f0; border: 1px solid #555; }
        QLineEdit, QComboBox, QListWidget, QTableWidget, QPlainTextEdit { background-color: #151515; color: #e6eef6; border: 1px solid #2b2b2b; }
        QLineEdit, QComboBox { selection-background-color: #264653; selection-color: #e6eef6; }
        QTableWidget, QListWidget, QAbstractItemView { color: #e6eef6; background-color: #151515; }
        QTableWidget { gridline-color: #222; selection-background-color: #264653; selection-color: #e6eef6; }
        QHeaderView::section { background-color: #161616; color: #e6eef6; padding: 6px; border: 1px solid #222; }
        QTableCornerButton::section { background-color: #161616; }
        QMenuBar { background-color: #0f1113; color: #e6eef6; }
        QMenu { background-color: #121212; color: #e6eef6; }
        QTabWidget::pane { background: #121212; }
        QTabBar::tab { background: #1b1b1b; color: #e6eef6; padding: 6px 12px; }
        QPushButton { background-color: #1f6f8b; color: #fff; border-radius: 4px; padding: 6px 10px; }
        QPushButton:hover { background-color: #196075; }
        QLabel { color: #dfeaf6; }
        QMenu::item { color: #e6eef6; }
        QHeaderView::section:disabled { color: #999999; }
        QTableWidget QTableCornerButton { background: #161616; }
    """

    # High contrast dark theme for accessibility
    HIGH_CONTRAST_DARK = """
        QWidget { background-color: #000000; color: #ffffff; font-family: 'Segoe UI', Arial; }
        QLineEdit, QComboBox, QListWidget, QTableWidget { background-color: #000000; color: #ffffff; border: 2px solid #ffffff; }
        QLineEdit::placeholder { color: #cccccc; }
        QHeaderView::section { background-color: #000000; color: #ffffff; border: 2px solid #ffffff; }
        QTableWidget { gridline-color: #ffffff; selection-background-color: #ffffff; selection-color: #000000; color: #ffffff; }
        QTableWidget QTableCornerButton { background: #000000; }
        QPushButton { background-color: #ffffff; color: #000000; font-weight: bold; }
        QToolTip { background-color: #ffffff; color: #000000; border: 1px solid #fff; }
    """

    # Compact application stylesheet to reduce paddings and fonts
    COMPACT_APP_STYLESHEET = """
        QWidget { padding: 2px; margin: 0; font-size: 9pt; }
        QPushButton { padding: 4px 8px; min-height: 18px; }
        QTabBar::tab { padding: 6px 8px; min-width: 80px; }
    """
    
    # Button stylesheet
    BUTTON_STYLESHEET = """
        QPushButton {
            background-color: #3498db;
            color: white;
            border: none;
            padding: 10px 14px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 10pt;
            min-height: 20px;
        }
        QPushButton:hover {
            background-color: #2980b9;
        }
        QPushButton:pressed {
            background-color: #1f618d;
        }
        QPushButton:disabled {
            background-color: #95a5a6;
        }
    """
    
    # Clear button (red variant)
    CLEAR_BUTTON_STYLESHEET = """
        QPushButton {
            background-color: #e74c3c;
            color: white;
            border: none;
            padding: 10px 14px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 10pt;
            min-height: 20px;
        }
        QPushButton:hover {
            background-color: #c0392b;
        }
        QPushButton:pressed {
            background-color: #a93226;
        }
    """
    
    # Dialog stylesheets
    DIALOG_STYLESHEET = """
        QDialog {
            background-color: #f5f5f5;
            color: #333333;
        }
        QDialog QLabel {
            color: #333333;
            font-size: 10pt;
        }
        QDialog QPushButton {
            background-color: #3498db;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
            min-height: 24px;
        }
        QDialog QPushButton:hover {
            background-color: #2980b9;
        }
        QDialog QPushButton:pressed {
            background-color: #1f618d;
        }
        QDialog QDialogButtonBox {
            button-layout: 0;
        }
        QDialog QLineEdit, QDialog QSpinBox, QDialog QDoubleSpinBox, QDialog QComboBox {
            background-color: #ffffff;
            color: #333333;
            border: 1px solid #bdc3c7;
            border-radius: 3px;
            padding: 6px;
            font-size: 10pt;
        }
        QDialog QLineEdit:focus, QDialog QSpinBox:focus, QDialog QDoubleSpinBox:focus, QDialog QComboBox:focus {
            border: 2px solid #3498db;
            background-color: #ffffff;
        }
        QDialog QSlider::groove:vertical {
            border: 1px solid #bdc3c7;
            background: #ecf0f1;
            width: 8px;
        }
        QDialog QSlider::handle:vertical {
            background: #3498db;
            border: 1px solid #2980b9;
            width: 18px;
            margin: -5px 0;
            border-radius: 3px;
        }
        QDialog QSlider::handle:vertical:hover {
            background: #2980b9;
        }
    """
    
    # Dark dialog stylesheet
    DIALOG_STYLESHEET_DARK = """
        QDialog {
            background-color: #1e1e1e;
            color: #e6eef6;
        }
        QDialog QLabel {
            color: #dfeaf6;
            font-size: 10pt;
        }
        QDialog QPushButton {
            background-color: #1f6f8b;
            color: #fff;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
            min-height: 24px;
        }
        QDialog QPushButton:hover {
            background-color: #196075;
        }
        QDialog QPushButton:pressed {
            background-color: #154a5e;
        }
        QDialog QLineEdit, QDialog QSpinBox, QDialog QDoubleSpinBox, QDialog QComboBox {
            background-color: #151515;
            color: #e6eef6;
            border: 1px solid #2b2b2b;
            border-radius: 3px;
            padding: 6px;
            font-size: 10pt;
        }
        QDialog QLineEdit:focus, QDialog QSpinBox:focus, QDialog QDoubleSpinBox:focus, QDialog QComboBox:focus {
            border: 2px solid #1f6f8b;
            background-color: #1a1a1a;
        }
        QDialog QSlider::groove:vertical {
            border: 1px solid #2b2b2b;
            background: #222222;
            width: 8px;
        }
        QDialog QSlider::handle:vertical {
            background: #1f6f8b;
            border: 1px solid #196075;
            width: 18px;
            margin: -5px 0;
            border-radius: 3px;
        }
        QDialog QSlider::handle:vertical:hover {
            background: #196075;
        }
    """

    def __init__(self):
        super().__init__()
        self.setWindowTitle("RaceX v1.0 - Plat et Obstacles")
        self.setWindowIcon(QIcon(QPixmap('assets/racer.png')))
        self.setGeometry(100, 100, 1200, 800)
        # Persistent settings
        self.settings = QSettings('zone_turf', 'race_scraper_app')

        # Initialize labels for bet generation (needed early to avoid AttributeError)
        self.prognosis_label = QLabel("")
        self.summary_label = QLabel("")
        self.exclusive_label = QLabel("")
        self.trending_label = QLabel("")
        self.prognosis_only_label = QLabel("")
        self.summary_prognosis_label = QLabel("")
        self.favorable_cordes_label = QLabel("")

        # Initialize QSettings ONCE at the start
        self.settings = QSettings('zone_turf', 'race_scraper_app')
        
        # Initialize betting analysis toggle flag
        try:
            # QSettings might return string "True"/"False" or boolean, handle both
            val = self.settings.value('show_analysis', 'True')
            if isinstance(val, bool):
                self.show_analysis_enabled = val
            else:
                self.show_analysis_enabled = str(val).lower() in ('true', '1', 'yes')
        except Exception:
            self.show_analysis_enabled = True
        
        # Clean up any legacy boolean storage issues by re-saving as string
        self.settings.setValue('show_analysis', 'True' if self.show_analysis_enabled else 'False')
        self.settings.sync()
        print(f"[DEBUG] Initial show_analysis_enabled={self.show_analysis_enabled}")

        # Apply persisted UI preferences (theme and compact mode)
        try:
            theme = str(self.settings.value('theme', 'Light')) if self.settings.value('theme') is not None else 'Light'
            compact_pref = bool(self.settings.value('compact_mode', False, type=bool))
            # apply theme and compact mode
            try:
                self.apply_theme(theme)
            except Exception:
                try:
                    from PyQt5.QtWidgets import QApplication
                    if theme and theme.lower().startswith('dark'):
                        QApplication.instance().setStyleSheet(self.DARK_STYLESHEET_EXTENDED)
                except Exception:
                    pass
            try:
                self.set_compact_mode(compact_pref)
            except Exception:
                pass
        except Exception:
            pass
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # =====================================================
        # MENU BAR
        # =====================================================
        menubar = self.menuBar()
        
        # Menu Fichier
        file_menu = menubar.addMenu("Fichier")
        
        export_action = file_menu.addAction("Exporter les Donnees")
        export_action.triggered.connect(lambda: self.export_data_from_menu())
        export_action.setToolTip("Exporter la vue actuelle ou l'ensemble des donnees en CSV")
        
        file_menu.addSeparator()
        
        clear_db_action = file_menu.addAction("Effacer la Base de Donnees")
        clear_db_action.triggered.connect(self.clear_database)
        clear_db_action.setToolTip("Supprimer toutes les entrees de la base de donnees selectionnee (utiliser avec prudence)")
        
        file_menu.addSeparator()
        
        exit_action = file_menu.addAction("Quitter")
        exit_action.triggered.connect(self.close)
        
        # Menu Edition
        edit_menu = menubar.addMenu("Edition")
        
        remove_dupes_action = edit_menu.addAction("Supprimer les Doublons")
        remove_dupes_action.triggered.connect(self.remove_duplicates)
        remove_dupes_action.setToolTip("Supprimer les entrees dupliquees de la base de donnees")
        
        delete_action = edit_menu.addAction("Supprimer les Lignes Selectionnees")
        delete_action.triggered.connect(self.delete_selected_rows)
        delete_action.setToolTip("Supprimer les lignes selectionnees du tableau")

        delete_filtered_action = edit_menu.addAction("Supprimer les Entrees Filtrees")
        delete_filtered_action.setToolTip("Supprimer toutes les entrees de la base de donnees correspondant aux filtres actuels (course ou hippodrome+date)")
        delete_filtered_action.triggered.connect(self.delete_filtered_entries)
        
        edit_menu.addSeparator()
        
        update_race_action = edit_menu.addAction("Mettre e Jour la Course Filtree")
        update_race_action.triggered.connect(self.update_single_race)
        update_race_action.setToolTip("Recuperer les donnees les plus recentes (cotes, poids, etc.) pour la course filtree")
        
        edit_menu.addSeparator()
        
        settings_action = edit_menu.addAction("Parametres")
        settings_action.triggered.connect(self.open_settings_dialog)
        settings_action.setToolTip("Ouvrir les parametres de l'application")
        
        # Menu Vue
        view_menu = menubar.addMenu("Affichage")
        
        reset_action = view_menu.addAction("Reinitialiser l'apparence")
        reset_action.triggered.connect(lambda: self.reset_view())
        reset_action.setToolTip("Effacer les filtres et recharger les donnees des bases de donnees")

        # Basculer le panneau de controle
        toggle_ctrl_action = view_menu.addAction("Afficher | Masquer les filtres")
        toggle_ctrl_action.setCheckable(True)
        toggle_ctrl_action.setChecked(True)
        toggle_ctrl_action.triggered.connect(self.toggle_control_panel)
        toggle_ctrl_action.setToolTip("Afficher ou masquer la barre de controle compacte laterale")
        self.control_panel_action = toggle_ctrl_action

        # Dark theme toggle
        dark_action = view_menu.addAction("Theme Sombre")
        dark_action.setCheckable(True)
        try:
            dark_pref = bool(self.settings.value('dark_theme', False, type=bool))
        except Exception:
            dark_pref = False
        dark_action.setChecked(dark_pref)
        dark_action.triggered.connect(self.toggle_dark_theme)
        dark_action.setToolTip("Basculer vers le theme sombre pour l'application")
        self.dark_theme_action = dark_action
        
        # Compact mode (persisted)
        compact_action = view_menu.addAction("Mode Compact")
        compact_action.setCheckable(True)
        try:
            compact_pref = bool(self.settings.value('compact_mode', False, type=bool))
        except Exception:
            compact_pref = False
        compact_action.setChecked(compact_pref)
        compact_action.triggered.connect(lambda checked: self.set_compact_mode(bool(checked)))
        compact_action.setToolTip("Basculer en mode UI compact (espacement et entrees reduits)")
        self.compact_action = compact_action

        # Betting analysis toggle (persisted)
        analysis_action = view_menu.addAction("Afficher Analyse de Course")
        analysis_action.setCheckable(True)
        analysis_action.setChecked(self.show_analysis_enabled)
        analysis_action.triggered.connect(self.toggle_betting_analysis)
        analysis_action.setToolTip("Afficher ou masquer la section d'analyse de course (apparaet quand une course est filtree)")
        self.analysis_action = analysis_action

        # Sous-menu Theme avec choix
        theme_menu = view_menu.addMenu("Theme")
        from PyQt5.QtWidgets import QActionGroup
        theme_group = QActionGroup(self)
        light_theme_action = theme_menu.addAction("Clair")
        light_theme_action.setCheckable(True)
        light_theme_action.triggered.connect(lambda: self.apply_theme('Light'))
        theme_group.addAction(light_theme_action)
        dark_theme_action = theme_menu.addAction("Sombre")
        dark_theme_action.setCheckable(True)
        dark_theme_action.triggered.connect(lambda: self.apply_theme('Dark'))
        theme_group.addAction(dark_theme_action)
        hc_theme_action = theme_menu.addAction("Contraste Eleve Sombre")
        hc_theme_action.setCheckable(True)
        hc_theme_action.triggered.connect(lambda: self.apply_theme('HighContrast'))
        theme_group.addAction(hc_theme_action)
        # set checked based on current setting
        try:
            cur_theme = str(self.settings.value('theme', 'Light'))
            if cur_theme and cur_theme.lower().startswith('dark'):
                dark_theme_action.setChecked(True)
            elif cur_theme and cur_theme.lower().startswith('high'):
                hc_theme_action.setChecked(True)
            else:
                light_theme_action.setChecked(True)
        except Exception:
            light_theme_action.setChecked(True)
        
        view_menu.addSeparator()
        
        # Export actions (created but not added to Affichage/view menu — placed under Analyse menu)
        self.export_png_action = QAction("Exporter Analyse (PNG)", self)
        self.export_png_action.triggered.connect(self.save_analysis_as_png)
        self.export_png_action.setToolTip("Exporter l'analyse de course en tant qu'image PNG (sans les paris generes)")
        self.export_png_action.setEnabled(False)  # Disabled until race is filtered

        self.export_pdf_action = QAction("Exporter Analyse (PDF)", self)
        self.export_pdf_action.triggered.connect(self.save_analysis_as_pdf)
        self.export_pdf_action.setToolTip("Exporter l'analyse de course en tant que document PDF (sans les paris generes)")
        self.export_pdf_action.setEnabled(False)  # Disabled until race is filtered

        self.print_analysis_action = QAction("Imprimer l'Analyse", self)
        self.print_analysis_action.triggered.connect(self.print_analysis)
        self.print_analysis_action.setToolTip("Imprimer l'analyse de course avec l'imprimante par defaut")
        self.print_analysis_action.setEnabled(False)  # Disabled until race is filtered

        self.export_combos_action = QAction("Exporter Combinaisons (PDF)", self)
        self.export_combos_action.triggered.connect(self.save_combinations_only)
        self.export_combos_action.setToolTip("Exporter les combinaisons generees en tant que document PDF")
        self.export_combos_action.setEnabled(False)  # Disabled until race is filtered
        
        view_menu.addSeparator()

        # Standalone Analyse menu: group analysis/export/print/combination options
        analyse_menu = menubar.addMenu("Analyse")
        analyse_menu.addAction(self.export_png_action)
        analyse_menu.addAction(self.export_pdf_action)
        analyse_menu.addAction(self.print_analysis_action)
        analyse_menu.addSeparator()
        analyse_menu.addAction(self.export_combos_action)

        # Menu Paris (Bets)
        bets_menu = menubar.addMenu("Paris")
        
        self.advanced_bet_gen_action = bets_menu.addAction("🎲 Generateur de Combinaisons")
        self.advanced_bet_gen_action.triggered.connect(self.open_bet_generator_window)
        self.advanced_bet_gen_action.setToolTip("Ouvrir le generateur avance de paris avec strategies independantes")
        self.advanced_bet_gen_action.setEnabled(False)  # Disabled until race is filtered
        
        # Menu Aide
        help_menu = menubar.addMenu("Aide")
        
        help_action = help_menu.addAction("📖 Guide d'Utilisation")
        help_action.triggered.connect(self.show_help)
        
        help_menu.addSeparator()
        
        tips_action = help_menu.addAction("💡 Conseils d'Analyse")
        tips_action.triggered.connect(self.show_tips)
        
        metrics_action = help_menu.addAction("📊 Metriques Expliquees")
        metrics_action.triggered.connect(self.show_metrics)
        
        help_menu.addSeparator()
        
        about_action = help_menu.addAction("ℹ️ A Propos")
        about_action.triggered.connect(self.show_about)
        
        # Main layout
        layout = QVBoxLayout(central_widget)
        
        # URL input section
        url_layout = QHBoxLayout()
        
        # Add app icon and title
        icon_label = QLabel()
        icon_pixmap = QPixmap('assets/racer.png')
        if not icon_pixmap.isNull():
            icon_pixmap = icon_pixmap.scaledToHeight(64, Qt.SmoothTransformation)
            icon_label.setPixmap(icon_pixmap)
            icon_label.setMinimumWidth(120)
            icon_label.setAlignment(Qt.AlignCenter)
        url_layout.addWidget(icon_label)
        
        # Add title text
        title_label = QLabel("VOTRE OUTIL D'INTELLIGENCE ET D'ANALYSE DES COURSES HIPPIQUES")
        title_font = QFont()
        title_font.setPointSize(11)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("color: #2c3e50;")
        url_layout.addWidget(title_label)
        
        url_layout.addStretch()
        
        # Date picker for selecting date
        url_layout.addWidget(QLabel("Date :"))
        self.date_picker = QDateEdit()
        self.date_picker.setDate(QDate.currentDate())
        self.date_picker.setDisplayFormat("dd/MM/yyyy")
        self.date_picker.setStyleSheet(self.INPUT_STYLESHEET)
        # Don't connect signal yet - wait until all widgets are created
        url_layout.addWidget(self.date_picker)
        
        url_layout.addWidget(QLabel("Réunion :"))
        self.url_combo = QComboBox()
        self.url_combo.setPlaceholderText("Chargement des réunions...")
        self.url_combo.setStyleSheet(self.INPUT_STYLESHEET)
        url_layout.addWidget(self.url_combo)
        
        # Refresh button to reload meetings
        self.refresh_meetings_btn = QPushButton("🔄 Rafraîchir")
        self.refresh_meetings_btn.setStyleSheet(self.BUTTON_STYLESHEET)
        self.refresh_meetings_btn.setMaximumWidth(100)
        self.refresh_meetings_btn.clicked.connect(self.load_meeting_urls)
        url_layout.addWidget(self.refresh_meetings_btn)
        
        # Race type combo hidden - auto-detected from HTML
        self.race_type_combo = QComboBox()
        self.race_type_combo.setStyleSheet(self.INPUT_STYLESHEET)
        self.race_type_combo.addItems(["Plat", "Trot"])
        self.race_type_combo.setCurrentIndex(0)  # Default to Flat
        self.race_type_combo.setVisible(False)  # Hidden - auto-detected from URL
        url_layout.addWidget(self.race_type_combo)
        
        # Instance variable to store auto-detected race type
        self.detected_race_type = 'flat'
        
        self.scrape_btn = QPushButton("Scraper les Courses")
        self.scrape_btn.setStyleSheet(self.BUTTON_STYLESHEET)
        self.scrape_btn.clicked.connect(self.start_scraping)
        url_layout.addWidget(self.scrape_btn)
        
        # Connect URL selection to auto-detect race type
        self.url_combo.currentIndexChanged.connect(self.on_url_selected)
        
        # Cancel button to stop ongoing download
        self.cancel_btn = QPushButton("Annuler")
        self.cancel_btn.setStyleSheet(self.BUTTON_STYLESHEET)
        self.cancel_btn.setMaximumWidth(80)
        self.cancel_btn.clicked.connect(self.cancel_scraping)
        self.cancel_btn.setEnabled(False)  # Disabled by default
        url_layout.addWidget(self.cancel_btn)
        
        layout.addLayout(url_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Main content layout (horizontal)
        content_layout = QHBoxLayout()
        
        # Left side - Data view
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        
        # Tab widget for different views
        self.tab_widget = QTabWidget()
        
        # Current data tab
        self.current_table = QTableWidget()
        self.current_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.current_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.current_table.setAlternatingRowColors(True)
        self.tab_widget.addTab(self.current_table, "Scrape Actuel")
        
        # Database view tab
        self.db_table = QTableWidget()
        self.db_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.db_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.db_table.setAlternatingRowColors(True)
        self.tab_widget.addTab(self.db_table, "Apercu des Donnees")
        
        # Set Database View as default tab
        self.tab_widget.setCurrentIndex(1)
        
        left_layout.addWidget(self.tab_widget)
        
        # Horizontal widget below data view for statistics display
        self.horizontal_widget = QWidget()
        self.horizontal_widget.setMaximumHeight(150)
        self.horizontal_widget.setStyleSheet("""
            QWidget {
                background-color: #f8f9fa;
                border-top: 2px solid #dee2e6;
            }
        """)
        horizontal_layout = QHBoxLayout(self.horizontal_widget)
        horizontal_layout.setContentsMargins(10, 6, 10, 6)
        horizontal_layout.setSpacing(12)
        horizontal_layout.addWidget(QLabel("Stats de la base de donnees :"))
        self.stats_label = QLabel("Aucune donnee")
        self.stats_label.setStyleSheet("font-weight: bold; color: #2c3e50;")
        self.stats_label.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        horizontal_layout.addWidget(self.stats_label, 0)
        
        horizontal_layout.addStretch()
        
        # Set size policy on horizontal_widget to allow it to expand
        self.horizontal_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        left_layout.addWidget(self.horizontal_widget)
        
        content_layout.addWidget(left_widget, 3)  # 3/4 width
        
        # Right side - Vertical widget
        self.vertical_widget = QWidget()
        self.vertical_widget.setMaximumWidth(250)
        vertical_layout = QVBoxLayout(self.vertical_widget)
        
        # Filters section
        vertical_layout.addWidget(QLabel("Filtres :"))
        
        # Filtre type de course
        vertical_layout.addWidget(QLabel("Type de Course :"))
        self.race_type_filter = QComboBox()
        self.race_type_filter.addItems(["Plat", "Trot"])
        self.race_type_filter.setCurrentIndex(0)
        self.race_type_filter.currentTextChanged.connect(self.on_race_type_changed)
        vertical_layout.addWidget(self.race_type_filter)
        
        vertical_layout.addWidget(QLabel("Date de Course :"))
        self.date_filter = QComboBox()
        self.date_filter.setStyleSheet(self.INPUT_STYLESHEET)
        self.date_filter.setEditable(True)
        self.date_filter.setInsertPolicy(QComboBox.NoInsert)
        self.date_filter.setMaximumWidth(200)
        vertical_layout.addWidget(self.date_filter)

        vertical_layout.addWidget(QLabel("ID Course :"))
        self.race_id_filter = QComboBox()
        self.race_id_filter.setStyleSheet(self.INPUT_STYLESHEET)
        self.race_id_filter.setEditable(True)
        self.race_id_filter.setInsertPolicy(QComboBox.NoInsert)
        self.race_id_filter.setMaximumWidth(200)
        vertical_layout.addWidget(self.race_id_filter)

        # HIDDEN: Race Track widget (to be reinstated later)
        # vertical_layout.addWidget(QLabel("Race Track:"))
        self.track_filter = QComboBox()
        self.track_filter.setStyleSheet(self.INPUT_STYLESHEET)
        self.track_filter.setEditable(True)
        self.track_filter.setInsertPolicy(QComboBox.NoInsert)
        self.track_filter.setMaximumWidth(200)
        # vertical_layout.addWidget(self.track_filter)  # HIDDEN
        # Apply compact input style to sidebar filters
        try:
            self.date_filter.setStyleSheet(self.COMPACT_INPUT_STYLESHEET)
            self.race_id_filter.setStyleSheet(self.COMPACT_INPUT_STYLESHEET)
            self.track_filter.setStyleSheet(self.COMPACT_INPUT_STYLESHEET)
        except Exception:
            pass
        self.date_filter.setToolTip("Filtrer les courses par date (correspondance exacte). Tapez pour rechercher.")
        self.race_id_filter.setToolTip("Filtrer les courses par ID de Course (correspondance exacte)")
        self.track_filter.setToolTip("Filtrer par nom d'hippodrome (correspondance exacte)")
        
        filter_btn = QPushButton("Appliquer les Filtres")
        filter_btn.setStyleSheet(self.BUTTON_STYLESHEET)
        filter_btn.setMinimumHeight(28)
        filter_btn.clicked.connect(self.apply_filters)
        filter_btn.setToolTip("Appliquer les filtres selectionnes e la vue donnees")
        filter_btn.setEnabled(False)  # DEACTIVATED (to be reinstated later)
        # vertical_layout.addWidget(filter_btn)  # HIDDEN
        self.filter_btn = filter_btn  # Store reference for later
        
        # Bouton Reset consolide : efface les filtres et recharge les donnees
        reset_btn = QPushButton("Reinitialiser")
        reset_btn.setStyleSheet(self.CLEAR_BUTTON_STYLESHEET)
        reset_btn.setMinimumHeight(28)
        reset_btn.clicked.connect(self.reset_view)
        reset_btn.setToolTip("Effacer les filtres et recharger les donnees des bases de donnees")
        vertical_layout.addWidget(reset_btn)

        # Manual analysis button - allows user to trigger analysis without auto-open
        manual_analysis_btn = QPushButton("📊 Charger l'Analyse")
        manual_analysis_btn.setStyleSheet(self.BUTTON_STYLESHEET)
        manual_analysis_btn.setMinimumHeight(28)
        manual_analysis_btn.clicked.connect(self.trigger_manual_analysis)
        manual_analysis_btn.setToolTip("Charger manuellement l'analyse pour la course selectionnee")
        vertical_layout.addWidget(manual_analysis_btn)
        self.manual_analysis_btn = manual_analysis_btn  # Store reference for later

        # Auto-filter when combo selections/text change
        self.date_filter.currentTextChanged.connect(lambda _: self.apply_filters())
        self.race_id_filter.currentTextChanged.connect(lambda _: self.apply_filters())
        self.track_filter.currentTextChanged.connect(lambda _: self.apply_filters())
        
        # Store current filter values for toggle purposes
        self.current_filtered_df = None
        self.current_race_id_filter = ""
        self.current_date_filter = ""
        self.current_track_filter = ""
        
        # HIDDEN: Actions widget (to be reinstated later)
        # vertical_layout.addWidget(QLabel("Actions:"))
        # removed separate Refresh button (functionality merged into Reset)
        vertical_layout.addStretch()
        
        content_layout.addWidget(self.vertical_widget, 1)  # 1/4 width
        
        layout.addLayout(content_layout)
        
        # =====================================================
        # ANALYSIS SECTION (shown when single race is filtered)
        # =====================================================
        self.analysis_widget = QWidget()
        self.analysis_widget.setVisible(False)
        analysis_layout = QVBoxLayout(self.analysis_widget)
        analysis_layout.setContentsMargins(5, 5, 5, 5)
        
        self.analysis_title = QLabel("? Analyse de Course (Une Seule Course)")
        title_font = QFont("Arial", 10, QFont.Bold)
        title_font.setStyleStrategy(QFont.PreferAntialias)
        self.analysis_title.setFont(title_font)
        analysis_layout.addWidget(self.analysis_title)

        # Load persisted metric weights from QSettings (no inline sliders)
        self.last_filtered_df = None
        self.current_composite_df = None
        # NOTE: QSettings already initialized above at __init__ start
        default_perc = {'IC':20, 'S_COEFF':20, 'IF':15, 'n_weight':10, 'COTE':20, 'Corde':10, 'VALEUR':5}
        self.metric_weights = {}
        for metric in ['IC','S_COEFF','IF','n_weight','COTE','Corde','VALEUR']:
            try:
                saved = int(self.settings.value(metric, default_perc.get(metric, 10)))
            except Exception:
                saved = default_perc.get(metric, 10)
            self.metric_weights[metric] = float(saved)/100.0
        
        # Create horizontal tabs for different analyses
        analysis_tabs = QTabWidget()
        analysis_tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #d0d0d0; }
            QTabBar::tab {
                background-color: #ecf0f1;
                color: #2c3e50;
                padding: 8px 20px;
                margin-right: 2px;
                min-width: 100px;
                border: 1px solid #bdc3c7;
                border-bottom: none;
                border-radius: 4px 4px 0px 0px;
                font-weight: bold;
            }
            QTabBar::tab:selected {
                background-color: #3498db;
                color: white;
                border: 1px solid #2980b9;
            }
            QTabBar::tab:hover {
                background-color: #2ecc71;
            }
        """)

        # --- Summary tab (composite + heatmap) ---
        self.summary_widget = QWidget()
        s_layout = QHBoxLayout(self.summary_widget)
        self.composite_table = QTableWidget()
        self.composite_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.composite_table.setMaximumWidth(300)
        self.composite_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.composite_table.setAlternatingRowColors(True)
        s_layout.addWidget(self.composite_table, 1)

        # *** Internal bets table (used by generate_bets() for BetGeneratorWindow) ***
        # This table holds generated betting combinations and is read by BetGeneratorWindow
        self.bets_table = QTableWidget()
        self.bets_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.bets_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.bets_table.setAlternatingRowColors(True)
        # Note: Not added to UI - only for internal use by generate_bets()
        print("[DEBUG] RaceScraperApp: Created self.bets_table for bet generation")

        # Scrollable heatmap container with expand button
        heatmap_widget = QWidget()
        heatmap_layout = QVBoxLayout(heatmap_widget)
        heatmap_layout.setContentsMargins(0, 0, 0, 0)
        
        # Top bar: expand button + external heatmap controls
        top_bar = QWidget()
        top_bar_layout = QHBoxLayout(top_bar)
        top_bar_layout.setContentsMargins(0, 0, 0, 0)

        # Bouton pour deverrouiller le heatmap en plein ecran/nouvelle fenetre
        expand_btn = QPushButton("👁️  View Heatmap (Partants)")
        expand_btn.setMaximumHeight(30)
        expand_btn.setStyleSheet("background-color: #3498db; color: white; font-weight: bold; border-radius: 4px;")
        expand_btn.clicked.connect(self.show_heatmap_fullscreen)
        top_bar_layout.addWidget(expand_btn)

        # Create external controls widget (will be populated with columns when data is available)
        try:
            self.heatmap_controls_widget, self.heatmap_controls_dict = create_heatmap_controls(parent=self)
            top_bar_layout.addWidget(self.heatmap_controls_widget, 1)
        except Exception:
            self.heatmap_controls_widget = None
            self.heatmap_controls_dict = None

        heatmap_layout.addWidget(top_bar)
        
        # Scrollable canvas area
        self.summary_canvas_scroll = QScrollArea()
        self.summary_canvas_scroll.setWidgetResizable(True)
        self.summary_canvas_container = QWidget()
        self.summary_canvas_layout = QVBoxLayout(self.summary_canvas_container)
        self.summary_canvas_layout.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.summary_canvas_scroll.setWidget(self.summary_canvas_container)
        heatmap_layout.addWidget(self.summary_canvas_scroll, 1)
        
        s_layout.addWidget(heatmap_widget, 2)

        analysis_tabs.addTab(self.summary_widget, "📋 Synthese")

        # --- Statistiques Course tab (two-column layout with statistics and analyses) ---
        self.stats_course_widget = QWidget()
        stats_course_layout = QVBoxLayout(self.stats_course_widget)
        stats_course_layout.setContentsMargins(6, 6, 6, 6)
        stats_course_layout.setSpacing(10)

        # Create two-column scrollable area
        stats_course_scroll = QScrollArea()
        stats_course_scroll.setWidgetResizable(True)
        stats_course_scroll_widget = QWidget()
        two_col_layout = QHBoxLayout(stats_course_scroll_widget)
        two_col_layout.setContentsMargins(0, 0, 0, 0)
        two_col_layout.setSpacing(10)

        # --- LEFT COLUMN: Statistics ---
        left_col_widget = QWidget()
        left_col_layout = QVBoxLayout(left_col_widget)
        left_col_layout.setContentsMargins(0, 0, 0, 0)
        left_col_layout.setSpacing(10)

        # Summary display
        self.summary_widget_detailed = QWidget()
        self.summary_widget_detailed.setStyleSheet("background-color:#e2e3e5;border:1px solid #d6d8db;border-radius:4px;")
        self.summary_widget_detailed.setFixedHeight(50)
        summ_layout = QHBoxLayout(self.summary_widget_detailed)
        summ_layout.setContentsMargins(6, 6, 6, 6)
        summ_label_title = QLabel("📋 Synthese:")
        summ_label_title.setStyleSheet("color:#383d41;font-weight:bold;")
        summ_layout.addWidget(summ_label_title)
        self.summary_label_detailed = QLabel("")
        self.summary_label_detailed.setStyleSheet("color:#383d41;font-size:14px;")
        self.summary_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.summary_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.summary_label_detailed.setWordWrap(True)
        summ_layout.addWidget(self.summary_label_detailed)
        left_col_layout.addWidget(self.summary_widget_detailed)

        # Exclusive display
        self.exclusive_widget_detailed = QWidget()
        self.exclusive_widget_detailed.setStyleSheet("background-color:#fff3cd;border:1px solid #ffeaa7;border-radius:4px;")
        self.exclusive_widget_detailed.setFixedHeight(50)
        excl_layout = QHBoxLayout(self.exclusive_widget_detailed)
        excl_layout.setContentsMargins(6, 6, 6, 6)
        excl_label_title = QLabel("🎁 Exclusif:")
        excl_label_title.setStyleSheet("color:#856404;font-weight:bold;")
        excl_layout.addWidget(excl_label_title)
        self.exclusive_label_detailed = QLabel("")
        self.exclusive_label_detailed.setStyleSheet("color:#856404;font-size:14px;")
        self.exclusive_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.exclusive_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.exclusive_label_detailed.setWordWrap(True)
        excl_layout.addWidget(self.exclusive_label_detailed)
        left_col_layout.addWidget(self.exclusive_widget_detailed)

        # Trending display
        self.trending_widget_detailed = QWidget()
        self.trending_widget_detailed.setStyleSheet("background-color:#e7f3ff;border:1px solid #cfe8ff;border-radius:4px;")
        self.trending_widget_detailed.setFixedHeight(50)
        trend_layout = QHBoxLayout(self.trending_widget_detailed)
        trend_layout.setContentsMargins(6, 6, 6, 6)
        trend_label_title = QLabel("📈 Populaires:")
        trend_label_title.setStyleSheet("color:#0b5394;font-weight:bold;")
        trend_layout.addWidget(trend_label_title)
        self.trending_label_detailed = QLabel("")
        self.trending_label_detailed.setStyleSheet("color:#0b5394;font-size:14px;")
        self.trending_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.trending_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.trending_label_detailed.setWordWrap(True)
        trend_layout.addWidget(self.trending_label_detailed)
        left_col_layout.addWidget(self.trending_widget_detailed)

        # Race statistics display
        self.stats_widget_detailed = QWidget()
        self.stats_widget_detailed.setStyleSheet("background-color:#d4edda;border:1px solid #c3e6cb;border-radius:4px;")
        stats_layout = QVBoxLayout(self.stats_widget_detailed)
        stats_layout.setContentsMargins(6, 6, 6, 6)
        stats_title = QLabel("📊 Statistiques de Course:")
        stats_title.setStyleSheet("color:#155724;font-weight:bold;")
        stats_layout.addWidget(stats_title)
        self.stats_label_detailed = QLabel("")
        self.stats_label_detailed.setStyleSheet("color:#155724;font-size:12px;")
        self.stats_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.stats_label_detailed.setWordWrap(True)
        stats_layout.addWidget(self.stats_label_detailed)
        left_col_layout.addWidget(self.stats_widget_detailed)

        left_col_layout.addStretch()
        two_col_layout.addWidget(left_col_widget, 1)

        # --- RIGHT COLUMN: Detailed Analyses ---
        right_col_widget = QWidget()
        right_col_layout = QVBoxLayout(right_col_widget)
        right_col_layout.setContentsMargins(0, 0, 0, 0)
        right_col_layout.setSpacing(10)

        # Prognosis display
        self.prognosis_widget_detailed = QWidget()
        self.prognosis_widget_detailed.setStyleSheet("background-color:#d4edda;border:1px solid #c3e6cb;border-radius:4px;")
        self.prognosis_widget_detailed.setFixedHeight(50)
        prog_layout = QHBoxLayout(self.prognosis_widget_detailed)
        prog_layout.setContentsMargins(6, 6, 6, 6)
        prog_label_title = QLabel("🎯 Pronostics:")
        prog_label_title.setStyleSheet("color:#155724;font-weight:bold;")
        prog_layout.addWidget(prog_label_title)
        self.prognosis_label_detailed = QLabel("")
        self.prognosis_label_detailed.setStyleSheet("color:#155724;font-size:14px;")
        self.prognosis_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.prognosis_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.prognosis_label_detailed.setWordWrap(True)
        prog_layout.addWidget(self.prognosis_label_detailed)
        right_col_layout.addWidget(self.prognosis_widget_detailed)

        # Prognosis-only display
        self.prognosis_only_widget_detailed = QWidget()
        self.prognosis_only_widget_detailed.setStyleSheet("background-color:#f0e6ff;border:1px solid #e1d7ff;border-radius:4px;")
        self.prognosis_only_widget_detailed.setFixedHeight(50)
        po_layout = QHBoxLayout(self.prognosis_only_widget_detailed)
        po_layout.setContentsMargins(6, 6, 6, 6)
        po_label_title = QLabel("🔍 Pred Only:")
        po_label_title.setStyleSheet("color:#5a2a86;font-weight:bold;")
        po_layout.addWidget(po_label_title)
        self.prognosis_only_label_detailed = QLabel("")
        self.prognosis_only_label_detailed.setStyleSheet("color:#5a2a86;font-size:14px;")
        self.prognosis_only_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.prognosis_only_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.prognosis_only_label_detailed.setWordWrap(True)
        po_layout.addWidget(self.prognosis_only_label_detailed)
        right_col_layout.addWidget(self.prognosis_only_widget_detailed)

        # Summary+Prognosis intersection display
        self.summary_prognosis_widget_detailed = QWidget()
        self.summary_prognosis_widget_detailed.setStyleSheet("background-color:#d1ecf1;border:1px solid #bee5eb;border-radius:4px;")
        self.summary_prognosis_widget_detailed.setFixedHeight(50)
        sp_layout = QHBoxLayout(self.summary_prognosis_widget_detailed)
        sp_layout.setContentsMargins(6, 6, 6, 6)
        sp_label_title = QLabel("🎪 Resume + pronostic:")
        sp_label_title.setStyleSheet("color:#0c5460;font-weight:bold;")
        sp_layout.addWidget(sp_label_title)
        self.summary_prognosis_label_detailed = QLabel("")
        self.summary_prognosis_label_detailed.setStyleSheet("color:#0c5460;font-size:14px;")
        self.summary_prognosis_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.summary_prognosis_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.summary_prognosis_label_detailed.setWordWrap(True)
        sp_layout.addWidget(self.summary_prognosis_label_detailed)
        right_col_layout.addWidget(self.summary_prognosis_widget_detailed)

        # Favorable cordes display
        self.favorable_cordes_widget_detailed = QWidget()
        self.favorable_cordes_widget_detailed.setStyleSheet("background-color:#fff3cd;border:1px solid #ffc107;border-radius:4px;")
        self.favorable_cordes_widget_detailed.setFixedHeight(50)
        cordes_layout = QHBoxLayout(self.favorable_cordes_widget_detailed)
        cordes_layout.setContentsMargins(6, 6, 6, 6)
        cordes_label_title = QLabel("🎵 Cordes Favorables:")
        cordes_label_title.setStyleSheet("color:#856404;font-weight:bold;")
        cordes_layout.addWidget(cordes_label_title)
        self.favorable_cordes_label_detailed = QLabel("")
        self.favorable_cordes_label_detailed.setStyleSheet("color:#856404;font-size:14px;")
        self.favorable_cordes_label_detailed.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.favorable_cordes_label_detailed.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.favorable_cordes_label_detailed.setWordWrap(True)
        cordes_layout.addWidget(self.favorable_cordes_label_detailed)
        right_col_layout.addWidget(self.favorable_cordes_widget_detailed)

        right_col_layout.addStretch()
        two_col_layout.addWidget(right_col_widget, 1)

        stats_course_scroll.setWidget(stats_course_scroll_widget)
        stats_course_layout.addWidget(stats_course_scroll)

        analysis_tabs.addTab(self.stats_course_widget, "📊 Statistiques Course")

        # --- Dashboard tab (historical + filtered insights) ---
        self.dashboard_widget = QWidget()
        db_layout = QVBoxLayout(self.dashboard_widget)
        # Summary cards row
        cards_row = QWidget()
        cards_layout = QHBoxLayout(cards_row)
        cards_layout.setContentsMargins(6,6,6,6)
        self.dashboard_total_label = QLabel("Rows: 0")
        self.dashboard_total_label.setStyleSheet("font-weight:bold; font-size:12px;")
        cards_layout.addWidget(self.dashboard_total_label)
        self.dashboard_races_label = QLabel("Races: 0")
        cards_layout.addWidget(self.dashboard_races_label)
        self.dashboard_tracks_label = QLabel("Tracks: 0")
        cards_layout.addWidget(self.dashboard_tracks_label)
        self.dashboard_avg_odds_label = QLabel("Avg Odds: e?")
        cards_layout.addWidget(self.dashboard_avg_odds_label)
        self.dashboard_median_comp_label = QLabel("Median Comp: e?")
        cards_layout.addWidget(self.dashboard_median_comp_label)
        cards_layout.addStretch()
        db_layout.addWidget(cards_row)

        # Distance segmentation controls for dashboard (All / Short / Medium / Long)
        dist_ctrl_row = QWidget()
        dist_ctrl_layout = QHBoxLayout(dist_ctrl_row)
        dist_ctrl_layout.setContentsMargins(6,2,6,2)
        
        # Race type filter (Flat/Obstacles/All)
        dist_ctrl_layout.addWidget(QLabel("Race Type:"))
        self.dashboard_racetype_combo = QComboBox()
        self.dashboard_racetype_combo.addItems(["All", "Flat", "Obstacles"])
        try:
            self.dashboard_racetype_combo.setStyleSheet(self.INPUT_STYLESHEET)
        except Exception:
            pass
        dist_ctrl_layout.addWidget(self.dashboard_racetype_combo)
        
        # HIPPODROME (track) filter for plat historical data
        dist_ctrl_layout.addWidget(QLabel("Track:"))
        self.dashboard_hippodrome_combo = QComboBox()
        self.dashboard_hippodrome_combo.addItem("All")
        try:
            self.dashboard_hippodrome_combo.setStyleSheet(self.INPUT_STYLESHEET)
        except Exception:
            pass
        dist_ctrl_layout.addWidget(self.dashboard_hippodrome_combo)
        
        dist_ctrl_layout.addWidget(QLabel("Distance view:"))
        self.dashboard_distance_combo = QComboBox()
        self.dashboard_distance_combo.addItems(["All","Short (<1600m)","Medium (1600-2400m)","Long (>2400m)"])
        try:
            self.dashboard_distance_combo.setStyleSheet(self.INPUT_STYLESHEET)
        except Exception:
            pass
        dist_ctrl_layout.addWidget(self.dashboard_distance_combo)
        dist_ctrl_layout.addStretch()
        db_layout.addWidget(dist_ctrl_row)

        # Plots area: Composite histogram + Odds histogram side-by-side
        plots_row = QWidget()
        plots_layout = QHBoxLayout(plots_row)
        plots_layout.setContentsMargins(6,6,6,6)

        # Composite histogram canvas
        try:
            fig1 = plt.figure(figsize=(4,3))
            self.dashboard_comp_canvas = FigureCanvas(fig1)
        except Exception:
            self.dashboard_comp_canvas = QLabel("Composite plot unavailable")
        plots_layout.addWidget(self.dashboard_comp_canvas, 1)

        # Odds histogram canvas
        try:
            fig2 = plt.figure(figsize=(4,3))
            self.dashboard_odds_canvas = FigureCanvas(fig2)
        except Exception:
            self.dashboard_odds_canvas = QLabel("Odds plot unavailable")
        plots_layout.addWidget(self.dashboard_odds_canvas, 1)

        db_layout.addWidget(plots_row, 1)
        # Insights row: CORDE performance, sex distribution, and quick textual insights
        insights_row = QWidget()
        insights_layout = QHBoxLayout(insights_row)
        insights_layout.setContentsMargins(6,6,6,6)
        try:
            fig3 = plt.figure(figsize=(4,2.5))
            self.dashboard_corde_canvas = FigureCanvas(fig3)
        except Exception:
            self.dashboard_corde_canvas = QLabel("Corde plot unavailable")
        insights_layout.addWidget(self.dashboard_corde_canvas, 1)
        
        # Sex distribution pie chart canvas
        try:
            fig_sex = plt.figure(figsize=(3,2.5))
            self.dashboard_sex_canvas = FigureCanvas(fig_sex)
        except Exception:
            self.dashboard_sex_canvas = QLabel("Sex distribution unavailable")
        insights_layout.addWidget(self.dashboard_sex_canvas, 1)
        
        self.dashboard_insights_label = QLabel("Insights: e?")
        self.dashboard_insights_label.setWordWrap(True)
        self.dashboard_insights_label.setMinimumWidth(320)
        insights_layout.addWidget(self.dashboard_insights_label, 1)
        db_layout.addWidget(insights_row)

        # POIDS (weight) visualization row: shows relationship between POIDS and finishing position
        poids_row = QWidget()
        poids_layout = QHBoxLayout(poids_row)
        poids_layout.setContentsMargins(6,6,6,6)
        try:
            fig_poids = plt.figure(figsize=(8,2.5))
            self.dashboard_poids_canvas = FigureCanvas(fig_poids)
        except Exception:
            self.dashboard_poids_canvas = QLabel("POIDS plot unavailable")
        poids_layout.addWidget(self.dashboard_poids_canvas)
        db_layout.addWidget(poids_row)
        
        # N_WEIGHT (weight difference) visualization row: violin + place-rate by weight-change category
        nweight_row = QWidget()
        nweight_layout = QHBoxLayout(nweight_row)
        nweight_layout.setContentsMargins(6,6,6,6)
        try:
            fig_nweight = plt.figure(figsize=(8,2.5))
            self.dashboard_nweight_canvas = FigureCanvas(fig_nweight)
        except Exception:
            self.dashboard_nweight_canvas = QLabel("N_WEIGHT plot unavailable")
        nweight_layout.addWidget(self.dashboard_nweight_canvas)
        db_layout.addWidget(nweight_row)
        
        # HIDDEN: Dashboard tab (to be reinstated later)
        # Add Dashboard as a top-level tab (after Database View) and make it scrollable
        try:
            dash_scroll = QScrollArea()
            dash_scroll.setWidgetResizable(True)
            dash_scroll.setWidget(self.dashboard_widget)
            # self.tab_widget.addTab(dash_scroll, "Dashboard")  # HIDDEN
        except Exception:
            # fallback: add non-scrollable widget
            try:
                # self.tab_widget.addTab(self.dashboard_widget, "Dashboard")  # HIDDEN
                pass
            except Exception:
                # analysis_tabs.addTab(self.dashboard_widget, "Dashboard")  # HIDDEN
                pass

        # Connect distance combo to update dashboard when changed
        try:
            self.dashboard_distance_combo.currentIndexChanged.connect(lambda _: self.update_dashboard(self.last_filtered_df if hasattr(self, 'last_filtered_df') else None))
        except Exception:
            pass

        # Connect hippodrome combo to update dashboard when changed
        try:
            self.dashboard_hippodrome_combo.currentIndexChanged.connect(lambda _: self.update_dashboard(None))
        except Exception:
            pass

        # Connect race type combo to update dashboard when changed
        try:
            self.dashboard_racetype_combo.currentIndexChanged.connect(lambda _: self.update_dashboard(None))
        except Exception:
            pass

        # --- Horse cards layout (for individual race display, not in tabs) ---
        self.cards_scroll = QScrollArea()
        self.cards_scroll.setWidgetResizable(True)
        cards_inner = QWidget()
        cards_inner.setLayout(QVBoxLayout())
        self.cards_layout = cards_inner.layout()
        self.cards_layout.setAlignment(Qt.AlignTop)
        self.cards_scroll.setWidget(cards_inner)
        # Note: cards_scroll is not added to analysis_tabs anymore

        # All horses detailed table (show stats for every horse)
        self.all_horses_table = QTableWidget()
        self.all_horses_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.all_horses_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.all_horses_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.all_horses_table, "🐴 Tous les Chevaux")

        # Fitness (IF) analysis table
        self.fitness_table = QTableWidget()
        self.fitness_table.setMaximumHeight(250)
        self.fitness_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.fitness_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.fitness_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.fitness_table, "💪 Forme (IF)")

        # Class IC analysis table
        self.ic_table = QTableWidget()
        self.ic_table.setMaximumHeight(250)
        self.ic_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.ic_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.ic_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.ic_table, "🏆 Classe (IC)")

        # Success Coefficient analysis table
        self.success_table = QTableWidget()
        self.success_table.setMaximumHeight(250)
        self.success_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.success_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.success_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.success_table, "✅ Coeff Succes")

        # Weight Stability analysis table
        self.weight_stab_table = QTableWidget()
        self.weight_stab_table.setMaximumHeight(250)
        self.weight_stab_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.weight_stab_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.weight_stab_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.weight_stab_table, "⚖️ Poids Stable")

        # Tableau d'analyse Poids Leger
        self.light_weight_table = QTableWidget()
        self.light_weight_table.setMaximumHeight(250)
        self.light_weight_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.light_weight_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.light_weight_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.light_weight_table, "🦵 Poids Leger")

        # *** NEW: Outsiders (Divergence) analysis table ***
        self.outsiders_divergence_table = QTableWidget()
        self.outsiders_divergence_table.setMaximumHeight(250)
        self.outsiders_divergence_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.outsiders_divergence_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.outsiders_divergence_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.outsiders_divergence_table, "📈 Outsiders (Divergence)")

        # *** NEW: Outsiders (Consistency) analysis table ***
        self.outsiders_consistency_table = QTableWidget()
        self.outsiders_consistency_table.setMaximumHeight(250)
        self.outsiders_consistency_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.outsiders_consistency_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.outsiders_consistency_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.outsiders_consistency_table, "📉 Outsiders (Consistance)")

        # *** NEW: Underperforming Favorites analysis table ***
        self.underperforming_favorites_table = QTableWidget()
        self.underperforming_favorites_table.setMaximumHeight(250)
        self.underperforming_favorites_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.underperforming_favorites_table.setStyleSheet(self.TABLE_STYLESHEET)
        self.underperforming_favorites_table.setAlternatingRowColors(True)
        analysis_tabs.addTab(self.underperforming_favorites_table, "⚠️  Favorites (Underperforming)")

        # NOTE: Bet Generator moved to separate window (Paris menu ? G?n?rateur Avanc?)
        analysis_layout.addWidget(analysis_tabs)
        
        # Wrap analysis widget in a scrollable container for better visibility
        self.analysis_scroll = QScrollArea()
        self.analysis_scroll.setWidgetResizable(True)
        self.analysis_scroll.setWidget(self.analysis_widget)
        self.analysis_scroll.setVisible(False)  # Hidden initially, shown when race is filtered
        layout.addWidget(self.analysis_scroll, 1)  # Give it proportional space
        # Status area: small color indicator + text label
        status_area = QWidget()
        status_layout = QHBoxLayout(status_area)
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(6)
        self.status_icon = QLabel()
        self.status_icon.setFixedSize(14, 14)
        self.status_icon.setStyleSheet("background-color: #9ea7ad; border-radius: 7px;")
        self.status_icon.setToolTip("Indicateur d'etat : vert=succes, orange=en cours, rouge=erreur")
        self.status_label = QLabel("Pret")
        status_layout.addWidget(self.status_icon, 0, Qt.AlignVCenter)
        status_layout.addWidget(self.status_label, 1)
        status_layout.addStretch()
        layout.addWidget(status_area)
        
        # Now that all widgets are created, connect the date picker signal
        self.date_picker.dateChanged.connect(self.on_date_changed)
        
        # Load database data on startup
        self.load_database_data()
        
        # Load meeting URLs from zone-turf.fr
        self.load_meeting_urls()
    
    def on_url_selected(self):
        """Auto-detect race type when URL is selected from dropdown."""
        url = self.url_combo.currentData()
        if not url:
            self.detected_race_type = 'flat'
            return
        
        try:
            from bs4 import BeautifulSoup
            import requests
            from model_functions import detect_race_type_from_html
            
            # Fetch the HTML
            response = requests.get(url, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Detect race type from HTML
            self.detected_race_type = detect_race_type_from_html(soup)
            print(f"[DEBUG] Auto-detected race type: {self.detected_race_type}")
            
        except Exception as e:
            print(f"[ERROR] Failed to auto-detect race type: {e}")
            self.detected_race_type = 'flat'
    
    def start_scraping(self):
        url = self.url_combo.currentData()
        if not url:
            QMessageBox.warning(self, "Avertissement", "Veuillez sélectionner une réunion")
            return

        # Validate URL domain and race type
        validation_error = self.validate_scraping_url(url)
        if validation_error:
            QMessageBox.warning(self, "URL Invalide", validation_error)
            return

        # Use auto-detected race type instead of user selection
        race_type = self.detected_race_type
        source_key = "zone_turf"
        db_name = f'{source_key}_{race_type}.db'
        table_name = f'{race_type}_races'
        
        # Extract numeric MEETING_ID from URL
        meeting_id = self.extract_meeting_id(url)
        if meeting_id:
            existing_count = self.check_meeting_exists_in_db(meeting_id, db_name, table_name)
            if existing_count > 0:
                # Meeting already exists - ask user if they want to replace
                meeting_label = self.url_combo.currentText()
                if not self.prompt_replace_meeting(meeting_label, existing_count):
                    self.status_label.setText("Téléchargement annulé - réunion déjà présente")
                    return

        # Prepare and start scraper thread
        try:
            source = "zone-turf"  # Default source
            
            # Store current meeting info for duplicate checking
            self.current_scraping_url = url
            self.current_meeting_label = self.url_combo.currentText()
            
            self.scrape_btn.setEnabled(False)
            self.cancel_btn.setEnabled(True)
            self.refresh_meetings_btn.setEnabled(False)
            self.url_combo.setEnabled(False)
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)  # Indeterminate progress
            self.status_label.setText(f"Scraping des courses en cours...")

            self.scraper_thread = ScraperThread(url, race_type, source)
            self.scraper_thread.finished.connect(self.on_scraping_finished)
            self.scraper_thread.error.connect(self.on_scraping_error)
            self.scraper_thread.progress.connect(self.on_progress_update)
            self.scraper_thread.start()
        except Exception as e:
            QMessageBox.critical(self, "Erreur de Scraping", f"Impossible de demarrer le scraper : {e}")
            try:
                self.scrape_btn.setEnabled(True)
                self.cancel_btn.setEnabled(False)
                self.refresh_meetings_btn.setEnabled(True)
                self.url_combo.setEnabled(True)
                self.progress_bar.setVisible(False)
            except Exception:
                pass
    
    def cancel_scraping(self):
        """Cancel the ongoing scraping operation."""
        if hasattr(self, 'scraper_thread') and self.scraper_thread.isRunning():
            self.scraper_thread.cancel()
            self.status_label.setText("Annulation en cours...")
            self.status_icon.setStyleSheet("background-color: #f39c12; border-radius: 7px;")  # Orange for cancelling
            # Note: UI will be fully reset in on_scraping_error callback

    def load_meeting_urls(self, date_qdate=None):
        """Load meeting URLs from zone-turf.fr in a separate thread."""
        print(f"[DEBUG] load_meeting_urls called with date_qdate={date_qdate}")
        
        try:
            # Stop any previous thread that might be running
            if hasattr(self, 'meeting_loader_thread') and self.meeting_loader_thread.isRunning():
                print("[DEBUG] Stopping previous meeting loader thread")
                self.meeting_loader_thread.cancel()
                self.meeting_loader_thread.wait(timeout=2000)  # Wait max 2 seconds for thread to finish
        except Exception as e:
            print(f"[DEBUG] Error stopping previous meeting loader thread: {e}")
        
        try:
            self.refresh_meetings_btn.setEnabled(False)
            self.refresh_meetings_btn.setText("Chargement...")
            self.url_combo.clear()
            print("[DEBUG] Cleared combobox and disabled refresh button")
            
            # Use provided date or current date from date picker
            if date_qdate is None:
                if hasattr(self, 'date_picker') and self.date_picker:
                    date_qdate = self.date_picker.date()
                    print(f"[DEBUG] Got date from picker: {date_qdate}")
                else:
                    date_qdate = None
            
            # Validate date is a proper GUI date object
            if not isinstance(date_qdate, QDate):
                print(f"[ERROR] Invalid date type: {type(date_qdate)}, using current date")
                date_qdate = QDate.currentDate()
            
            if not date_qdate.isValid():
                print(f"[ERROR] Invalid date object, using current date")
                date_qdate = QDate.currentDate()
            
            print(f"[DEBUG] Using date: {date_qdate.toString()}")
            
            # Validate the date is not too far in the past or future
            today = QDate.currentDate()
            try:
                days_diff = today.daysTo(date_qdate)
                if abs(days_diff) > 365:
                    print(f"[WARNING] Date {date_qdate.toString()} is more than 1 year away, may not have meetings")
            except Exception as e:
                print(f"[WARNING] Could not validate date range: {e}")
            
            # Start loading in background thread
            print("[DEBUG] Creating and starting MeetingLoaderThread")
            self.meeting_loader_thread = MeetingLoaderThread(date_qdate)
            self.meeting_loader_thread.finished.connect(self.on_meetings_loaded)
            self.meeting_loader_thread.error.connect(self.on_meetings_loading_error)
            print("[DEBUG] Signals connected, starting thread")
            self.meeting_loader_thread.start()
            print("[DEBUG] Thread started")
        except Exception as e:
            print(f"[ERROR] Failed to load meeting URLs: {e}")
            import traceback
            traceback.print_exc()
            self.status_label.setText(f"Erreur lors du chargement des réunions")
            self.refresh_meetings_btn.setEnabled(True)
            self.refresh_meetings_btn.setText("🔄 Rafraîchir")
    
    def on_meetings_loaded(self, meetings):
        """Populate the combobox with loaded meetings."""
        print(f"[DEBUG] on_meetings_loaded called with {len(meetings) if meetings else 0} meetings")
        print(f"[DEBUG] hasattr url_combo: {hasattr(self, 'url_combo')}")
        if hasattr(self, 'url_combo'):
            print(f"[DEBUG] url_combo is not None: {self.url_combo is not None}")
            try:
                print(f"[DEBUG] url_combo.isVisible: {self.url_combo.isVisible()}")
            except Exception as widget_check:
                print(f"[DEBUG] Exception checking widget visibility: {widget_check}")
        
        try:
            # Verify UI widgets still exist (app might be shutting down)
            # Double-check because widget references can become stale in multi-threaded scenarios
            if not hasattr(self, 'url_combo'):
                print("[ERROR] URL combobox attribute no longer exists, skipping update")
                return
            
            if self.url_combo is None:
                print("[ERROR] URL combobox is None, skipping update")
                return
            
            # Try a safe operation to verify widget is alive
            try:
                widget_count = self.url_combo.count()
                print(f"[DEBUG] Widget is alive, current item count: {widget_count}")
            except RuntimeError as widget_error:
                print(f"[ERROR] Widget is no longer valid (RuntimeError): {widget_error}")
                return
            except Exception as widget_error:
                print(f"[ERROR] Widget check failed: {widget_error}")
                return
            
            print(f"[DEBUG] Proceeding to populate combobox with {len(meetings)} meetings")
            
            if meetings:
                # Sort meetings by time
                sorted_meetings = sorted(meetings.items())
                print(f"[DEBUG] Adding {len(sorted_meetings)} sorted meetings to combobox")
                
                for label, url in sorted_meetings:
                    try:
                        print(f"[DEBUG] Adding meeting: {label} -> {url}")
                        self.url_combo.addItem(label, url)
                    except RuntimeError as runtime_error:
                        print(f"[ERROR] RuntimeError adding meeting item: {runtime_error}")
                        print("[ERROR] Widget was destroyed during update, aborting")
                        return
                    except Exception as item_error:
                        print(f"[ERROR] Failed to add meeting item: {item_error}")
                        import traceback
                        traceback.print_exc()
                        raise
                
                print(f"[DEBUG] Combobox now has {self.url_combo.count()} items")
                
                # Select the first active meeting (if any starts with R)
                for i in range(self.url_combo.count()):
                    item_text = self.url_combo.itemText(i)
                    print(f"[DEBUG] Checking item {i}: {item_text}")
                    if item_text.startswith('R'):
                        self.url_combo.setCurrentIndex(i)
                        print(f"[DEBUG] Selected meeting at index {i}")
                        break
                
                # Update status label
                try:
                    if hasattr(self, 'status_label') and self.status_label:
                        self.status_label.setText(f"Chargé : {len(meetings)} réunions disponibles")
                except Exception as label_error:
                    print(f"[DEBUG] Could not update status label: {label_error}")
                
                print(f"[INFO] Successfully loaded {len(meetings)} meetings")
            else:
                print("[DEBUG] No meetings in dict, adding 'not found' message")
                self.url_combo.addItem("Aucune réunion trouvée", None)
                try:
                    if hasattr(self, 'status_label') and self.status_label:
                        self.status_label.setText("Aucune réunion trouvée pour cette date")
                except Exception as label_error:
                    print(f"[DEBUG] Could not update status label: {label_error}")
                print("[INFO] No meetings found for the selected date")
        
        except Exception as e:
            print(f"[ERROR] Exception in on_meetings_loaded: {e}")
            import traceback
            traceback.print_exc()
            if hasattr(self, 'url_combo') and self.url_combo:
                try:
                    self.url_combo.addItem(f"Erreur : {e}", None)
                except Exception as combo_error:
                    print(f"[ERROR] Could not add error message to combobox: {combo_error}")
            if hasattr(self, 'status_label') and self.status_label:
                try:
                    self.status_label.setText(f"Erreur lors du chargement")
                except Exception as label_error:
                    print(f"[ERROR] Could not update status on error: {label_error}")
        
        finally:
            try:
                if hasattr(self, 'refresh_meetings_btn'):
                    self.refresh_meetings_btn.setEnabled(True)
                    self.refresh_meetings_btn.setText("🔄 Rafraîchir")
            except Exception:
                pass
    
    def on_meetings_loading_error(self, error_msg):
        """Handle error when loading meetings."""
        self.url_combo.clear()
        self.url_combo.addItem(f"Erreur : {error_msg}", None)
        self.status_label.setText(f"Erreur de chargement : {error_msg}")
        self.refresh_meetings_btn.setEnabled(True)
        self.refresh_meetings_btn.setText("🔄 Rafraîchir")
    
    def on_date_changed(self):
        """Handle date selection change - reload meetings for the selected date.
        Debounced to prevent rapid fire from calendar widget.
        """
        # Only reload if the date picker is actually shown and not just being initialized
        if not hasattr(self, 'date_picker') or not self.date_picker.isVisible():
            return
        
        # Prevent too-rapid reloads (user might be typing fast)
        if not hasattr(self, '_last_date_change'):
            self._last_date_change = QDateTime.currentDateTime()
        
        current_time = QDateTime.currentDateTime()
        elapsed_ms = self._last_date_change.msecsTo(current_time)
        
        # Only reload if at least 500ms has passed since last change
        if elapsed_ms < 500:
            print(f"[DEBUG] Date change debounced ({elapsed_ms}ms since last change)")
            return
        
        self._last_date_change = current_time
        self.load_meeting_urls()
    
    def extract_meeting_identifier(self, url):
        """Extract a unique identifier for the meeting from the URL.
        Example: https://zone-turf.fr/programmes/r1-vincennes-194587.html -> 'r1-vincennes-194587'
        """
        try:
            # Extract the part after 'programmes/'
            match = re.search(r'/programmes/([^/]+?)(?:\.html)?/?$', url)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def extract_meeting_id(self, url):
        """Extract only the numeric MEETING_ID from the URL.
        Example: https://zone-turf.fr/programmes/r1-vincennes-194587.html -> 194587
        """
        try:
            # Extract the numeric part at the end (before .html)
            match = re.search(r'-(\d+?)(?:\.html)?/?$', url)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def check_meeting_exists_in_db(self, meeting_id, db_name, table_name):
        """
        Check if a meeting already exists in the database.
        Returns the count of existing entries for this meeting.
        """
        try:
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                conn.close()
                return 0
            
            # Check if URL or meeting ID exists (via REF_COURSE pattern matching)
            # REF_COURSE typically contains the meeting identifier
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE REF_COURSE LIKE ?", (f"%{meeting_id}%",))
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            print(f"Error checking meeting existence: {e}")
            return 0
    
    
    def prompt_replace_meeting(self, meeting_label, existing_count):
        """
        Prompt user if they want to replace existing meeting data.
        Sets flag and returns True if user wants to replace, False otherwise.
        """
        reply = QMessageBox.question(
            self,
            "Réunion Déjà Téléchargée",
            f"La réunion '{meeting_label}' contient déjà {existing_count} course(s) dans la base de données.\n\n"
            f"Voulez-vous remplacer les données existantes par les nouvelles données téléchargées ?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.should_replace_meeting = True
            return True
        return False

    def validate_scraping_url(self, url):
        """
        Validate that the URL is from zone-turf.fr and contains only flat or obstacle races.
        
        Returns:
            Error message (str) if validation fails, None if valid
        """
        from urllib.parse import urlparse
        import requests
        from bs4 import BeautifulSoup
        
        try:
            # Check domain
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            if 'zone-turf.fr' not in domain:
                return f"Seules les URLs de zone-turf.fr sont supportees.\nVous avez entre : {domain}"
            # Fetch page and check race types
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, timeout=10, headers=headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find table with class "hidden-smallDevice" under div with id "tab-0"
                tab_0_div = soup.find('div', {'id': 'tab-0'})
                if tab_0_div:
                    # Look for table with class "hidden-smallDevice"
                    table = tab_0_div.find('table', {'class': 'hidden-smallDevice'})
                    if table:
                        # Check for trotting race indicators (Attelé or Monté)
                        table_text = table.get_text(strip=True)
                        
                        # Allow both flat and trotting races
                        # Trotting races will be auto-detected and saved to appropriate DB

                
            except requests.RequestException as re:
                return f"Impossible de recuperer la page : {str(re)}\nVeuillez verifier l'URL et reessayer."
            
            # All validations passed
            return None
            
        except Exception as e:
            print(f"Erreur de validation d'URL: {e}")
            return f"Erreur de validation d'URL: {str(e)}"

    def normalize_track_name(self, track_str):
        """Normalize track names (HIPPODROME uppercase to title case for matching)."""
        if track_str is None:
            return ""
        return str(track_str).strip().title()

    def load_plat_history(self):
        """Load historical `plat` table from zone_turf_flat.db into `self.plat_history_df`.
        This is used for track-level historical comparisons on the Dashboard.
        Normalize HIPPODROME names to title case for consistency with downloaded data.
        """
        try:
            if hasattr(self, 'plat_history_df') and self.plat_history_df is not None:
                return
            db_path = 'zone_turf_flat.db'
            try:
                conn = sqlite3.connect(db_path)
                df = pd.read_sql_query('SELECT * FROM plat', conn)
                conn.close()
                # Normalize HIPPODROME column to match downloaded data track names
                if 'HIPPODROME' in df.columns:
                    df['HIPPODROME'] = df['HIPPODROME'].apply(self.normalize_track_name)
                self.plat_history_df = df
                # Populate dashboard HIPPODROME combo with unique tracks
                self.populate_dashboard_hippodrome_combo()
            except Exception as e:
                print(f"Error loading plat history from {db_path}: {e}")
                self.plat_history_df = pd.DataFrame()
        except Exception as e:
            print(f"Une erreur inattendue s'est produite dans load_plat_history: {e}")
            self.plat_history_df = pd.DataFrame()

    def populate_dashboard_hippodrome_combo(self):
        """Populate the dashboard HIPPODROME combobox with unique tracks from plat_history_df, filtered by race type."""
        try:
            if not hasattr(self, 'dashboard_hippodrome_combo') or self.plat_history_df is None or self.plat_history_df.empty:
                return
            if 'HIPPODROME' not in self.plat_history_df.columns:
                return
            
            # Filter by selected race type
            data = self.plat_history_df
            if 'DSCP' in data.columns and hasattr(self, 'dashboard_racetype_combo'):
                selected_racetype = self.dashboard_racetype_combo.currentText()
                if selected_racetype == 'Flat':
                    data = data[data['DSCP'] == 1]
                elif selected_racetype == 'Obstacles':
                    data = data[data['DSCP'].isin([2, 3, 4])]
            
            # Get unique normalized tracks
            hippododromes = sorted(data['HIPPODROME'].dropna().unique())
            self.dashboard_hippodrome_combo.blockSignals(True)
            self.dashboard_hippodrome_combo.clear()
            self.dashboard_hippodrome_combo.addItem("All")
            for h in hippododromes:
                self.dashboard_hippodrome_combo.addItem(str(h))
            self.dashboard_hippodrome_combo.blockSignals(False)
        except Exception as e:
            print(f"Error populating dashboard hippodrome combo: {e}")

    def safe_canvas_draw(self, canvas_name):
        """Safely draw a canvas, handling deleted C++ objects gracefully.
        Returns True if draw succeeded, False otherwise.
        """
        try:
            if not hasattr(self, canvas_name):
                return False
            canvas = getattr(self, canvas_name)
            if canvas is None or not hasattr(canvas, 'figure') or canvas.figure is None:
                return False
            canvas.draw()
            return True
        except RuntimeError as e:
            if "wrapped C/C++ object" in str(e):
                # Canvas was deleted, which is OK - just skip
                return False
            raise
        except Exception:
            return False
    
    def parse_numeric_series(self, ser):
        """Convert a pandas Series to numeric where possible (handles commas).
        Returns a float Series with NaN for unparseable entries.
        """
        try:
            if ser is None or ser.empty:
                return pd.Series(dtype=float)
            s = ser.astype(str).str.replace(',', '.').str.extract(r'([0-9]+\.?[0-9]*)')[0]
            return pd.to_numeric(s, errors='coerce')
        except Exception:
            return pd.to_numeric(ser, errors='coerce')

    def update_dashboard(self, filtered_df):
        """Compute aggregate stats and redraw dashboard plots for `filtered_df`.
        If filtered_df is empty, show zeros/empty state.
        """
        try:
            # Ensure history is loaded (best-effort)
            try:
                self.load_plat_history()
            except Exception:
                pass

            # If no filtered_df provided, fall back to historical plat data
            use_historical = False
            data_df = None
            if filtered_df is None or filtered_df.empty:
                # try to use plat_history_df as the default data source
                try:
                    if hasattr(self, 'plat_history_df') and self.plat_history_df is not None and not self.plat_history_df.empty:
                        data_df = self.plat_history_df.copy()
                        
                        # Apply race-type filter first
                        if 'DSCP' in data_df.columns and hasattr(self, 'dashboard_racetype_combo'):
                            selected_racetype = self.dashboard_racetype_combo.currentText()
                            if selected_racetype == 'Flat':
                                data_df = data_df[data_df['DSCP'] == 1]
                            elif selected_racetype == 'Obstacles':
                                data_df = data_df[data_df['DSCP'].isin([2, 3, 4])]
                        
                        # Apply HIPPODROME filter if one is selected
                        if hasattr(self, 'dashboard_hippodrome_combo'):
                            selected_hippodrome = self.dashboard_hippodrome_combo.currentText().strip()
                            if selected_hippodrome and selected_hippodrome != "All" and 'HIPPODROME' in data_df.columns:
                                data_df = data_df[data_df['HIPPODROME'] == selected_hippodrome]
                        use_historical = True
                    else:
                        # clear labels and plots if no data available
                        try:
                            self.dashboard_total_label.setText("Rows: 0")
                            self.dashboard_races_label.setText("Races: 0")
                            self.dashboard_tracks_label.setText("Tracks: 0")
                            self.dashboard_avg_odds_label.setText("Avg Odds: e?")
                            self.dashboard_median_comp_label.setText("Median Comp: e?")
                        except Exception:
                            pass
                        # clear canvases
                        try:
                            if hasattr(self, 'dashboard_comp_canvas') and hasattr(self.dashboard_comp_canvas, 'figure'):
                                ax = self.dashboard_comp_canvas.figure.subplots()
                                ax.clear()
                                self.dashboard_comp_canvas.draw()
                        except Exception:
                            pass
                except Exception:
                    return
            else:
                data_df = filtered_df

            total = len(data_df)
            races = data_df[['RACE_DATE', 'REF_COURSE']].drop_duplicates().shape[0] if {'RACE_DATE','REF_COURSE'}.issubset(data_df.columns) else data_df['REF_COURSE'].nunique() if 'REF_COURSE' in data_df.columns else 1
            tracks = 0
            for col in ('HIPPODROME', 'track_name', 'race_track', 'track', 'Track', 'Lieu', 'location'):
                if col in data_df.columns:
                    tracks = data_df[col].dropna().astype(str).str.strip().nunique()
                    break

            # Average odds
            avg_odds = 'e?'
            try:
                # prefer uppercase COTE, fallback to Cote or odds
                if 'COTE' in data_df.columns:
                    odds = self.parse_numeric_series(data_df['COTE'])
                    if not odds.dropna().empty:
                        avg_odds = f"{odds.mean():.2f}"
                elif 'Cote' in data_df.columns:
                    odds = self.parse_numeric_series(data_df['Cote'])
                    if not odds.dropna().empty:
                        avg_odds = f"{odds.mean():.2f}"
                elif 'odds' in data_df.columns:
                    odds = self.parse_numeric_series(data_df['odds'])
                    if not odds.dropna().empty:
                        avg_odds = f"{odds.mean():.2f}"
            except Exception:
                avg_odds = 'e?'

            # Median composite
            median_comp = 'e?'
            try:
                comp_ser = None
                if 'Composite' in data_df.columns:
                    comp_ser = pd.to_numeric(data_df['Composite'], errors='coerce')
                elif hasattr(self, 'current_composite_df') and self.current_composite_df is not None and not self.current_composite_df.empty:
                    comp_ser = pd.to_numeric(self.current_composite_df['Composite'], errors='coerce')
                if comp_ser is not None and not comp_ser.dropna().empty:
                    median_comp = f"{comp_ser.median():.2f}"
            except Exception:
                median_comp = 'e?'

            # Update labels
            try:
                self.dashboard_total_label.setText(f"Rows: {total}")
                self.dashboard_races_label.setText(f"Races: {races}")
                self.dashboard_tracks_label.setText(f"Tracks: {tracks}")
                self.dashboard_avg_odds_label.setText(f"Avg Odds: {avg_odds}")
                self.dashboard_median_comp_label.setText(f"Median Comp: {median_comp}")
            except Exception:
                pass

            # Draw Composite histogram
            try:
                if (hasattr(self, 'dashboard_comp_canvas') and self.dashboard_comp_canvas is not None and 
                    hasattr(self.dashboard_comp_canvas, 'figure') and self.dashboard_comp_canvas.figure is not None):
                    try:
                        fig = self.dashboard_comp_canvas.figure
                        fig.clear()
                        ax = fig.add_subplot(111)
                        if comp_ser is not None and not comp_ser.dropna().empty:
                            ax.hist(comp_ser.dropna(), bins=20, color='#2b8cbe', alpha=0.8)
                            ax.set_title('Composite Distribution')
                            ax.set_xlabel('Composite')
                        else:
                            ax.text(0.5, 0.5, 'No Composite data', ha='center')
                        self.dashboard_comp_canvas.draw()
                    except RuntimeError as re:
                        # Canvas was deleted, skip
                        if "wrapped C/C++ object" in str(re):
                            pass
                        else:
                            raise
            except Exception as e:
                print(f"Error drawing composite histogram: {e}")

            # Draw Odds histogram
            try:
                if (hasattr(self, 'dashboard_odds_canvas') and self.dashboard_odds_canvas is not None and 
                    hasattr(self.dashboard_odds_canvas, 'figure') and self.dashboard_odds_canvas.figure is not None):
                    try:
                        fig2 = self.dashboard_odds_canvas.figure
                        fig2.clear()
                        ax2 = fig2.add_subplot(111)
                        # prefer uppercase COTE
                        if 'COTE' in data_df.columns:
                            odds = self.parse_numeric_series(data_df['COTE']).dropna()
                        elif 'Cote' in data_df.columns:
                            odds = self.parse_numeric_series(data_df['Cote']).dropna()
                        elif 'odds' in data_df.columns:
                            odds = self.parse_numeric_series(data_df['odds']).dropna()
                        else:
                            odds = pd.Series(dtype=float)
                        if not odds.empty:
                            ax2.hist(odds, bins=20, color='#e6550d', alpha=0.8)
                            ax2.set_title('Odds Distribution')
                            ax2.set_xlabel('Odds')
                        else:
                            ax2.text(0.5, 0.5, 'No Odds data', ha='center')
                        self.dashboard_odds_canvas.draw()
                    except RuntimeError as re:
                        # Canvas was deleted, skip
                        if "wrapped C/C++ object" in str(re):
                            pass
                        else:
                            raise
            except Exception as e:
                print(f"Error drawing odds histogram: {e}")
            except Exception as e:
                print(f"Error drawing odds histogram: {e}")

            # Draw CORDE (starting post) performance: avg RANG per CORDE and counts
            try:
                if hasattr(self, 'dashboard_corde_canvas') and hasattr(self.dashboard_corde_canvas, 'figure'):
                    fig3 = self.dashboard_corde_canvas.figure
                    fig3.clear()
                    ax3 = fig3.add_subplot(111)
                    # prepare data: need numeric RANG and CORDE
                    if 'RANG' in data_df.columns:
                        rang = pd.to_numeric(data_df['RANG'], errors='coerce')
                    else:
                        rang = None
                    # prefer 'CORDE' or 'CORDE' variants
                    corde_col = None
                    for c in ('CORDE', 'Corde', 'corde'):
                        if c in data_df.columns:
                            corde_col = c
                            break
                    if corde_col and rang is not None:
                        try:
                            # For obstacles races, CORDE is typically 0 (not applicable); skip them in CORDE analysis
                            df_for_analysis = data_df.copy()
                            if corde_col in df_for_analysis.columns:
                                df_for_analysis = df_for_analysis[pd.to_numeric(df_for_analysis[corde_col], errors='coerce') > 0]
                            
                            # apply distance segmentation if requested
                            # parse numeric distance from available columns
                            dist_col = None
                            for dcol in ('DISTANCE','DIST','Distance','distance'):
                                if dcol in data_df.columns:
                                    dist_col = dcol
                                    break
                            selected_bin = None
                            try:
                                if hasattr(self, 'dashboard_distance_combo'):
                                    selected_bin = self.dashboard_distance_combo.currentText()
                            except Exception:
                                selected_bin = 'All'

                            df_for_corde = df_for_analysis
                            if dist_col and selected_bin and selected_bin != 'All':
                                # extract numeric distances (meters)
                                try:
                                    dvals = pd.to_numeric(df_for_corde[dist_col].astype(str).str.extract(r'(\d+)')[0], errors='coerce')
                                    df_for_corde = df_for_corde.assign(_dist_m=dvals)
                                    if selected_bin.startswith('Short'):
                                        df_for_corde = df_for_corde[df_for_corde['_dist_m'] < 1600]
                                    elif selected_bin.startswith('Medium'):
                                        df_for_corde = df_for_corde[(df_for_corde['_dist_m'] >= 1600) & (df_for_corde['_dist_m'] <= 2400)]
                                    elif selected_bin.startswith('Long'):
                                        df_for_corde = df_for_corde[df_for_corde['_dist_m'] > 2400]
                                except Exception:
                                    pass

                            corde_vals = df_for_corde[corde_col].astype(str).str.extract(r'(\d+)')[0]
                            corde_vals = pd.to_numeric(corde_vals, errors='coerce')
                            # align RANG to the same filtered df_for_corde index if distance segmentation applied
                            if 'df_for_corde' in locals():
                                rang_aligned = pd.to_numeric(df_for_corde['RANG'], errors='coerce')
                                df_corde = pd.DataFrame({'CORDE': corde_vals, 'RANG': rang_aligned})
                            else:
                                df_corde = pd.DataFrame({'CORDE': corde_vals, 'RANG': rang})
                            df_corde = df_corde.dropna(subset=['CORDE','RANG'])
                            if not df_corde.empty:
                                # Optionally segment by distance bins if available
                                dist_col = None
                                for dcol in ('DISTANCE','DIST','Distance','distance'):
                                    if dcol in data_df.columns:
                                        dist_col = dcol
                                        break
                                # compute overall place-rate (RANG<=3) and counts per CORDE
                                try:
                                    df_corde['place'] = df_corde['RANG'].apply(lambda r: 1 if float(r) <= 3 else 0)
                                except Exception:
                                    df_corde['place'] = (df_corde['RANG'] <= 3).astype(int)

                                summary = df_corde.groupby('CORDE').agg(count=('RANG','count'), places=('place','sum'))
                                summary['place_rate'] = summary['places'] / summary['count']
                                summary = summary.sort_index()
                                x = summary.index.tolist()
                                y = summary['place_rate'].tolist()
                                ax3.bar(x, y, color='#6baed6', alpha=0.9)
                                ax3.set_xlabel('Starting Post (CORDE)')
                                ax3.set_ylabel('Place Rate (proportion finishing top-3)')
                                ax3.set_ylim(0, 1)
                                ax3.set_title('Place Rate by Starting Post (overall)')
                                # annotate counts and places
                                for xi, yi, cnt, places in zip(x, y, summary['count'].tolist(), summary['places'].tolist()):
                                    ax3.text(xi, yi + 0.02, f"n={int(cnt)}\n p={int(places)}", ha='center', va='bottom', fontsize=8)
                                try:
                                    self.dashboard_corde_canvas.draw()
                                except RuntimeError as re:
                                    if "wrapped C/C++ object" not in str(re):
                                        raise
                                # produce textual insights: best posts by place_rate with minimum counts
                                try:
                                    min_count = 20
                                    eligible = summary[summary['count'] >= min_count]
                                    if not eligible.empty:
                                        best = eligible.sort_values('place_rate', ascending=False).head(3)
                                        best_posts = ", ".join([f"{int(i)} (rate {v:.2f}, n={int(n)})" for i,v,n in zip(best.index, best['place_rate'], best['count'])])
                                        insights = [f"Best posts for place bets (n>={min_count}): {best_posts}"]
                                    else:
                                        insights = [f"Not enough data per post (min n={min_count}) to recommend best posts for place bets."]
                                except Exception:
                                    insights = ["Could not compute best posts summary."]
                            else:
                                ax3.text(0.5,0.5,'No CORDE/RANG data', ha='center')
                                try:
                                    self.dashboard_corde_canvas.draw()
                                except RuntimeError as re:
                                    if "wrapped C/C++ object" not in str(re):
                                        raise
                                insights = ["No CORDE data available."]
                        except Exception:
                            ax3.text(0.5,0.5,'Error computing CORDE stats', ha='center')
                            try:
                                self.dashboard_corde_canvas.draw()
                            except RuntimeError as re:
                                if "wrapped C/C++ object" not in str(re):
                                    raise
                            insights = ["Error computing CORDE stats."]
                    else:
                        ax3.text(0.5,0.5,'CORDE or RANG missing', ha='center')
                        try:
                            self.dashboard_corde_canvas.draw()
                        except RuntimeError as re:
                            if "wrapped C/C++ object" not in str(re):
                                raise
                        insights = ["CORDE or RANG column missing in data."]
                else:
                    insights = ["Corde plot unavailable."]
            except Exception as e:
                print(f"Error drawing CORDE plot: {e}")
                insights = ["Error drawing CORDE plot."]

            # Draw sex distribution pie chart (place horses only: RANG <= 3)
            try:
                if hasattr(self, 'dashboard_sex_canvas') and hasattr(self.dashboard_sex_canvas, 'figure'):
                    fig_sex = self.dashboard_sex_canvas.figure
                    fig_sex.clear()
                    ax_sex = fig_sex.add_subplot(111)
                    
                    # Filter for place horses (RANG <= 3)
                    # support both SEX / SEXE columns
                    if 'RANG' in data_df.columns and ('SEXE' in data_df.columns or 'SEX' in data_df.columns or 'Sex' in data_df.columns):
                        try:
                            rang_numeric = pd.to_numeric(data_df['RANG'], errors='coerce')
                            place_horses = data_df[rang_numeric <= 3].copy()
                            
                            if not place_horses.empty:
                                # Count by sex and decode numeric codes if needed
                                # pick sex column variant
                                sex_col = 'SEXE' if 'SEXE' in place_horses.columns else ('SEX' if 'SEX' in place_horses.columns else ('Sex' if 'Sex' in place_horses.columns else None))
                                sex_counts = place_horses[sex_col].value_counts() if sex_col else pd.Series(dtype=int)
                                sex_decode = {'0': 'H (Stallion)', '1': 'M (Castrated)', '2': 'F (Female)'}
                                # Try to decode if values are numeric strings, otherwise use as-is
                                sex_labels = [sex_decode.get(str(s), str(s)) for s in sex_counts.index]
                                
                                if not sex_counts.empty:
                                    # Define colors for different sexes
                                    color_map_by_label = {
                                        'H (Stallion)': '#99ff99', 
                                        'M (Castrated)': '#66b3ff', 
                                        'F (Female)': '#ff9999',
                                        'H': '#99ff99',
                                        'M': '#66b3ff',
                                        'F': '#ff9999'
                                    }
                                    colors = [color_map_by_label.get(label, '#cccccc') for label in sex_labels]
                                    
                                    # Create pie chart
                                    wedges, texts, autotexts = ax_sex.pie(
                                        sex_counts.values, 
                                        labels=sex_labels,
                                        autopct='%1.1f%%',
                                        colors=colors,
                                        startangle=90
                                    )
                                    
                                    # Format text
                                    for text in texts:
                                        text.set_fontsize(10)
                                    for autotext in autotexts:
                                        autotext.set_color('white')
                                        autotext.set_fontsize(9)
                                        autotext.set_weight('bold')
                                    
                                    ax_sex.set_title(f'Place Horses by Sex (n={len(place_horses)})', fontsize=11)
                                    try:
                                        self.dashboard_sex_canvas.draw()
                                    except RuntimeError as re:
                                        if "wrapped C/C++ object" not in str(re):
                                            raise
                                else:
                                    ax_sex.text(0.5, 0.5, 'No sex data', ha='center', va='center')
                                    try:
                                        self.dashboard_sex_canvas.draw()
                                    except RuntimeError as re:
                                        if "wrapped C/C++ object" not in str(re):
                                            raise
                            else:
                                ax_sex.text(0.5, 0.5, 'No place horses', ha='center', va='center')
                                try:
                                    self.dashboard_sex_canvas.draw()
                                except RuntimeError as re:
                                    if "wrapped C/C++ object" not in str(re):
                                        raise
                        except Exception as e:
                            print(f"Error processing sex distribution: {e}")
                            ax_sex.text(0.5, 0.5, 'Error processing sex data', ha='center', va='center')
                            try:
                                self.dashboard_sex_canvas.draw()
                            except RuntimeError as re:
                                if "wrapped C/C++ object" not in str(re):
                                    raise
                    else:
                        ax_sex.text(0.5, 0.5, 'SEXE column missing', ha='center', va='center')
                        self.dashboard_sex_canvas.draw()
            except Exception as e:
                print(f"Error drawing sex distribution pie chart: {e}")

            # Draw POIDS vs finishing position visualization (boxplot + mean per finishing position)
            try:
                if hasattr(self, 'dashboard_poids_canvas') and hasattr(self.dashboard_poids_canvas, 'figure'):
                    fig_poids = self.dashboard_poids_canvas.figure
                    fig_poids.clear()
                    ax_p = fig_poids.add_subplot(121)
                    ax_mean = fig_poids.add_subplot(122)

                    # determine POIDS column name
                    poids_col = None
                    for pc in ('POIDS','Poids','PoidsKg','Weight'):
                        if pc in data_df.columns:
                            poids_col = pc
                            break

                    if poids_col and 'RANG' in data_df.columns:
                        try:
                            # normalize numeric poids
                            df_poids = data_df.copy()
                            df_poids['_poids_num'] = pd.to_numeric(df_poids[poids_col].astype(str).str.extract(r'(\d+\.?\d*)')[0], errors='coerce')
                            rang_vals = pd.to_numeric(df_poids['RANG'], errors='coerce')

                            place_mask = rang_vals <= 3
                            place_poids = df_poids.loc[place_mask, '_poids_num'].dropna()
                            nonplace_poids = df_poids.loc[~place_mask, '_poids_num'].dropna()

                            if not place_poids.empty or not nonplace_poids.empty:
                                data_for_box = [place_poids.values, nonplace_poids.values]
                                ax_p.boxplot(data_for_box, labels=['Place (<=3)','Other'], patch_artist=True,
                                             boxprops=dict(facecolor='#ffd27f', color='#8c6d31'),
                                             medianprops=dict(color='black'))
                                ax_p.set_title('POIDS: Place vs Other')
                                ax_p.set_ylabel('POIDS (kg)')
                            else:
                                ax_p.text(0.5,0.5,'No POIDS data', ha='center')

                            # mean POIDS per finishing position (1,2,3,4+)
                            df_mean = df_poids.copy()
                            df_mean['RANG_CAT'] = df_mean['RANG'].apply(lambda r: int(r) if pd.notna(r) and float(r) <= 3 else (4 if pd.notna(r) else None))
                            df_mean = df_mean.dropna(subset=['RANG_CAT','_poids_num'])
                            if not df_mean.empty:
                                mean_series = df_mean.groupby('RANG_CAT')['_poids_num'].mean().reindex([1,2,3,4]).dropna()
                                ax_mean.bar(mean_series.index.astype(str), mean_series.values, color='#9ecae1')
                                ax_mean.set_title('Mean POIDS by Finish (1,2,3,4+)')
                                ax_mean.set_xlabel('Finish Position')
                                ax_mean.set_ylabel('Mean POIDS (kg)')
                            else:
                                ax_mean.text(0.5,0.5,'No POIDS data for means', ha='center')

                            try:
                                self.dashboard_poids_canvas.draw()
                            except RuntimeError as re:
                                if "wrapped C/C++ object" not in str(re):
                                    raise
                        except Exception as e:
                            print(f"Error processing POIDS visualization: {e}")
                            fig_poids.clear()
                            ax_err = fig_poids.add_subplot(111)
                            ax_err.text(0.5,0.5,'Error processing POIDS', ha='center')
                            self.dashboard_poids_canvas.draw()
                    else:
                        fig_poids.clear()
                        ax_missing = fig_poids.add_subplot(111)
                        ax_missing.text(0.5,0.5,'POIDS or RANG missing', ha='center')
                        self.dashboard_poids_canvas.draw()
            except Exception as e:
                print(f"Error drawing POIDS visualization: {e}")

            # Draw N_WEIGHT (weight difference) visualization: violin + place-rate by weight category
            try:
                if hasattr(self, 'dashboard_nweight_canvas') and hasattr(self.dashboard_nweight_canvas, 'figure'):
                    fig_nweight = self.dashboard_nweight_canvas.figure
                    fig_nweight.clear()
                    ax_violin = fig_nweight.add_subplot(121)
                    ax_category = fig_nweight.add_subplot(122)

                    # Determine N_WEIGHT column name
                    nweight_col = None
                    for nc in ('N_WEIGHT','Nweight','n_weight','WeightChange'):
                        if nc in data_df.columns:
                            nweight_col = nc
                            break

                    if nweight_col and 'RANG' in data_df.columns:
                        try:
                            df_nw = data_df.copy()
                            df_nw['_nweight_num'] = pd.to_numeric(df_nw[nweight_col].astype(str).str.replace(',','.'), errors='coerce')
                            rang_vals = pd.to_numeric(df_nw['RANG'], errors='coerce')

                            place_mask = rang_vals <= 3
                            place_nweight = df_nw.loc[place_mask, '_nweight_num'].dropna()
                            nonplace_nweight = df_nw.loc[~place_mask, '_nweight_num'].dropna()

                            # Left panel: Violin/density plot
                            if not place_nweight.empty or not nonplace_nweight.empty:
                                data_for_violin = [place_nweight.values, nonplace_nweight.values]
                                parts = ax_violin.violinplot(data_for_violin, positions=[1,2], showmeans=True, showmedians=True)
                                ax_violin.set_xticks([1, 2])
                                ax_violin.set_xticklabels(['Place (e?3)', 'Other'])
                                ax_violin.set_ylabel('N_WEIGHT (kg)')
                                ax_violin.set_title('Weight Change Distribution')
                                ax_violin.axhline(0, color='red', linestyle='--', alpha=0.3)
                            else:
                                ax_violin.text(0.5, 0.5, 'No N_WEIGHT data', ha='center')

                            # Right panel: Place rate by weight-change category
                            df_cat = df_nw.dropna(subset=['_nweight_num', 'RANG'])
                            if not df_cat.empty:
                                # Define weight-change categories
                                def categorize_weight_change(w):
                                    if pd.isna(w):
                                        return None
                                    if w < -5:
                                        return 'Lost >5kg'
                                    elif w < 0:
                                        return 'Lost 0-5kg'
                                    elif w <= 5:
                                        return 'Stable'
                                    elif w <= 15:
                                        return 'Gained 5-15kg'
                                    else:
                                        return 'Gained >15kg'

                                df_cat['weight_cat'] = df_cat['_nweight_num'].apply(categorize_weight_change)
                                df_cat['place'] = (pd.to_numeric(df_cat['RANG'], errors='coerce') <= 3).astype(int)
                                
                                # Compute place rate by category
                                cat_summary = df_cat.groupby('weight_cat', observed=True).agg(
                                    count=('RANG', 'count'),
                                    places=('place', 'sum')
                                )
                                cat_summary['place_rate'] = cat_summary['places'] / cat_summary['count']
                                
                                # Order categories logically
                                cat_order = ['Lost >5kg', 'Lost 0-5kg', 'Stable', 'Gained 5-15kg', 'Gained >15kg']
                                cat_summary = cat_summary.reindex([c for c in cat_order if c in cat_summary.index])
                                
                                if not cat_summary.empty:
                                    x_pos = range(len(cat_summary))
                                    colors_by_rate = ['#d7191c' if r < 0.25 else '#fdae61' if r < 0.33 else '#abdda4' if r < 0.40 else '#2b83ba' 
                                                     for r in cat_summary['place_rate']]
                                    ax_category.bar(x_pos, cat_summary['place_rate'].values, color=colors_by_rate, alpha=0.8)
                                    ax_category.set_xticks(x_pos)
                                    ax_category.set_xticklabels(cat_summary.index, rotation=45, ha='right', fontsize=9)
                                    ax_category.set_ylabel('Place Rate')
                                    ax_category.set_ylim(0, max(0.5, cat_summary['place_rate'].max() * 1.2))
                                    ax_category.set_title('Place Rate by Weight Change')
                                    ax_category.axhline(0.33, color='gray', linestyle=':', alpha=0.5, label='Base rate (33%)')
                                    
                                    # Annotate bars with counts
                                    for i, (rate, count) in enumerate(zip(cat_summary['place_rate'], cat_summary['count'])):
                                        ax_category.text(i, rate + 0.01, f'n={int(count)}', ha='center', va='bottom', fontsize=8)
                                else:
                                    ax_category.text(0.5, 0.5, 'No category data', ha='center')
                            else:
                                ax_category.text(0.5, 0.5, 'No N_WEIGHT data', ha='center')

                            try:
                                self.dashboard_nweight_canvas.draw()
                            except RuntimeError as re:
                                if "wrapped C/C++ object" not in str(re):
                                    raise
                        except Exception as e:
                            print(f"Error processing N_WEIGHT visualization: {e}")
                            fig_nweight.clear()
                            ax_err = fig_nweight.add_subplot(111)
                            ax_err.text(0.5, 0.5, 'Error processing N_WEIGHT', ha='center')
                            self.dashboard_nweight_canvas.draw()
                    else:
                        fig_nweight.clear()
                        ax_missing = fig_nweight.add_subplot(111)
                        ax_missing.text(0.5, 0.5, 'N_WEIGHT or RANG missing', ha='center')
                        try:
                            self.dashboard_nweight_canvas.draw()
                        except RuntimeError as re:
                            if "wrapped C/C++ object" not in str(re):
                                raise
            except Exception as e:
                print(f"Error drawing N_WEIGHT visualization: {e}")

            # Compute top correlations with RANG (Spearman) and show as textual insights
            try:
                corr_insights = []
                if 'RANG' in data_df.columns:
                    # numeric candidates
                    cand = ['IC','S_COEFF','RAW_MEAN','N_WEIGHT','COTE','DIST','DISTANCE','NUM_STARTERS','AGE']
                    numeric_cols = [c for c in cand if c in data_df.columns]
                    corr_list = []
                    for c in numeric_cols:
                        try:
                            s1 = pd.to_numeric(data_df[c].astype(str).str.replace(',','.'), errors='coerce')
                            s2 = pd.to_numeric(data_df['RANG'], errors='coerce')
                            if s1.dropna().shape[0] > 10 and s2.dropna().shape[0] > 10:
                                corr = s1.corr(s2, method='spearman')
                                if pd.notna(corr):
                                    corr_list.append((c, corr))
                        except Exception:
                            continue
                    if corr_list:
                        # sort by absolute correlation
                        corr_list = sorted(corr_list, key=lambda x: abs(x[1]), reverse=True)
                        topn = corr_list[:5]
                        corr_lines = [f"{col}: {'{:+.2f}'.format(val)}" for col,val in topn]
                        corr_insights.append("Top correlations (Spearman) with RANG: " + "; ".join(corr_lines))
                    else:
                        corr_insights.append("No sufficient numeric data to compute correlations.")
                else:
                    corr_insights.append("RANG missing - cannot compute correlations.")
            except Exception as e:
                corr_insights.append("Error computing correlations.")

            # Combine insights and set label
            try:
                all_insights = (insights if 'insights' in locals() else []) + corr_insights
                self.dashboard_insights_label.setText("\n".join(all_insights))
            except Exception:
                pass

        except Exception as e:
            print(f"Unexpected error in update_dashboard: {e}")
        
        # removed accidental scraper startup code from here (belongs in start_scraping)

    def on_progress_update(self, percent, message):
        # update progress bar and status
        try:
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(int(percent))
            if message:
                self.status_label.setText(message)
                # set in-progress indicator (orange)
                try:
                    self.status_icon.setStyleSheet("background-color: #f39c12; border-radius: 7px;")
                except Exception:
                    pass
                # also reflect in window title briefly
                self.setWindowTitle(f"RaceX - {message}")
        except Exception:
            pass
    
    def clean_trotting_data(self, df):
        """Apply trotting-specific data cleaning to DataFrame.
        Cleans GAINS, DIST., REF_COURSE, and REC. columns using specialized functions."""
        try:
            from model_functions import clean_gains_trot, time_to_seconds, clean_ref_course, clean_distance
            
            df = df.copy()
            
            # Clean GAINS column (convert "20 711€" to 20711)
            if 'GAINS' in df.columns:
                df['GAINS'] = df['GAINS'].apply(clean_gains_trot)
            
            # Clean DIST. column (convert "2 950m" to 2950)
            if 'DIST.' in df.columns:
                df['DIST.'] = df['DIST.'].apply(clean_distance)
            elif 'DISTANCE' in df.columns:
                df['DISTANCE'] = df['DISTANCE'].apply(clean_distance)
            
            # Clean REF_COURSE column (convert "R1 Course N°1" to "R1C1")
            if 'REF_COURSE' in df.columns:
                df['REF_COURSE'] = df['REF_COURSE'].apply(clean_ref_course)
            
            # Clean REC. column (convert "1'11\"0" to seconds)
            if 'REC.' in df.columns:
                df['REC.'] = df['REC.'].apply(time_to_seconds)
            
            print(f"[INFO] Applied trotting data cleaning to {len(df)} rows")
            return df
        except Exception as e:
            print(f"[WARNING] Trotting data cleaning failed: {e}")
            return df

    def compute_trotting_metrics(self, df):
        """Compute trotting-specific metrics (FA/FM, S_COEFF) from 'DERNIÈRES PERF.'
        These metrics are used by the trotting analysis tabs.
        (Data cleaning is applied at DB load time via clean_trotting_data)
        """
        try:
            from model_functions import compute_d_perf, success_coefficient, parse_performance_string
            print("[DEBUG] compute_trotting_metrics called")
            df = df.copy()
            perf_col = 'DERNIÈRES PERF.' if 'DERNIÈRES PERF.' in df.columns else None
            if not perf_col:
                return df

            # Determine discipline
            discipline = 'a'
            if 'RACE_TYPE' in df.columns and str(df['RACE_TYPE'].iloc[0]).lower() == 'monté':
                discipline = 'm'
            elif 'RACE_CONDITIONS' in df.columns and 'monté' in str(df['RACE_CONDITIONS'].iloc[0]).lower():
                discipline = 'm'

            # Compute FA/FM
            if 'FA' not in df.columns and 'FM' not in df.columns:
                df['FA'] = df[perf_col].apply(lambda x: compute_d_perf(x).get('a') if pd.notna(x) else None)
                df['FM'] = df[perf_col].apply(lambda x: compute_d_perf(x).get('m') if pd.notna(x) else None)

            # Compute S_COEFF
            if 'S_COEFF' not in df.columns:
                df['S_COEFF'] = df[perf_col].apply(lambda x: success_coefficient(x, discipline) if pd.notna(x) else 0.0)

            # Normalize S_COEFF
            if 'S_COEFF' in df.columns and 'S_COEFF_norm' not in df.columns:
                s_min = df['S_COEFF'].min()
                s_max = df['S_COEFF'].max()
                if s_max > s_min:
                    df['S_COEFF_norm'] = ((df['S_COEFF'] - s_min) / (s_max - s_min) * 100).round(2)
                else:
                    df['S_COEFF_norm'] = 50.0

            # Extract performance metrics
            perf_metrics_list = []
            for idx, row in df.iterrows():
                perf_str = row.get(perf_col)
                if pd.notna(perf_str):
                    perf_metrics = parse_performance_string(str(perf_str))
                else:
                    perf_metrics = pd.Series({
                        'num_races': 0, 'avg_rank': None, 'best_rank': None, 'worst_rank': None,
                        'time_decay_avg_rank': None, 'disq_count': 0, 'disq_harness_rate': 0,
                        'disq_mounted_rate': 0, 'recent_avg_rank': None, 'recent_disq_count': 0,
                        'recent_disq_rate': 0, 'mounted_ratio': None, 'trend_rank_slope': None,
                        'trend_label': 'Unknown'
                    })
                perf_metrics_list.append(perf_metrics)

            perf_df = pd.DataFrame(perf_metrics_list)
            for col in perf_df.columns:
                df[col] = perf_df[col].values

            # Parse shoeing features
            try:
                if 'DEF.' in df.columns:
                    from model_functions import parse_shoeing_features
                    shoe_feats = df['DEF.'].apply(lambda x: parse_shoeing_features(x))
                    shoe_df = pd.DataFrame(list(shoe_feats))
                    for col in shoe_df.columns:
                        if col not in df.columns:
                            df[col] = shoe_df[col].values
            except Exception:
                pass

            # Compute DQ_Risk composite score from disqualification metrics
            try:
                df['DQ_Risk'] = self.compute_dq_risk_score(df)
            except Exception:
                df['DQ_Risk'] = 0.0

            return df
        except Exception as e:
            print(f"[WARNING] Trotting metric computation failed: {e}")
            import traceback
            traceback.print_exc()
            return df

    def compute_dq_risk_score(self, df):
        """Compute a composite disqualification risk score (0-100) from available disq metrics.
        Used for inclusion in composite score and Disq Risk tab.
        Higher value = more risky disqualifications.
        """
        try:
            risk_series = pd.Series(0.0, index=df.index)
            
            # Normalize individual metrics and combine
            metrics_data = pd.DataFrame(index=df.index)
            
            # disq_count: higher = worse
            if 'disq_count' in df.columns:
                vals = pd.to_numeric(df['disq_count'], errors='coerce').fillna(0)
                if vals.max() > 0:
                    metrics_data['disq_count'] = vals / vals.max()
                else:
                    metrics_data['disq_count'] = 0.0
            
            # disq_harness_rate: higher = worse
            if 'disq_harness_rate' in df.columns:
                vals = pd.to_numeric(df['disq_harness_rate'], errors='coerce').fillna(0)
                metrics_data['disq_harness_rate'] = vals  # Already 0-1
            
            # disq_mounted_rate: higher = worse
            if 'disq_mounted_rate' in df.columns:
                vals = pd.to_numeric(df['disq_mounted_rate'], errors='coerce').fillna(0)
                metrics_data['disq_mounted_rate'] = vals  # Already 0-1
            
            # recent_disq_count: higher = worse (recent disqs are more concerning)
            if 'recent_disq_count' in df.columns:
                vals = pd.to_numeric(df['recent_disq_count'], errors='coerce').fillna(0)
                if vals.max() > 0:
                    metrics_data['recent_disq_count'] = vals / vals.max()
                else:
                    metrics_data['recent_disq_count'] = 0.0
            
            # recent_disq_rate: higher = worse
            if 'recent_disq_rate' in df.columns:
                vals = pd.to_numeric(df['recent_disq_rate'], errors='coerce').fillna(0)
                metrics_data['recent_disq_rate'] = vals  # Already 0-1
            
            # Weights for combining: count (0.3), harness rate (0.2), mounted rate (0.2), recent count (0.2), recent rate (0.1)
            weights = {
                'disq_count': 0.3,
                'disq_harness_rate': 0.2,
                'disq_mounted_rate': 0.2,
                'recent_disq_count': 0.2,
                'recent_disq_rate': 0.1
            }
            
            total_weight = sum(weights.get(c, 0) for c in metrics_data.columns)
            if total_weight > 0:
                for col in metrics_data.columns:
                    risk_series += metrics_data[col] * weights.get(col, 0)
                risk_series = risk_series / total_weight
            
            # Scale to 0-100
            dq_risk = (risk_series * 100).round(2)
            return dq_risk
        except Exception as e:
            print(f"[DEBUG] compute_dq_risk_score failed: {e}")
            return pd.Series(0.0, index=df.index)
        except Exception as e:
            print(f"[WARNING] Trotting metric computation failed: {e}")
            import traceback
            traceback.print_exc()
            return df

    def on_scraping_finished(self, df, race_type, source, custom_table):
        self.scrape_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.refresh_meetings_btn.setEnabled(True)
        self.url_combo.setEnabled(True)
        self.progress_bar.setVisible(False)
        
        if df.empty:
            self.status_label.setText("Aucune donnee trouvee")
            QMessageBox.information(self, "Info", "Aucune donnee de course trouvee")
            return
        
        # Apply trotting-specific cleaning if race type is trot
        if race_type.lower() == 'trot':
            print("[INFO] Applying trotting race data cleaning...")
            df = self.clean_trotting_data(df)
        
        # Display in current table
        self.populate_table(self.current_table, df)
        
        # Determine database and table names
        source_key = source.lower().replace('-', '_')
        
        if custom_table:
            # Use custom table name with source prefix
            db_name = f'{source_key}_races.db'
            table_name = custom_table
        else:
            # Use default naming: {source}_{race_type}_races
            db_name = f'{source_key}_{race_type}.db'
            table_name = f'{race_type}_races'
        
        # If user previously confirmed replacement, delete old meeting entries
        if hasattr(self, 'should_replace_meeting') and self.should_replace_meeting:
            meeting_id = self.extract_meeting_id(self.current_scraping_url)
            if meeting_id:
                try:
                    conn = sqlite3.connect(db_name)
                    cursor = conn.cursor()
                    cursor.execute(f"DELETE FROM {table_name} WHERE REF_COURSE LIKE ?", (f"%{meeting_id}%",))
                    conn.commit()
                    conn.close()
                    self.status_label.setText("Anciennes données supprimées, ajout des nouvelles...")
                except Exception as e:
                    print(f"Error deleting old meeting data: {e}")
            self.should_replace_meeting = False  # Reset flag
        
        try:
            # Sanitize DataFrame columns: strip whitespace and remove duplicate column names
            df = df.copy()
            df.columns = [str(c).strip() for c in df.columns]
            if df.columns.duplicated().any():
                dup_cols = list(df.columns[df.columns.duplicated(keep=False)])
                print(f"[WARN] Dropping duplicate DataFrame columns before saving: {dup_cols}")
                df = df.loc[:, ~df.columns.duplicated()]

            conn = sqlite3.connect(db_name)
            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                table_exists = cursor.fetchone() is not None

                # If the table exists and we have REF_COURSE & race_date, only append rows that are not already present
                if table_exists and 'REF_COURSE' in df.columns and 'race_date' in df.columns:
                    try:
                        existing = pd.read_sql_query(f"SELECT REF_COURSE, race_date FROM {table_name}", conn)
                        if not existing.empty:
                            existing_pairs = set(tuple(x) for x in existing[['REF_COURSE', 'race_date']].values.tolist())
                        else:
                            existing_pairs = set()
                    except Exception:
                        existing_pairs = set()

                    # Filter incoming df to only new pairs
                    def is_new(row):
                        try:
                            return (row['REF_COURSE'], row['race_date']) not in existing_pairs
                        except Exception:
                            return True

                    df_to_append = df[df.apply(is_new, axis=1)]
                else:
                    df_to_append = df

                if df_to_append.empty:
                    # nothing to append
                    conn.close()
                    self.status_label.setText(f"No new entries to append to {db_name} ({table_name})")
                else:
                    try:
                        df_to_append.to_sql(table_name, conn, if_exists='append', index=False)
                    except Exception:
                        # as a last resort, replace the table if append consistently fails
                        df_to_append.to_sql(table_name, conn, if_exists='replace', index=False)
                    conn.close()
            except Exception as e:
                conn.close()
                QMessageBox.critical(self, "Erreur de Base de Donnees", f"Impossible d'enregistrer les donnees : {e}")
                return
            conn.close()
            
            table_info = f" (custom table: {table_name})" if custom_table else f" ({table_name} table)"
            self.status_label.setText(f"Recupere {len(df)} entrees et enregistre dans {db_name}{table_info}")
            try:
                self.status_icon.setStyleSheet("background-color: #27ae60; border-radius: 7px;")
            except Exception:
                pass

            # Refresh database view
            self.load_database_data()
            # Reset UI for new data
            self.reset_app_window()

        except Exception as e:
            QMessageBox.critical(self, "Erreur de Base de Donnees", f"Impossible d'enregistrer les donnees : {e}")
    
    def update_single_race(self):
        """Update odds for a single filtered race with the latest data from zone-turf.
        
        Efficiently updates only the odds columns (PMU, PMU_FR, Unnamed: 13) and Cote
        by matching ID_COURSE against HTML structure. Does not re-scrape entire race.
        """
        try:
            # Check if a single race is filtered
            if not hasattr(self, 'last_filtered_df') or self.last_filtered_df is None or self.last_filtered_df.empty:
                QMessageBox.warning(self, "Avertissement", "Veuillez d'abord filtrer une course unique")
                return
            
            filtered_df = self.last_filtered_df
            
            # Get ID_COURSE and RACE_URL from filtered data
            if 'ID_COURSE' not in filtered_df.columns or 'RACE_URL' not in filtered_df.columns:
                QMessageBox.warning(self, "Avertissement", "Donnees ID_COURSE ou RACE_URL manquantes")
                return
            
            id_course = filtered_df['ID_COURSE'].iloc[0]
            race_url = filtered_df['RACE_URL'].iloc[0]
            race_date = filtered_df['RACE_DATE'].iloc[0] if 'RACE_DATE' in filtered_df.columns else None
            race_ref = filtered_df['REF_COURSE'].iloc[0] if 'REF_COURSE' in filtered_df.columns else None
            
            if not id_course or not race_url or not race_date or not race_ref:
                QMessageBox.warning(self, "Avertissement", "Donnees de course incompletes")
                return
            
            # Extract meeting URL
            if '#' not in race_url:
                QMessageBox.warning(self, "Erreur", "Format RACE_URL invalide")
                return
            
            meeting_url, race_id = race_url.split('#', 1)
            
            # Confirm update with user
            reply = QMessageBox.question(
                self, "Confirmation",
                f"Mettre e jour les cotes de {race_ref} du {race_date}?\n"
                f"ID Course: {id_course}\n"
                f"Seules les cotes (PMU, PMU_FR) seront mises e jour",
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply == QMessageBox.No:
                return
            
            # Determine race type
            race_type = "flat"
            if hasattr(self, 'race_type_combo'):
                race_type = "trot" if self.race_type_combo.currentText() == "Trot" else "flat"
            
            # Show progress
            self.status_label.setText(f"Mise e jour des cotes de {race_ref}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)
            
            # Scrape and extract odds columns
            import requests
            from bs4 import BeautifulSoup
            import pandas as pd
            
            try:
                response = requests.get(meeting_url, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find the race block by matching ID_COURSE with <a name="...">
                course_blocks = soup.find_all('div', class_='bloc data course')
                race_table = None
                found_id_course = None
                
                for block in course_blocks:
                    name_anchor = block.find('a', attrs={'name': True})
                    if name_anchor:
                        block_id_course = name_anchor.get('name')
                        if str(block_id_course) == str(id_course):
                            # Found matching block! Extract the table
                            found_id_course = block_id_course
                            course_table_elem = block.find('table', id=lambda x: x and x.startswith('course'))
                            if course_table_elem:
                                race_table = course_table_elem
                                print(f"[INFO] Found race table for ID_COURSE {id_course}")
                                break
                
                if not race_table:
                    QMessageBox.warning(self, "Erreur", f"Impossible de trouver la course avec ID_COURSE {id_course}")
                    self.progress_bar.setVisible(False)
                    self.status_label.setText("Mise e jour echouee")
                    return
                
                # Extract non-partant horses (rows with class "non-partant") before parsing
                non_partant_horses = []
                tbody = race_table.find('tbody')
                if tbody:
                    non_partant_rows = tbody.find_all('tr', class_=lambda x: x and 'non-partant' in x)
                    for row in non_partant_rows:
                        tds = row.find_all('td')
                        if tds:
                            try:
                                horse_num_text = tds[0].get_text(strip=True)
                                horse_num = int(horse_num_text)
                                non_partant_horses.append(horse_num)
                                print(f"[DEBUG] Found non-partant horse: {horse_num}")
                            except (ValueError, IndexError):
                                pass
                
                if non_partant_horses:
                    print(f"[INFO] Non-partant horses to remove: {non_partant_horses}")
                
                # Parse table to DataFrame
                try:
                    df_parsed = pd.read_html(str(race_table))[0]
                except Exception as e:
                    print(f"[ERROR] Failed to parse race table: {e}")
                    QMessageBox.warning(self, "Erreur", "Impossible de parser la table de course")
                    self.progress_bar.setVisible(False)
                    self.status_label.setText("Mise e jour echouee")
                    return
                
                if df_parsed.empty:
                    QMessageBox.warning(self, "Erreur", "La table de course est vide")
                    self.progress_bar.setVisible(False)
                    self.status_label.setText("Mise e jour echouee")
                    return
                
                # Extract only the columns we need to update
                # Horse number is in column 'N?' (or index 0)
                # Odds columns: 'Unnamed: 11' (PMU), 'Unnamed: 12' (PMU_FR), 'Unnamed: 13' (never renamed)
                odds_cols_needed = ['N?', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13']
                available_cols = [col for col in odds_cols_needed if col in df_parsed.columns]
                
                if len(available_cols) < 2:  # At minimum need horse number and some odds
                    print(f"[ERROR] Missing odds columns. Available: {df_parsed.columns.tolist()}")
                    QMessageBox.warning(self, "Erreur", "Colonnes de cotes manquantes dans la table")
                    self.progress_bar.setVisible(False)
                    self.status_label.setText("Mise e jour echouee")
                    return
                
                odds_df = df_parsed[available_cols].copy()
                
                # Update database: for each horse, update its odds and recompute COTE
                db_name = f'zone_turf_{race_type}.db'
                table_name = f'{race_type}_races'
                
                conn = sqlite3.connect(db_name)
                cursor = conn.cursor()
                
                # Delete non-partant horses from database
                deleted_count = 0
                for horse_num in non_partant_horses:
                    try:
                        cursor.execute(
                            f"DELETE FROM {table_name} WHERE REF_COURSE = ? AND RACE_DATE = ? AND \"N?\" = ?",
                            (race_ref, race_date, horse_num)
                        )
                        if cursor.rowcount > 0:
                            deleted_count += cursor.rowcount
                            print(f"[DEBUG] Deleted non-partant horse {horse_num} from database")
                    except Exception as e:
                        print(f"[WARN] Failed to delete non-partant horse {horse_num}: {e}")
                
                if deleted_count > 0:
                    conn.commit()
                    print(f"[INFO] Deleted {deleted_count} non-partant horses")
                
                # Helper function to extract COTE from odds columns (same logic as flat_zone.py)
                def compute_cote_from_odds(row):
                    """Extract COTE using heuristic from flat_zone.py logic"""
                    import re
                    
                    def parse_odds_string(s):
                        if pd.isna(s):
                            return None
                        raw = str(s).strip()
                        raw = re.sub(r'<[^>]+>', '', raw)
                        raw = raw.replace('\xa0', ' ').replace(',', '.')
                        # Try fraction like '2/1' -> decimal odds = 1 + 2/1
                        frac = re.search(r'(\d+)\s*/\s*(\d+)', raw)
                        if frac:
                            try:
                                return float(1.0 + int(frac.group(1)) / int(frac.group(2)))
                            except Exception:
                                pass
                        # Find decimal/integer tokens
                        nums = re.findall(r'\d+(?:\.\d+)?', raw)
                        if not nums:
                            return None
                        try:
                            return float(nums[-1])
                        except Exception:
                            return None
                    
                    # Check columns in priority order
                    candidates = ['Unnamed: 13', 'Unnamed: 12', 'Unnamed: 11']
                    
                    for col in candidates:
                        if col in row.index:
                            raw = row.get(col)
                            if pd.notna(raw) and str(raw).strip() not in ['', '-', 'nan']:
                                parsed = parse_odds_string(raw)
                                if parsed is not None:
                                    return round(parsed, 1)
                    
                    return None
                
                updated_count = 0
                updated_horses = []
                cote_updates = []
                
                for idx, row in odds_df.iterrows():
                    try:
                        horse_num = row.get('N?')
                        if pd.isna(horse_num):
                            continue
                        
                        horse_num = int(horse_num)
                        
                        # Get the odds values
                        pmu = row.get('Unnamed: 11')
                        pmu_fr = row.get('Unnamed: 12')
                        unnamed_13 = row.get('Unnamed: 13')
                        
                        # Build UPDATE statement
                        update_cols = []
                        update_vals = []
                        
                        if 'Unnamed: 11' in odds_df.columns and pd.notna(pmu):
                            update_cols.append("PMU = ?")
                            update_vals.append(pmu)
                        
                        if 'Unnamed: 12' in odds_df.columns and pd.notna(pmu_fr):
                            update_cols.append("PMU_FR = ?")
                            update_vals.append(pmu_fr)
                        
                        # Compute COTE from odds columns
                        cote_new = compute_cote_from_odds(row)
                        if cote_new is not None:
                            update_cols.append("COTE = ?")
                            update_vals.append(cote_new)
                            cote_updates.append((horse_num, cote_new))
                            print(f"[DEBUG] Horse {horse_num}: new COTE = {cote_new} (from PMU={pmu}, PMU_FR={pmu_fr}, Unnamed:13={unnamed_13})")
                        
                        if not update_cols:
                            continue
                        
                        update_vals.extend([race_ref, race_date, horse_num])
                        
                        update_sql = f"""UPDATE {table_name} 
                                       SET {', '.join(update_cols)}
                                       WHERE REF_COURSE = ? AND RACE_DATE = ? AND \"N?\" = ?"""
                        
                        cursor.execute(update_sql, update_vals)
                        if cursor.rowcount > 0:
                            updated_horses.append(horse_num)
                            updated_count += cursor.rowcount
                            print(f"[DEBUG] Updated horse {horse_num} in database")
                        else:
                            print(f"[WARN] No rows updated for horse {horse_num} (REF_COURSE={race_ref}, RACE_DATE={race_date})")
                    
                    except Exception as e:
                        print(f"[WARN] Failed to update horse {horse_num}: {e}")
                        continue
                
                print(f"[INFO] Total updates: {updated_count} rows, {len(updated_horses)} horses")
                print(f"[INFO] Updated horses: {sorted(updated_horses)}")
                if cote_updates:
                    print(f"[INFO] COTE updates summary: {cote_updates}")
                
                conn.commit()
                conn.close()
                
                self.progress_bar.setVisible(False)
                self.status_label.setText(f"Cotes mises e jour pour {updated_count} chevaux")
                self.status_icon.setStyleSheet("background-color: #27ae60; border-radius: 7px;")
                
                QMessageBox.information(
                    self, "Succes",
                    f"Cotes de {race_ref} mises e jour!\n"
                    f"{updated_count} chevaux ont recu de nouvelles cotes\n"
                    f"Veuillez recharger le filtre pour voir les modifications"
                )
                
                # Reload data
                self.load_database_data()
                
                # Re-apply filters to refresh analysis with updated odds
                try:
                    self.apply_filters()
                    # If analysis is visible, refresh composite/heatmap with new odds
                    if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and self.analysis_widget.isVisible():
                        self.refresh_composite_and_heatmap(self.last_filtered_df)
                        
                        # Update race statistics with new odds
                        composite_df = self.current_composite_df if hasattr(self, 'current_composite_df') else pd.DataFrame()
                        self.update_race_statistics_display(self.last_filtered_df, composite_df)
                except Exception as e:
                    print(f"[WARN] Error refreshing analysis after odds update: {e}")
                
            except Exception as db_error:
                self.progress_bar.setVisible(False)
                QMessageBox.critical(self, "Erreur", f"Erreur lors de la mise e jour de la base de donnees:\n{db_error}")
                self.status_label.setText("Mise e jour echouee")
        
        except Exception as e:
            self.progress_bar.setVisible(False)
            QMessageBox.critical(self, "Erreur", f"Erreur lors de la mise e jour:\n{e}")
            print(f"[ERROR] update_single_race failed: {e}")
    
    def reset_app_window(self):
        """Reset the app window after successful data download: clear filters, reset status icon, reload tables."""
        try:
            # Reset status icon to neutral gray
            self.status_icon.setStyleSheet("background-color: #9ea7ad; border-radius: 7px;")
        except Exception:
            pass
        try:
            # Clear all filter inputs
            if hasattr(self, 'race_type_filter'):
                self.race_type_filter.setCurrentIndex(0)
            if hasattr(self, 'track_filter_input'):
                self.track_filter_input.clear()
            if hasattr(self, 'date_filter_input'):
                self.date_filter_input.clear()
            if hasattr(self, 'race_number_filter_input'):
                self.race_number_filter_input.clear()
            if hasattr(self, 'distance_filter_input'):
                self.distance_filter_input.clear()
        except Exception:
            pass
        try:
            # Clear the main table view
            if hasattr(self, 'current_table'):
                self.current_table.setRowCount(0)
                self.current_table.setColumnCount(0)
        except Exception:
            pass
        try:
            # Reset statistics label
            if hasattr(self, 'stats_label'):
                self.stats_label.setText("Aucune donnee")
        except Exception:
            pass
        try:
            # Reload database data (will repopulate tables automatically)
            self.load_database_data()
        except Exception as e:
            print(f"Erreur lors de la reinitialisation de la fenetre de l'application: {e}")
    
    def on_scraping_error(self, error_msg):
        self.scrape_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.refresh_meetings_btn.setEnabled(True)
        self.url_combo.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.status_label.setText("Erreur de scraping")
        try:
            self.status_icon.setStyleSheet("background-color: #e74c3c; border-radius: 7px;")
        except Exception:
            pass
        QMessageBox.critical(self, "Erreur de Scraping", f"Impossible de scraper les donnees : {error_msg}")
    
    def on_race_type_changed(self):
        """Handle race type filter change"""
        selected_race_type = self.race_type_filter.currentText()
        self.load_database_data(race_type=selected_race_type)
    
    def populate_table(self, table_widget, df):
        # Exclude noisy/internal columns (case-insensitive) when showing DB table
        try:
            drop_candidates = {
                'ENTRAeNEUR', 'ENTRAINEUR', 'PROPRIeTAIRE', 'PROPRIETAIRE', 'COURSE_ID', 'PMU', 'PMU_FR', 'UNNAMED: 13',
                'RACE_CONDITIONS', 'RACE_CONDITION', 'DESCRIPTIF', 'EXT_CONDITIONS',
                'JOCKEY_MUSIC', 'TRAINER_MUSIC', 'HORSE_LINK', 'HORSELINK', 'source', 'source_db'
            }
            # Build list of actual columns to drop by comparing uppercase names
            drop_cols = [c for c in df.columns if c.upper() in drop_candidates]
            display_df = df.drop(columns=drop_cols, errors='ignore')
        except Exception:
            display_df = df.copy()

        # Reset index so row indices start at 0 for QTableWidget
        display_df = display_df.reset_index(drop=True)

        table_widget.setRowCount(len(display_df))
        table_widget.setColumnCount(len(display_df.columns))
        table_widget.setHorizontalHeaderLabels(display_df.columns.tolist())

        for i, (_, row) in enumerate(display_df.iterrows()):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value) if pd.notna(value) else "")
                table_widget.setItem(i, j, item)

        table_widget.resizeColumnsToContents()
    
    def load_database_data(self, race_type=None):
        try:
            dfs = []
            # Default database configurations (Zone-Turf only)
            db_config = [
                ('zone_turf_flat.db', 'flat_races', 'Zone-Turf', 'Flat'),
                ('zone_turf_trot.db', 'trot_races', 'Zone-Turf', 'Trot'),
                # Legacy database support
                ('flat_zone.db', 'flat_races', 'Zone-Turf', 'Flat'),
                ('trot_zone.db', 'trot_races', 'Zone-Turf', 'Trot'),
            ]

            # Determine which race type to load; default to selector value
            if race_type is None:
                race_type = self.race_type_filter.currentText()

            # Normalize French UI labels to internal DB labels (keep DB names unchanged)
            rt_norm = (race_type or '').strip().lower()
            if rt_norm in ('plat', 'flat'):
                race_type_key = 'Flat'
            elif rt_norm == 'trot':
                race_type_key = 'Trot'
            else:
                race_type_key = race_type

            # Only load the selected race type (compare against DB config labels)
            db_config = [cfg for cfg in db_config if cfg[3] == race_type_key]
            
            for db_name, table_name, source_label, race_type_db in db_config:
                try:
                    conn = sqlite3.connect(db_name)
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                    if cursor.fetchone():
                        df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY race_date DESC", conn)
                        if not df.empty:
                            # Apply trotting-specific cleaning if this is trot data
                            if race_type_db == 'Trot':
                                df = self.clean_trotting_data(df)
                            
                            df['source'] = source_label
                            df['source_db'] = db_name
                            dfs.append(df)
                    conn.close()
                except:
                    pass
            
            if dfs:
                self.flat_df = pd.DataFrame()
                self.trot_df = pd.DataFrame()
                
                # Separate data by source for later operations and prioritize zone_turf_flat.db
                for df in dfs:
                    if 'source' in df.columns:
                        if 'Flat' in df['source'].iloc[0]:
                            self.flat_df = pd.concat([self.flat_df, df], ignore_index=True)
                        elif 'Trot' in df['source'].iloc[0]:
                            self.trot_df = pd.concat([self.trot_df, df], ignore_index=True)

                # Prioritize displaying zone_turf_flat.db by sorting dfs so that
                # entries from that DB come first when concatenated
                def priority_key(d):
                    try:
                        return 0 if ('source_db' in d.columns and d['source_db'].iloc[0] == 'zone_turf_flat.db') else 1
                    except Exception:
                        return 1

                dfs_sorted = sorted(dfs, key=priority_key)

                # Concatenate for display (will have NaN for mismatched columns)
                self.full_df = pd.concat(dfs_sorted, ignore_index=True)
                
                # Pre-normalize composite score columns for performance optimization
                # This caches normalization parameters and speeds up repeated composite calculations
                try:
                    self.full_df = normalize_composite_columns(self.full_df)
                except Exception as e:
                    print(f"[INFO] Normalization optimization skipped: {e}")
                
                self.populate_table(self.db_table, self.full_df)
                self.update_stats(self.full_df)
                # Populate filter comboboxes with available values
                try:
                    # Dates
                    if 'RACE_DATE' in self.full_df.columns:
                        dates = sorted(self.full_df['RACE_DATE'].dropna().astype(str).unique(), reverse=True)
                        self.date_filter.blockSignals(True)
                        self.date_filter.clear()
                        self.date_filter.addItem("")
                        for d in dates:
                            self.date_filter.addItem(d)
                        self.date_filter.blockSignals(False)

                    # Race IDs
                    if 'REF_COURSE' in self.full_df.columns:
                        ids = sorted(self.full_df['REF_COURSE'].dropna().astype(str).unique())
                        self.race_id_filter.blockSignals(True)
                        self.race_id_filter.clear()
                        self.race_id_filter.addItem("")
                        for rid in ids:
                            self.race_id_filter.addItem(rid)
                        self.race_id_filter.blockSignals(False)

                    # Tracks - prefer HIPPODROME as primary column
                    track_col = 'HIPPODROME' if 'HIPPODROME' in self.full_df.columns else None
                    if track_col is None:
                        for c in ['track_name','race_track','track','Track']:
                            if c in self.full_df.columns:
                                track_col = c
                                break
                    if track_col is not None:
                        # normalize track names from downloaded data to match plat table
                        tracks = sorted(self.full_df[track_col].dropna().astype(str).apply(self.normalize_track_name).unique())
                        self.track_filter.blockSignals(True)
                        self.track_filter.clear()
                        self.track_filter.addItem("")
                        for t in tracks:
                            self.track_filter.addItem(t)
                        self.track_filter.blockSignals(False)
                except Exception as e:
                    print(f"Failed to populate filter combos: {e}")
                race_type_label = f" ({race_type})" if race_type != "All" else ""
                self.status_label.setText(f"Loaded {len(self.full_df)} entries from database{race_type_label}")
                # Ensure plat history is loaded and dashboard shows default historical stats
                try:
                    self.load_plat_history()
                    if hasattr(self, 'update_dashboard'):
                        # pass None so update_dashboard falls back to historical plat data
                        self.update_dashboard(None)
                except Exception as e:
                    print(f"Error initializing dashboard with historical data: {e}")
            else:
                self.full_df = pd.DataFrame()
                self.flat_df = pd.DataFrame()
                self.trot_df = pd.DataFrame()
                # Clear the database table widget so old data isn't left visible
                try:
                    self.db_table.setRowCount(0)
                    self.db_table.setColumnCount(0)
                    self.db_table.setHorizontalHeaderLabels([])
                except Exception:
                    pass

                race_type_label = f" ({race_type})" if race_type != "All" else ""
                self.status_label.setText(f"Aucune donnee en base trouvee{race_type_label}")
                try:
                    self.load_plat_history()
                    if hasattr(self, 'update_dashboard'):
                        self.update_dashboard(None)
                except Exception as e:
                    print(f"Error initializing dashboard with historical data (no DB): {e}")
                
        except Exception as e:
            print(f"Database load error: {e}")
    
    def apply_filters(self):
        if not hasattr(self, 'full_df') or self.full_df.empty:
            return
        
        # Work on a copy and normalize string comparison by stripping whitespace
        filtered_df = self.full_df.copy()

        date_text = self.date_filter.currentText().strip() if isinstance(self.date_filter, QComboBox) else (self.date_filter.text().strip() if hasattr(self.date_filter, 'text') else '')

        # Update race_id options to only those available for the selected date
        try:
            if 'REF_COURSE' in self.full_df.columns and isinstance(self.race_id_filter, QComboBox):
                self.race_id_filter.blockSignals(True)
                current_selection = self.race_id_filter.currentText().strip()
                self.race_id_filter.clear()
                self.race_id_filter.addItem("")
                if date_text:
                    ids = sorted(self.full_df[self.full_df['RACE_DATE'].astype(str).str.strip() == date_text]['REF_COURSE'].dropna().astype(str).unique())
                else:
                    ids = sorted(self.full_df['REF_COURSE'].dropna().astype(str).unique())
                for rid in ids:
                    self.race_id_filter.addItem(rid)
                # restore selection if still valid
                if current_selection in ids:
                    self.race_id_filter.setCurrentText(current_selection)
                else:
                    # default to empty selection
                    self.race_id_filter.setCurrentIndex(0)
                self.race_id_filter.blockSignals(False)
        except Exception:
            pass

        race_id_text = self.race_id_filter.currentText().strip() if isinstance(self.race_id_filter, QComboBox) else (self.race_id_filter.text().strip() if hasattr(self.race_id_filter, 'text') else '')

        # Build mask progressively to ensure exact matches when multiple filters provided
        mask = pd.Series(True, index=filtered_df.index)

        if date_text and 'RACE_DATE' in filtered_df.columns:
            mask = mask & (filtered_df['RACE_DATE'].astype(str).str.strip() == date_text)

        if race_id_text and 'REF_COURSE' in filtered_df.columns:
            mask = mask & (filtered_df['REF_COURSE'].astype(str).str.strip() == race_id_text)

        track_text = self.track_filter.currentText().strip() if isinstance(self.track_filter, QComboBox) else (self.track_filter.text().strip() if hasattr(self.track_filter, 'text') else '')
        if track_text:
            # normalize the selected track for matching with normalized data
            track_text_normalized = self.normalize_track_name(track_text)
            if 'HIPPODROME' in filtered_df.columns:
                mask = mask & (filtered_df['HIPPODROME'].astype(str).apply(self.normalize_track_name) == track_text_normalized)
            elif 'track_name' in filtered_df.columns:
                mask = mask & (filtered_df['track_name'].astype(str).apply(self.normalize_track_name) == track_text_normalized)
            elif 'race_track' in filtered_df.columns:
                mask = mask & (filtered_df['race_track'].astype(str).apply(self.normalize_track_name) == track_text_normalized)

        filtered_df = filtered_df[mask]

        # Remember current filtered dataframe for actions (delete/export)
        try:
            self.last_filtered_df = filtered_df
        except Exception:
            self.last_filtered_df = None

        # Compute trotting-specific metrics if this is a trotting race
        if not filtered_df.empty:
            is_trot_race = False
            # Detect if this is a trotting race
            print(f"[DEBUG] RACE DETECTION: Checking RACE_TYPE columns...")
            if 'RACE_TYPE' in filtered_df.columns:
                race_type_val = filtered_df['RACE_TYPE'].iloc[0]
                print(f"[DEBUG] RACE_TYPE column found: value='{race_type_val}', lowered='{str(race_type_val).lower()}'")
                is_trot_race = str(race_type_val).lower() == 'trot'
            elif 'RACE_CONDITIONS' in filtered_df.columns:
                race_cond = str(filtered_df['RACE_CONDITIONS'].iloc[0]).lower()
                print(f"[DEBUG] RACE_CONDITIONS column found: '{race_cond}'")
                is_trot_race = 'trot' in race_cond
            elif 'race_type' in filtered_df.columns:
                race_type_val = str(filtered_df['race_type'].iloc[0]).lower()
                print(f"[DEBUG] race_type column found: '{race_type_val}'")
                is_trot_race = race_type_val == 'trot'
            
            print(f"[DEBUG] is_trot_race={is_trot_race}")
            
            if is_trot_race:
                print("[INFO] Trotting race detected - computing trotting metrics...")
                try:
                    filtered_df = self.compute_trotting_metrics(filtered_df)
                    print("[DEBUG] compute_trotting_metrics returned successfully")
                except Exception as e:
                    print(f"[ERROR] compute_trotting_metrics raised exception: {e}")
                    import traceback
                    traceback.print_exc()

        # Enable analysis-dependent menu items when race is filtered
        try:
            if hasattr(self, 'export_png_action'):
                self.export_png_action.setEnabled(not filtered_df.empty)
            if hasattr(self, 'export_pdf_action'):
                self.export_pdf_action.setEnabled(not filtered_df.empty)
            if hasattr(self, 'print_analysis_action'):
                self.print_analysis_action.setEnabled(not filtered_df.empty)
            if hasattr(self, 'export_combos_action'):
                self.export_combos_action.setEnabled(not filtered_df.empty)
            if hasattr(self, 'advanced_bet_gen_action'):
                self.advanced_bet_gen_action.setEnabled(not filtered_df.empty)
        except Exception:
            pass

        self.populate_table(self.db_table, filtered_df)
        self.update_stats(filtered_df)
        self.status_label.setText(f"Filtered: {len(filtered_df)} entries")
        
        # =====================================================
        # Update Dashboard and Betting Analysis for the filtered selection
        # =====================================================
        try:
            # dashboard gets overall filtered stats (multi/race)
            if hasattr(self, 'update_dashboard'):
                self.update_dashboard(filtered_df)
        except Exception as e:
            print(f"Error updating dashboard: {e}")

        # Check if a single race is filtered (both date and race_id selected)
        if date_text and race_id_text and len(filtered_df) > 0:
            # Determine race type
            is_trot_race = False
            if 'RACE_TYPE' in filtered_df.columns:
                is_trot_race = filtered_df['RACE_TYPE'].iloc[0].lower() == 'trot'
            elif 'RACE_CONDITIONS' in filtered_df.columns:
                is_trot_race = 'trot' in str(filtered_df['RACE_CONDITIONS'].iloc[0]).lower()
            elif 'race_type' in filtered_df.columns:
                is_trot_race = filtered_df['race_type'].iloc[0].lower() == 'trot'
            
            # Only open separate window for trotting races
            if is_trot_race:
                try:
                    self.open_analysis_window(filtered_df, race_id_text, date_text, track_text)
                except Exception as e:
                    print(f"[ERROR] Failed to open trotting analysis window: {e}")
        
        # Show inline analysis (flat races only; trotting will be hidden by show_race_analysis)
        self.show_race_analysis(filtered_df, race_id_text, date_text, track_text)
    
    def clear_filters(self):
        # Reset comboboxes to first (empty) entry instead of removing items
        try:
            if isinstance(self.date_filter, QComboBox) and self.date_filter.count() > 0:
                self.date_filter.setCurrentIndex(0)
            else:
                self.date_filter.clear()
        except Exception:
            pass

        try:
            if isinstance(self.race_id_filter, QComboBox) and self.race_id_filter.count() > 0:
                self.race_id_filter.setCurrentIndex(0)
            else:
                self.race_id_filter.clear()
        except Exception:
            pass

        try:
            if isinstance(self.track_filter, QComboBox) and self.track_filter.count() > 0:
                self.track_filter.setCurrentIndex(0)
            else:
                self.track_filter.clear()
        except Exception:
            pass
        if hasattr(self, 'full_df'):
            self.populate_table(self.db_table, self.full_df)
            self.update_stats(self.full_df)
            self.status_label.setText(f"{len(self.full_df)} Entrees chargees depuis la base de donnees")
        # Hide analysis when filters are cleared
        self.analysis_widget.setVisible(False)
        # Reset heatmap race key so next filter will trigger refresh
        self._last_heatmap_race_key = None
        # Clear remembered filtered df
        try:
            self.last_filtered_df = None
        except Exception:
            pass
        
        # Disable analysis-dependent menu items when filters are cleared
        try:
            if hasattr(self, 'export_png_action'):
                self.export_png_action.setEnabled(False)
            if hasattr(self, 'export_pdf_action'):
                self.export_pdf_action.setEnabled(False)
            if hasattr(self, 'print_analysis_action'):
                self.print_analysis_action.setEnabled(False)
            if hasattr(self, 'export_combos_action'):
                self.export_combos_action.setEnabled(False)
            if hasattr(self, 'advanced_bet_gen_action'):
                self.advanced_bet_gen_action.setEnabled(False)
        except Exception:
            pass

    def reset_view(self):
        """Clear filters and reload database data for the currently selected race type."""
        try:
            # Clear UI filters
            self.clear_filters()
        except Exception:
            pass
        try:
            # Reload underlying data (respect current race type selector)
            selected_race_type = self.race_type_filter.currentText() if hasattr(self, 'race_type_filter') else None
            self.load_database_data(race_type=selected_race_type)
        except Exception as e:
            print(f"Reset view error: {e}")
    
    def trigger_manual_analysis(self):
        """Manually trigger analysis for the currently filtered race."""
        try:
            # Get current filter values
            race_id_text = self.race_id_filter.currentText() if hasattr(self, 'race_id_filter') else ""
            date_text = self.date_filter.currentText() if hasattr(self, 'date_filter') else ""
            track_text = self.track_filter.currentText() if hasattr(self, 'track_filter') else ""
            
            # Get the currently displayed filtered dataframe
            if not hasattr(self, 'current_filtered_df') or self.current_filtered_df is None or self.current_filtered_df.empty:
                QMessageBox.warning(self, "Aucune donnee", "Aucune course selectionnee pour l'analyse")
                return
            
            # Trigger analysis with current filtered data
            self.show_race_analysis(self.current_filtered_df, race_id_text, date_text, track_text)
            print(f"[DEBUG] Manual analysis triggered for race: {race_id_text} on {date_text}")
        except Exception as e:
            print(f"[ERROR] Manual analysis trigger failed: {e}")
            QMessageBox.critical(self, "Erreur", f"Erreur lors de l'analyse: {e}")

    def open_analysis_window(self, filtered_df, race_id_text, date_text, track_text=None):
        """
        Open analysis window for a race. Opens a separate window based on race type.
        This method provides a consistent interface for both manual and automatic analysis opening.
        """
        try:
            if filtered_df is None or filtered_df.empty:
                print("[DEBUG] Cannot open analysis: filtered_df is empty")
                return
            
            # Determine race type from filtered data
            race_type = 'flat'
            if hasattr(self, 'race_type_filter') and self.race_type_filter:
                selected_type = self.race_type_filter.currentText().lower()
                if 'trot' in selected_type:
                    race_type = 'trot'
            
            # Open appropriate analysis window
            if race_type == 'trot':
                print(f"[INFO] Opening TrottingAnalysisWindow for: {race_id_text}")
                # Compute trotting metrics BEFORE opening the window
                filtered_df = self.compute_trotting_metrics_for_df(filtered_df)
                # Create TrottingAnalysisWindow
                self.analysis_window = TrottingAnalysisWindow(filtered_df=filtered_df, race_id_text=race_id_text, date_text=date_text, parent_app=self)
            else:
                self.analysis_window = BaseAnalysisWindow(filtered_df, race_id_text, date_text, race_type='flat', parent_app=self)
        except Exception as e:
            print(f"[ERROR] Failed to open analysis window: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Erreur", f"Erreur lors de l'ouverture de l'analyse: {e}")
    
    def compute_trotting_metrics_for_df(self, df):
        """
        Compute all trotting-specific metrics (FA, FM, S_COEFF, disqualification stats, etc.)
        for a filtered dataframe before opening analysis window.
        """
        try:
            from model_functions import compute_d_perf, success_coefficient, parse_performance_string
            df = df.copy()
            perf_col = 'DERNIÈRES PERF.' if 'DERNIÈRES PERF.' in df.columns else None
            if not perf_col:
                return df

            # Determine discipline
            discipline = 'a'
            if 'RACE_TYPE' in df.columns and df['RACE_TYPE'].iloc[0]:
                if 'monté' in str(df['RACE_TYPE'].iloc[0]).lower():
                    discipline = 'm'

            # Compute FA/FM
            if 'FA' not in df.columns and 'FM' not in df.columns:
                df['FA'] = df[perf_col].apply(lambda x: compute_d_perf(x).get('a') if pd.notna(x) else None)
                df['FM'] = df[perf_col].apply(lambda x: compute_d_perf(x).get('m') if pd.notna(x) else None)

            # Compute S_COEFF
            if 'S_COEFF' not in df.columns:
                df['S_COEFF'] = df[perf_col].apply(lambda x: success_coefficient(x, discipline) if pd.notna(x) else 0.0)

            # Normalize S_COEFF
            if 'S_COEFF' in df.columns and 'S_COEFF_norm' not in df.columns:
                s_min = df['S_COEFF'].min()
                s_max = df['S_COEFF'].max()
                if s_max > s_min:
                    df['S_COEFF_norm'] = ((df['S_COEFF'] - s_min) / (s_max - s_min) * 100).round(2)

            # Extract and merge performance metrics
            perf_metrics_list = []
            for idx, row in df.iterrows():
                perf_str = row.get(perf_col)
                if pd.notna(perf_str):
                    perf_metrics = parse_performance_string(str(perf_str))
                else:
                    perf_metrics = pd.Series({
                        'num_races': 0, 'avg_rank': None, 'best_rank': None, 'worst_rank': None,
                        'time_decay_avg_rank': None, 'disq_count': 0, 'disq_harness_rate': 0,
                        'disq_mounted_rate': 0, 'recent_avg_rank': None, 'recent_disq_count': 0,
                        'recent_disq_rate': 0, 'mounted_ratio': None, 'trend_rank_slope': None,
                        'trend_label': 'Unknown'
                    })
                perf_metrics_list.append(perf_metrics)

            perf_df = pd.DataFrame(perf_metrics_list)
            for col in perf_df.columns:
                df[col] = perf_df[col].values

            # Compute DQ_Risk composite score from disqualification metrics
            try:
                df['DQ_Risk'] = self.compute_dq_risk_score(df)
            except Exception:
                df['DQ_Risk'] = 0.0

            # Compute shoeing aggressiveness from DEF. column
            try:
                from model_functions import parse_shoeing_features
                def_col = 'DEF.' if 'DEF.' in df.columns else ('Def' if 'Def' in df.columns else None)
                if def_col:
                    shoeing_data = df[def_col].apply(lambda x: parse_shoeing_features(x))
                    df['shoeing_aggressiveness'] = shoeing_data.apply(lambda x: x.get('shoeing_aggressiveness'))
            except Exception as e:
                # If shoeing parsing fails, default to None
                df['shoeing_aggressiveness'] = None

            return df
        except Exception as e:
            print(f"[ERROR] compute_trotting_metrics_for_df failed: {e}")
            import traceback
            traceback.print_exc()
            return df
    
    def update_stats(self, df):
        if df.empty:
            self.stats_label.setText("Aucune donnee")
            return
        
        unique_races = df['REF_COURSE'].nunique() if 'REF_COURSE' in df.columns else 0
        unique_dates = df['RACE_DATE'].nunique() if 'RACE_DATE' in df.columns else 0
        self.stats_label.setText(f"Entries: {len(df)} | Races: {unique_races} | Dates: {unique_dates}")
    
    def update_race_statistics_display(self, filtered_df, composite_df=None):
        """Update the detailed race statistics tab display."""
        try:
            # Use current composite if not provided
            if composite_df is None:
                composite_df = self.current_composite_df if hasattr(self, 'current_composite_df') else pd.DataFrame()
            
            if filtered_df.empty or composite_df.empty:
                if hasattr(self, 'stats_label_detailed'):
                    self.stats_label_detailed.setText('')
                return
            
            stats = compute_race_statistics(filtered_df, composite_df)
            if stats and hasattr(self, 'stats_label_detailed'):
                stats_text = ""
                if 'avg_odds' in stats:
                    horses = stats.get('avg_odds_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Cote Moyenne: {stats['avg_odds']:.2f} ({horses_str})\n"
                if 'median_odds' in stats:
                    horses = stats.get('median_odds_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Cote Mediane: {stats['median_odds']:.2f} ({horses_str})\n"
                if 'mean_composite' in stats:
                    horses = stats.get('mean_composite_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Composite Moyen: {stats['mean_composite']:.4f} ({horses_str})\n"
                if 'median_composite' in stats:
                    horses = stats.get('median_composite_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Composite Median: {stats['median_composite']:.4f} ({horses_str})"
                
                self.stats_label_detailed.setText(stats_text)
            elif hasattr(self, 'stats_label_detailed'):
                self.stats_label_detailed.setText('')
        except Exception as e:
            print(f"Error updating race statistics display: {e}")
            if hasattr(self, 'stats_label_detailed'):
                self.stats_label_detailed.setText('')
    
    def populate_analysis_table(self, table_widget, df, title=""):
        """Populate an analysis table with data from a DataFrame."""
        if df.empty:
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)
            return
        
        # Reset index for clean display
        df = df.reset_index(drop=True)
        
        table_widget.setRowCount(len(df))
        table_widget.setColumnCount(len(df.columns))
        table_widget.setHorizontalHeaderLabels(df.columns.tolist())
        
        for i, (_, row) in enumerate(df.iterrows()):
            for j, value in enumerate(row):
                # Format N? as integer if this column is 'N?'
                if df.columns[j] == 'N?' and pd.notna(value):
                    try:
                        value = str(int(float(value)))
                    except (ValueError, TypeError):
                        value = str(value)
                else:
                    value = str(value) if pd.notna(value) else ""
                
                item = QTableWidgetItem(value)
                # Highlight winners/placers if RANG column exists
                if 'RANG' in df.columns:
                    rang_val = row.get('RANG')
                    try:
                        rang_val = int(float(rang_val)) if pd.notna(rang_val) else None
                        if rang_val and rang_val <= 3:
                            item.setBackground(QColor(144, 238, 144))  # Light green for winners/placers
                    except:
                        pass
                table_widget.setItem(i, j, item)
        
        table_widget.resizeColumnsToContents()

    def select_horse_by_name(self, name: str):
        """Select and focus the row for the given horse name in the composite table."""
        if not name:
            return
        try:
            cheval_col = None
            for i in range(self.composite_table.columnCount()):
                hdr = self.composite_table.horizontalHeaderItem(i)
                if hdr and hdr.text().lower() in ('cheval','cheval ' ,'cheval'):
                    cheval_col = i
                    break
            if cheval_col is None and self.composite_table.columnCount() > 0:
                cheval_col = 0

            # search rows
            for r in range(self.composite_table.rowCount()):
                item = self.composite_table.item(r, cheval_col)
                if item and item.text().strip().lower() == str(name).strip().lower():
                    self.composite_table.setCurrentCell(r, cheval_col)
                    self.composite_table.scrollToItem(item)
                    return
        except Exception:
            pass
    
    def show_heatmap_fullscreen(self):
        """Open the heatmap in a larger fullscreen dialog window."""
        if not hasattr(self, 'current_heatmap_data') or self.current_heatmap_data is None:
            QMessageBox.warning(self, "Avertissement", "Aucune donnee heatmap disponible")
            return
        
        try:
            norm_df, composite_scores = self.current_heatmap_data
            
            # Get REF_COURSE from current filtered data
            ref_course = ""
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                if 'REF_COURSE' in self.last_filtered_df.columns:
                    ref_course = str(self.last_filtered_df['REF_COURSE'].iloc[0])
            
            # Create dialog and store as instance variable to prevent garbage collection
            self.heatmap_dialog = QMainWindow()
            title = f"[TARGET] Heatmap for {ref_course} - Full View" if ref_course else "[TARGET] Heatmap - Full View"
            self.heatmap_dialog.setWindowTitle(title)
            self.heatmap_dialog.setGeometry(100, 100, 1400, 900)
            
            # Create interactive heatmap widget (Plotly) at higher quality/size
            try:
                widget = create_interactive_matplotlib_heatmap(norm_df, composite_scores, parent=self, external_controls=getattr(self, 'heatmap_controls_dict', None), on_metric_toggle=self.on_heatmap_metric_toggle)
            except Exception:
                widget = create_heatmap_canvas(norm_df, composite_scores)
            self.heatmap_dialog.setCentralWidget(widget)
            self.heatmap_dialog.raise_()  # Bring window to front
            self.heatmap_dialog.activateWindow()  # Give focus to window
            self.heatmap_dialog.show()
            
        except Exception as e:
            print(f"Heatmap error: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Erreur", f"Impossible d'afficher le heatmap : {e}")

    def on_heatmap_metric_toggle(self, metric_name, is_checked):
        """Callback when a metric checkbox is toggled in the heatmap.
        Triggers re-sort and re-render based on selected metrics.
        Currently just allows render_plot() in heatmap to handle the visual update.
        """
        try:
            print(f"[DEBUG] Metric '{metric_name}' toggled: {'checked' if is_checked else 'unchecked'}")
            # The heatmap's render_plot() will be called automatically after this returns
            # No additional recomputation needed - just let the visualization update
        except Exception as e:
            print(f"[ERROR] on_heatmap_metric_toggle failed: {e}")

    def refresh_composite_and_heatmap(self, filtered_df):
        """Recompute composite scores and heatmap using current UI weights (including corde slider).
        Updates `self.current_composite_df`, `self.current_heatmap_data`, and UI widgets.
        """
        print(f"[DEBUG] refresh_composite_and_heatmap called with df_len={len(filtered_df) if filtered_df is not None else 'None'}")
        try:
            if filtered_df is None or filtered_df.empty:
                print("[DEBUG] Filtered df is None or empty, returning")
                return

            # Build weights dict from runtime slider values
            weights = {k: float(v) for k, v in self.metric_weights.items()}

            composite_df = compute_composite_score(filtered_df, weights=weights)
            self.current_composite_df = composite_df

            # Populate composite table
            if composite_df is None or composite_df.empty:
                self.composite_table.setRowCount(0)
                self.composite_table.setColumnCount(0)
            else:
                self.populate_analysis_table(self.composite_table, composite_df)

                # Build normalized matrix for heatmap using metric list (include COTE but exclude AgeScore/SexScore)
                # Prefer pre-normalized values from composite_df or use direct computation from filtered_df
                metric_candidates = ['IF','IC','S_COEFF','n_weight','N_WEIGHT','VALEUR','Valeur','Corde','CORDE','COTE','Cote']
                metric_cols = []
                for c in metric_candidates:
                    if c in composite_df.columns or c in ['Corde']:
                        metric_cols.append(c)
                # Deduplicate while preserving order
                seen = set()
                metric_cols = [x for x in metric_cols if not (x in seen or seen.add(x))]

                if metric_cols:
                    # Get horse name column from filtered_df for proper alignment
                    cheval_col = 'Cheval' if 'Cheval' in filtered_df.columns else ('CHEVAL' if 'CHEVAL' in filtered_df.columns else None)
                    if not cheval_col:
                        print("[DEBUG] No horse name column found, using composite_df['Cheval']")
                        cheval_col = 'Cheval'
                    
                    # Create norm_df with filtered_df horse names as temporary index for proper alignment
                    filtered_horses = filtered_df[cheval_col].astype(str).values
                    temp_norm_df = pd.DataFrame(index=filtered_horses)
                    
                    for col in metric_cols:
                        # PRIORITY: Use pre-normalized columns from filtered_df if available
                        if f'_norm_{col}' in filtered_df.columns:
                            # Use pre-normalized values (already inverted and scaled 0-1 per race)
                            ser = filtered_df[f'_norm_{col}']
                        elif col == 'Corde':
                            ser = compute_corde_score(filtered_df)
                        elif col in ['COTE', 'Cote']:
                            # COTE handling: lower odds = better (market confidence)
                            if 'COTE' in filtered_df.columns:
                                ser = pd.to_numeric(filtered_df['COTE'], errors='coerce')
                            elif 'Cote' in filtered_df.columns:
                                ser = pd.to_numeric(filtered_df['Cote'], errors='coerce')
                            else:
                                ser = pd.Series([0]*len(filtered_df))
                            # Invert: lower odds = better
                            if not ser.dropna().empty and ser.dropna().max() != ser.dropna().min():
                                ser = -ser
                        else:
                            # Fallback: compute normalization on-the-fly
                            ser = pd.to_numeric(filtered_df[col], errors='coerce') if col in filtered_df.columns else pd.Series([0]*len(filtered_df))
                            # For metrics where lower is better, invert before normalizing; handle uppercase variants
                            if col in ['IF','n_weight','N_WEIGHT','VALEUR','Valeur']:
                                if col in ['n_weight','N_WEIGHT']:
                                    ser = ser.abs()
                                ser = -ser

                        if ser.dropna().empty or ser.max() == ser.min():
                            norm = pd.Series(0.5, index=filtered_df.index)
                        else:
                            norm = (ser - ser.min()) / (ser.max() - ser.min())
                        
                        # Reset index to match horse names for proper alignment
                        norm.index = filtered_horses
                        temp_norm_df[col] = norm
                    
                    # Reindex temp_norm_df to composite_df order
                    composite_horses = composite_df['Cheval'].astype(str).values
                    norm_df = temp_norm_df.reindex(composite_horses, fill_value=0.5)

                    # Detect horse number column - N° is the valid horse number (NOT CORDE which is something else)
                    num_col = None
                    # Prioritize 'N°' (with encoding variant 'N?'), skip CORDE as it's a different variable
                    num_candidates = ['N°', 'N?', 'Poste', 'POSTE', 'Post', 'N']
                    
                    # Debug: Print available columns that might contain horse numbers
                    print(f"[DEBUG] Checking for horse number column in {len(filtered_df.columns)} columns")
                    number_like_cols = [c for c in filtered_df.columns if 'n' in c.lower() or 'corde' in c.lower() or 'post' in c.lower()]
                    print(f"[DEBUG] Columns matching number patterns: {number_like_cols}")
                    
                    for candidate in num_candidates:
                        if candidate in filtered_df.columns:
                            num_col = candidate
                            print(f"[DEBUG] Found horse number column: '{num_col}'")
                            break
                    
                    if not num_col:
                        print(f"[DEBUG] No horse number column found. Available columns: {list(filtered_df.columns)}")
                    
                    # Build lookup: horse name -> horse number, using filtered_df
                    cheval_col = 'Cheval' if 'Cheval' in filtered_df.columns else ('CHEVAL' if 'CHEVAL' in filtered_df.columns else None)
                    name_to_number = {}
                    if cheval_col and num_col:
                        # Ensure both are strings for proper mapping
                        horse_names = filtered_df[cheval_col].astype(str).values
                        horse_numbers = filtered_df[num_col].astype(str).values
                        
                        # DEBUG: Print first few name-number pairs to verify alignment
                        print(f"[DEBUG] First 5 name-number pairs from filtered_df:")
                        for i in range(min(5, len(horse_names))):
                            print(f"  [{i}] '{horse_names[i]}' -> '{horse_numbers[i]}'")
                        
                        name_to_number = dict(zip(horse_names, horse_numbers))
                        print(f"[DEBUG] Built name_to_number mapping with {len(name_to_number)} horses from column '{num_col}'")
                    
                    # Create composite_scores Series using composite_df values, indexed by horse names
                    composite_scores = pd.Series(composite_df['Composite'].values, index=composite_horses)
                    # Sort by composite score descending (this determines heatmap display order)
                    composite_scores = composite_scores.sort_values(ascending=False)
                    
                    # Reorder norm_df to match sorted composite_scores order
                    norm_df = norm_df.loc[composite_scores.index]
                    
                    # Now update index to include horse numbers (after reordering, so alignment is maintained)
                    if name_to_number:
                        print(f"[DEBUG] Formatting heatmap index. norm_df has {len(norm_df)} horses")
                        formatted_index = []
                        for i, name in enumerate(norm_df.index):
                            number = name_to_number.get(name, '?')
                            formatted_entry = f"{name} ({number})"
                            formatted_index.append(formatted_entry)
                            if i < 5:  # Print first 5 for debugging
                                print(f"  [{i}] Looking up '{name}' -> got '{number}' -> formatted as '{formatted_entry}'")
                        
                        norm_df.index = formatted_index
                        # Also update composite_scores index to match
                        composite_scores.index = norm_df.index
                        print(f"[DEBUG] Final formatted heatmap index (first 5): {list(norm_df.index[:5])}")
                    
                    self.current_heatmap_data = (norm_df, composite_scores)

                    # Update external controls (populate sort dropdown with metric columns)
                    try:
                        if getattr(self, 'heatmap_controls_dict', None):
                            sc = self.heatmap_controls_dict.get('sort_combo')
                            if sc is not None:
                                try:
                                    sc.clear()
                                    sc.addItem('Composite')
                                    for col in norm_df.columns:
                                        sc.addItem(col)
                                except Exception:
                                    pass
                            vmin_w = self.heatmap_controls_dict.get('vmin_spin')
                            vmax_w = self.heatmap_controls_dict.get('vmax_spin')
                            if vmin_w is not None:
                                try:
                                    vmin_w.setValue(0.0)
                                except Exception:
                                    pass
                            if vmax_w is not None:
                                try:
                                    vmax_w.setValue(1.0)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    # Clean up old heatmap widget BEFORE creating new one
                    # (prevents old heatmap from appearing when switching races)
                    # Close all matplotlib figures to free memory
                    try:
                        plt.close('all')
                    except Exception:
                        pass
                    
                    # Remove all widgets from layout
                    while self.summary_canvas_layout.count() > 0:
                        item = self.summary_canvas_layout.takeAt(0)
                        if item:
                            w = item.widget()
                            if w:
                                w.setParent(None)
                                w.deleteLater()
                    
                    # Force immediate layout update
                    self.summary_canvas_layout.update()
                    
                    # Create the new heatmap widget
                    try:
                        widget = create_interactive_matplotlib_heatmap(norm_df, composite_scores, parent=self, external_controls=getattr(self, 'heatmap_controls_dict', None), on_metric_toggle=self.on_heatmap_metric_toggle)
                    except Exception as ex:
                        print(f"[ERROR] Failed to create interactive heatmap: {ex}")
                        widget = create_heatmap_canvas(norm_df, composite_scores)
                    
                    # Add new widget and refresh scroll area
                    self.summary_canvas_layout.addWidget(widget)
                    self.summary_canvas_scroll.setWidget(self.summary_canvas_container)
        except Exception as e:
            print(f"Error refreshing composite/heatmap: {e}")

    def open_settings_dialog(self):
        try:
            # Build initial weights dict from current sliders/metric_weights
            initial = self.metric_weights.copy() if hasattr(self, 'metric_weights') else {'IC':0.20, 'S_COEFF':0.20, 'IF':0.15, 'n_weight':0.10, 'COTE':0.20, 'Corde':0.10, 'VALEUR':0.05}
            dlg = SettingsDialog(self, metric_weights=initial)
            # Apply dialog styling
            self.apply_dialog_style(dlg)
            if dlg.exec_() == QDialog.Accepted:
                new_weights = dlg.get_weights()
                # Save into settings and update runtime sliders/labels
                for k, v in new_weights.items():
                    try:
                        perc = int(v*100)
                        self.settings.setValue(k, perc)
                        # update runtime metric weights
                        self.metric_weights[k] = float(v)
                    except Exception:
                        pass

                # Refresh analysis and regenerate bets with new weights
                if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and self.analysis_widget.isVisible():
                    self.refresh_composite_and_heatmap(self.last_filtered_df)
                    
                    # Update race statistics with new weights
                    composite_df = self.current_composite_df if hasattr(self, 'current_composite_df') else pd.DataFrame()
                    self.update_race_statistics_display(self.last_filtered_df, composite_df)
                    
                    # Regenerate betting combinations with new weights
                    try:
                        self.generate_bets()
                    except Exception as e:
                        print(f"[WARN] Error regenerating bets after weight change: {e}")
        except Exception as e:
            print(f"Error opening settings dialog: {e}")
    
    def toggle_betting_analysis(self, checked):
        """
        Toggle visibility of betting analysis section on/off.
        Saves preference to settings and immediately updates UI if applicable.
        """
        # Ensure checked is a boolean
        if not isinstance(checked, bool):
            checked = bool(checked)
        
        self.show_analysis_enabled = checked
        # Save as string "True"/"False" to avoid QSettings type issues
        self.settings.setValue('show_analysis', 'True' if checked else 'False')
        self.settings.sync()  # Ensure it's written immediately
        print(f"[DEBUG] toggle_betting_analysis: checked={checked}, show_analysis_enabled={self.show_analysis_enabled}, settings saved")
        
        # If unchecked, hide analysis widget and scroll area immediately
        if not checked:
            if hasattr(self, 'analysis_widget'):
                self.analysis_widget.setVisible(False)
            if hasattr(self, 'analysis_scroll'):
                self.analysis_scroll.setVisible(False)
        # If checked and a single race is filtered, show analysis widget
        else:
            # Re-trigger analysis display with stored filter values
            if (hasattr(self, 'current_filtered_df') and self.current_filtered_df is not None and
                not self.current_filtered_df.empty and
                self.current_race_id_filter and self.current_date_filter):
                self.show_race_analysis(
                    self.current_filtered_df,
                    self.current_race_id_filter,
                    self.current_date_filter,
                    self.current_track_filter
                )
    
    def show_race_analysis(self, filtered_df, race_id_text, date_text, track_text=None):
        """
        Show betting analysis if exactly one race is filtered (race_id + date).
        Only displays if both race_id and date filters are provided, and if analysis is enabled.
        
        IMPORTANT: Trotting races are displayed ONLY in a separate TrottingAnalysisWindow.
        This method skips trotting races entirely to avoid mixing analysis types.
        """
        print(f"[DEBUG] show_race_analysis called: race_id={race_id_text}, date={date_text}, df_len={len(filtered_df)}, show_analysis_enabled={self.show_analysis_enabled}")
        
        # Store current filter values early so manual analysis can use them
        if hasattr(self, 'current_filtered_df'):
            self.current_filtered_df = filtered_df.copy() if not filtered_df.empty else None
            self.current_race_id_filter = race_id_text or ""
            self.current_date_filter = date_text or ""
            self.current_track_filter = track_text or ""

        # Check if this is a trotting race - if so, skip inline display (use separate window only)
        if not filtered_df.empty:
            is_trot_race = False
            if 'RACE_TYPE' in filtered_df.columns:
                is_trot_race = filtered_df['RACE_TYPE'].iloc[0].lower() == 'trot'
            elif 'RACE_CONDITIONS' in filtered_df.columns:
                is_trot_race = 'trot' in str(filtered_df['RACE_CONDITIONS'].iloc[0]).lower()
            elif 'race_type' in filtered_df.columns:
                is_trot_race = filtered_df['race_type'].iloc[0].lower() == 'trot'
            
            if is_trot_race:
                print(f"[DEBUG] Trotting race detected - hiding inline analysis widgets (use TrottingAnalysisWindow only)")
                self.analysis_widget.setVisible(False)
                if hasattr(self, 'analysis_scroll'):
                    self.analysis_scroll.setVisible(False)
                return
        
        # Store current filter values for toggle purposes
        if hasattr(self, 'current_filtered_df'):
            self.current_filtered_df = filtered_df.copy() if not filtered_df.empty else None
            self.current_race_id_filter = race_id_text or ""
            self.current_date_filter = date_text or ""
            self.current_track_filter = track_text or ""
        
        # Check if analysis is enabled
        if not self.show_analysis_enabled:
            self.analysis_widget.setVisible(False)
            if hasattr(self, 'analysis_scroll'):
                self.analysis_scroll.setVisible(False)
            return
        
        # Only show analysis if both race_id and date are provided
        if not (race_id_text and date_text):
            print(f"[DEBUG] Skipping analysis: race_id_text={race_id_text}, date_text={date_text}")
            self.analysis_widget.setVisible(False)
            if hasattr(self, 'analysis_scroll'):
                self.analysis_scroll.setVisible(False)
            return
        
        # Check if the filtered_df represents a single race
        if filtered_df.empty:
            self.analysis_widget.setVisible(False)
            if hasattr(self, 'analysis_scroll'):
                self.analysis_scroll.setVisible(False)
            return
        
        # Verify we have a single race (all rows should have same race_id and race_date)
        unique_race_ids = filtered_df['REF_COURSE'].nunique() if 'REF_COURSE' in filtered_df.columns else 0
        unique_dates = filtered_df['RACE_DATE'].nunique() if 'RACE_DATE' in filtered_df.columns else 0
        
        if unique_race_ids != 1 or unique_dates != 1:
            self.analysis_widget.setVisible(False)
            if hasattr(self, 'analysis_scroll'):
                self.analysis_scroll.setVisible(False)
            return
        
        # We have a single race - show analysis and scroll area
        self.analysis_widget.setVisible(True)
        if hasattr(self, 'analysis_scroll'):
            self.analysis_scroll.setVisible(True)
        self.analysis_widget.setVisible(True)
        # remember filtered df for interactive updates
        self.last_filtered_df = filtered_df

        # Update analysis title to include race metadata (date, track, race_id)
        try:
            # prefer provided filter text; fallback to dataframe values
            race_date_disp = date_text if date_text else (str(filtered_df['RACE_DATE'].iloc[0]).strip() if 'RACE_DATE' in filtered_df.columns and not filtered_df['RACE_DATE'].isna().all() else '')
            race_id_disp = race_id_text if race_id_text else (str(filtered_df['REF_COURSE'].iloc[0]).strip() if 'REF_COURSE' in filtered_df.columns and not filtered_df['REF_COURSE'].isna().all() else '')
            # determine track/venue
            track_disp = ''
            if track_text:
                track_disp = track_text
            else:
                for col in ('HIPPODROME', 'track_name', 'race_track', 'track', 'Track', 'Lieu', 'location'):
                    if col in filtered_df.columns:
                        try:
                            val = filtered_df[col].dropna().astype(str).str.strip()
                            if not val.empty:
                                track_disp = val.iloc[0]
                                break
                        except Exception:
                            continue

            parts = [p for p in (race_date_disp, track_disp, race_id_disp) if p]
            if parts:
                title = f"[TARGET] Analyse de Course ({' | '.join(parts)})"
            else:
                title = "[TARGET] Analyse de Course (Une Seule Course)"
            if hasattr(self, 'analysis_title'):
                self.analysis_title.setText(title)
        except Exception as e:
            print(f"Error updating analysis title: {e}")

        # Run all analysis functions
        fitness_df = analyze_fitness_if(filtered_df)
        ic_df = analyze_class_ic(filtered_df)
        success_df = analyze_success_coeff(filtered_df)
        weight_stab_df = analyze_weight_stability(filtered_df)
        light_weight_df = analyze_light_weight_surprise(filtered_df)

        # Populate analysis tables
        self.populate_analysis_table(self.fitness_table, fitness_df, "Fitness")
        self.populate_analysis_table(self.ic_table, ic_df, "Class")
        self.populate_analysis_table(self.success_table, success_df, "Succes")
        self.populate_analysis_table(self.weight_stab_table, weight_stab_df, "Weight Stability")
        self.populate_analysis_table(self.light_weight_table, light_weight_df, "Poids Leger")

        # --- Summary composite and heatmap ---
        # Use refresh method so UI controls (e.g., corde slider) can re-run composite/heatmap
        self.refresh_composite_and_heatmap(filtered_df)
        
        # *** NEW: Compute and display outsider analysis (AFTER composite is generated) ***
        try:
            # Now current_composite_df is populated by refresh_composite_and_heatmap()
            if hasattr(self, 'current_composite_df') and self.current_composite_df is not None and not self.current_composite_df.empty:
                # Divergence analysis
                divergence_df = analyze_odds_divergence(filtered_df, self.current_composite_df)
                self.populate_analysis_table(self.outsiders_divergence_table, divergence_df, "Outsiders Divergence")
                
                # Consistency analysis
                consistency_df = analyze_consistency_score(filtered_df, self.current_composite_df)
                self.populate_analysis_table(self.outsiders_consistency_table, consistency_df, "Outsiders Consistency")
                
                # Underperforming favorites analysis
                underperforming_df = analyze_underperforming_favorites(filtered_df, self.current_composite_df)
                self.populate_analysis_table(self.underperforming_favorites_table, underperforming_df, "Underperforming Favorites")
                
                print(f"[DEBUG] Outsider analysis completed: {len(divergence_df)} divergence rows, {len(consistency_df)} consistency rows, {len(underperforming_df)} underperforming favorite rows")
        except Exception as e:
            print(f"[ERROR] Outsider analysis failed: {e}")
            import traceback
            traceback.print_exc()
        
        # Update in-app dashboard (aggregations + plots)
        try:
            if hasattr(self, 'update_dashboard'):
                self.update_dashboard(filtered_df)
        except Exception as e:
            print(f"Error updating dashboard from show_race_analysis: {e}")
        # ensure composite_df variable exists for downstream use (horse cards)
        composite_df = self.current_composite_df if hasattr(self, 'current_composite_df') and self.current_composite_df is not None else pd.DataFrame()

        # Populate All Horses detailed table
        try:
            if not filtered_df.empty:
                # Merge composite scores (and normalized Corde if present) into detailed view
                detailed = filtered_df.reset_index(drop=True).copy()
                
                # Find the Cheval column (case-insensitive) in both detailed and composite_df
                cheval_col_detailed = None
                cheval_col_composite = None
                for c in detailed.columns:
                    if c.upper() == 'CHEVAL':
                        cheval_col_detailed = c
                        break
                
                if composite_df is not None and not composite_df.empty:
                    for c in composite_df.columns:
                        if c.upper() == 'CHEVAL':
                            cheval_col_composite = c
                            break
                
                if cheval_col_composite and cheval_col_detailed:
                    merge_cols = [c for c in ['Cheval','Composite','Corde','COMPOSITE','CORDE'] if c in composite_df.columns]
                    if merge_cols:
                        # Rename composite_df columns to match detailed for merge
                        comp_rename = {c: c.lower() if c.isupper() else c for c in merge_cols}
                        merged = pd.merge(detailed, composite_df[merge_cols].rename(columns=comp_rename), left_on=cheval_col_detailed, right_on=cheval_col_composite, how='left')
                    else:
                        merged = detailed
                else:
                    merged = detailed

                # Reorder/clean columns for readability: show only a concise set of useful columns
                # Build list of columns to display, accepting case-insensitive variants
                candidates = [
                    ('CHEVAL', 'Cheval'),
                    ('N?', 'N?'),
                    ('N', 'N'),
                    ('COMPOSITE', 'Composite'),
                    ('COTE', 'Cote'),
                    ('IF', 'IF'),
                    ('IC', 'IC'),
                    ('S_COEFF', 'S_COEFF'),
                    ('N_WEIGHT', 'n_weight'),
                    ('POIDS', 'Poids'),
                    ('CORDE', 'Corde'),
                    ('EXPERIENCE', 'Experience'),
                    ('AGESCORE', 'AgeScore'),
                    ('SEXSCORE', 'SexScore')
                ]
                
                ordered_cols = []
                for upper_name, display_name in candidates:
                    for col in merged.columns:
                        if col.upper() == upper_name:
                            ordered_cols.append(col)
                            break
                
                # Fallback: if no ordered columns found, show a reasonable default set
                if not ordered_cols:
                    ordered_cols = [c for c in merged.columns if c.upper() in ('CHEVAL', 'COMPOSITE', 'COTE')]
                
                # If still empty, just show first few columns
                if not ordered_cols:
                    ordered_cols = merged.columns[:min(10, len(merged.columns))].tolist()
                
                display_df = merged[ordered_cols] if ordered_cols else merged
                self.populate_analysis_table(self.all_horses_table, display_df, "All Horses")
        except Exception as e:
            print(f"Error populating All Horses table: {e}")
            import traceback
            traceback.print_exc()

        # --- Horse cards ---
        # Clear previous cards
        while self.cards_layout.count():
            item = self.cards_layout.takeAt(0)
            w = item.widget()
            if w:
                w.setParent(None)

        # Build styled cards for each horse
        for idx, row in filtered_df.reset_index(drop=True).iterrows():
            card = QWidget()
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 8, 10, 8)
            card_layout.setSpacing(6)
            
            name = str(row.get('Cheval', ''))
            num = str(row.get('N?', row.get('N', '')))
            age = row.get('Age') if 'Age' in row.index else (row.get('AGE') if 'AGE' in row.index else '')
            sex = row.get('Sexe') if 'Sexe' in row.index else (row.get('SEXE') if 'SEXE' in row.index else '')
            cote = row.get('Cote', row.get('COTE', ''))
            composite_val = ''
            try:
                if 'Composite' in composite_df.columns:
                    composite_val = f"{composite_df.loc[composite_df['Cheval']==name,'Composite'].values[0]:.2f}"
            except Exception:
                composite_val = ''

            # Alternate card colors
            bg_color = '#f8f9ff' if idx % 2 == 0 else '#fff8f0'
            
            # --- Header: Horse number and name (compact) ---
            header_layout = QHBoxLayout()
            header_layout.setContentsMargins(0, 0, 0, 0)
            header_layout.setSpacing(8)
            
            num_label = QLabel(f"#{num}")
            num_label.setFont(QFont("Arial", 11, QFont.Bold))
            num_label.setStyleSheet("color: #2c3e50;")
            num_label.setMaximumWidth(40)
            
            name_label = QLabel(name)
            name_label.setFont(QFont("Arial", 10, QFont.Bold))
            name_label.setStyleSheet("color: #34495e;")
            
            header_layout.addWidget(num_label)
            header_layout.addWidget(name_label)
            
            # Composite score badge (inline)
            score_label = QLabel(f"{composite_val}")
            score_label.setFont(QFont("Arial", 9, QFont.Bold))
            score_label.setStyleSheet(f"background-color: #3498db; color: white; padding: 2px 6px; border-radius: 3px;")
            score_label.setAlignment(Qt.AlignCenter)
            score_label.setMaximumWidth(50)
            header_layout.addWidget(score_label)
            header_layout.addStretch()
            card_layout.addLayout(header_layout)
            
            # --- Info row: Age, Sex, Odds (compact) ---
            info_layout = QHBoxLayout()
            info_layout.setContentsMargins(0, 0, 0, 0)
            info_layout.setSpacing(8)
            
            age_label = QLabel(f"{age}y" if age else "e?")
            age_label.setStyleSheet("color: #7f8c8d; font-size: 8pt;")
            age_label.setMaximumWidth(30)
            
            sex_label = QLabel(f"{sex}")
            sex_label.setStyleSheet("color: #7f8c8d; font-size: 8pt;")
            sex_label.setMaximumWidth(20)
            
            cote_label = QLabel(f"{cote}")
            cote_label.setStyleSheet("color: #e74c3c; font-weight: bold; font-size: 9pt;")
            cote_label.setMaximumWidth(40)
            
            info_layout.addWidget(QLabel("Age:"))
            info_layout.addWidget(age_label)
            info_layout.addWidget(QLabel("Sex:"))
            info_layout.addWidget(sex_label)
            info_layout.addWidget(QLabel("Odds:"))
            info_layout.addWidget(cote_label)
            info_layout.addStretch()
            card_layout.addLayout(info_layout)
            
            # --- Metrics grid (compact, single row with abbreviations) ---
            metrics_layout = QHBoxLayout()
            metrics_layout.setContentsMargins(0, 0, 0, 0)
            metrics_layout.setSpacing(10)
            
            for key in ['IF', 'IC', 'S_COEFF', 'n_weight', 'Poids', 'POIDS']:
                if key in filtered_df.columns:
                    val = row.get(key, '')
                    try:
                        val = f"{float(val):.1f}" if pd.notna(val) else 'e?'
                    except:
                        val = str(val)
                    # Create compact label with abbreviation
                    abbrev = key.replace('n_weight', 'Wt').replace('Poids', 'Kg').replace('POIDS', 'Kg')
                    metric_label = QLabel(f"{abbrev}:{val}")
                    metric_label.setStyleSheet("color: #2c3e50; font-size: 8pt; padding: 0px;")
                    metric_label.setMaximumWidth(60)
                    metrics_layout.addWidget(metric_label)
            
            metrics_layout.addStretch()
            card_layout.addLayout(metrics_layout)
            
            # Apply card styling
            card_stylesheet = f"""
                QWidget {{
                    background-color: {bg_color};
                    border: 2px solid #bdc3c7;
                    border-radius: 8px;
                    padding: 0px;
                }}
                QWidget:hover {{
                    border: 2px solid #3498db;
                    background-color: #f0f8ff;
                }}
            """
            card.setStyleSheet(card_stylesheet)
            card.setMaximumHeight(110)
            self.cards_layout.addWidget(card)

        # Compute and display automated prognosis (numbers only)
        try:
            prog = compute_prognosis(filtered_df, max_len=8)
            # Sort prognosis by composite score (descending) if composite_df available
            if prog and composite_df is not None and not composite_df.empty and 'Composite' in composite_df.columns:
                # Build N? -> Composite score mapping
                num_col = 'N?' if 'N?' in composite_df.columns else ('N' if 'N' in composite_df.columns else None)
                if num_col:
                    score_map = {}
                    for _, row in composite_df.iterrows():
                        try:
                            n = str(row[num_col]).strip()
                            if n.endswith('.0'):
                                n = n[:-2]
                            if n and n != '0' and n.lower() not in ('nan', 'none'):
                                score_map[n] = float(row['Composite'])
                        except Exception:
                            pass
                    # Sort prognosis by score, preserving order for ties
                    prog = sorted(prog, key=lambda x: score_map.get(x, -1), reverse=True)
            # sanitize prognosis list (remove zeros/nan/none/non-digits)
            prog = sanitize_horse_list(prog)
            # format as space-separated N? values
            if prog:
                if hasattr(self, 'prognosis_label'):
                    self.prognosis_label.setText('  '.join(prog))
                if hasattr(self, 'prognosis_label_detailed'):
                    self.prognosis_label_detailed.setText('  '.join(prog))
            else:
                if hasattr(self, 'prognosis_label'):
                    self.prognosis_label.setText('')
                if hasattr(self, 'prognosis_label_detailed'):
                    self.prognosis_label_detailed.setText('')
        except Exception as e:
            print(f"Error updating prognosis display: {e}")
        
        # Compute and display summary horses (first-half of composite rankings)
        try:
            summary = compute_summary_horses(composite_df) if composite_df is not None and not composite_df.empty else []
            summary = sanitize_horse_list(summary)
            if summary:
                if hasattr(self, 'summary_label'):
                    self.summary_label.setText('  '.join(summary))
                if hasattr(self, 'summary_label_detailed'):
                    self.summary_label_detailed.setText('  '.join(summary))
            else:
                if hasattr(self, 'summary_label'):
                    self.summary_label.setText('')
                if hasattr(self, 'summary_label_detailed'):
                    self.summary_label_detailed.setText('')
        except Exception as e:
            print(f"Error updating summary display: {e}")
        
        # Compute and display exclusive horses (in summary but NOT in prognosis)
        try:
            prog = compute_prognosis(filtered_df, max_len=8)
            prog = sanitize_horse_list(prog)
            summary = compute_summary_horses(composite_df) if composite_df is not None and not composite_df.empty else []
            summary = sanitize_horse_list(summary)
            # summary-only (in summary but not in prognosis)
            exclusive = [h for h in summary if h not in prog]
            if exclusive:
                if hasattr(self, 'exclusive_label'):
                    self.exclusive_label.setText('  '.join(exclusive))
                if hasattr(self, 'exclusive_label_detailed'):
                    self.exclusive_label_detailed.setText('  '.join(exclusive))
            else:
                if hasattr(self, 'exclusive_label'):
                    self.exclusive_label.setText('')
                if hasattr(self, 'exclusive_label_detailed'):
                    self.exclusive_label_detailed.setText('')

            # prognosis-only (in prognosis but not in summary) -> populate new widget
            prognosis_only = [h for h in prog if h not in summary]
            if hasattr(self, 'prognosis_only_label'):
                if prognosis_only:
                    self.prognosis_only_label.setText('  '.join(prognosis_only))
                    if hasattr(self, 'prognosis_only_label_detailed'):
                        self.prognosis_only_label_detailed.setText('  '.join(prognosis_only))
                else:
                    self.prognosis_only_label.setText('')
                    if hasattr(self, 'prognosis_only_label_detailed'):
                        self.prognosis_only_label_detailed.setText('')

            # summary+prognosis intersection (horses found in both)
            summary_prognosis_intersection = [h for h in prog if h in summary]
            if hasattr(self, 'summary_prognosis_label'):
                if summary_prognosis_intersection:
                    self.summary_prognosis_label.setText('  '.join(summary_prognosis_intersection))
                    if hasattr(self, 'summary_prognosis_label_detailed'):
                        self.summary_prognosis_label_detailed.setText('  '.join(summary_prognosis_intersection))
                else:
                    self.summary_prognosis_label.setText('')
                    if hasattr(self, 'summary_prognosis_label_detailed'):
                        self.summary_prognosis_label_detailed.setText('')

            # Compute trending horses based on odds evolution and display after Exclusives
            try:
                trending = compute_trending_horses(filtered_df, top_n=3)
                if trending:
                    # show top 3 trending horses
                    if hasattr(self, 'trending_label'):
                        self.trending_label.setText('  '.join(trending[:3]))
                    if hasattr(self, 'trending_label_detailed'):
                        self.trending_label_detailed.setText('  '.join(trending[:3]))
                else:
                    if hasattr(self, 'trending_label'):
                        self.trending_label.setText('')
                    if hasattr(self, 'trending_label_detailed'):
                        self.trending_label_detailed.setText('')
            except Exception as te:
                print(f"Error computing/displaying trending horses: {te}")
                if hasattr(self, 'trending_label'):
                    self.trending_label.setText('')
                if hasattr(self, 'trending_label_detailed'):
                    self.trending_label_detailed.setText('')
            
            # Compute favorable corde horses and display
            try:
                hippodrome = None
                distance = None
                if 'HIPPODROME' in filtered_df.columns and not filtered_df['HIPPODROME'].empty:
                    hippodrome = filtered_df['HIPPODROME'].iloc[0]
                if 'DISTANCE' in filtered_df.columns and not filtered_df['DISTANCE'].empty:
                    distance = filtered_df['DISTANCE'].iloc[0]
                
                # Debug logging
                print(f"[DEBUG cordes] hippodrome={hippodrome}, distance={distance}")
                print(f"[DEBUG cordes] filtered_df columns: {filtered_df.columns.tolist()}")
                print(f"[DEBUG cordes] 'N?' in columns: {'N?' in filtered_df.columns}")
                print(f"[DEBUG cordes] 'CORDE' in columns: {'CORDE' in filtered_df.columns}")
                
                favorable_horses = compute_favorable_corde_horses(filtered_df, hippodrome, distance, top_n=3)
                print(f"[DEBUG cordes] Result: {favorable_horses}")
                
                if favorable_horses and hasattr(self, 'favorable_cordes_label'):
                    self.favorable_cordes_label.setText('  '.join(favorable_horses))
                    if hasattr(self, 'favorable_cordes_label_detailed'):
                        self.favorable_cordes_label_detailed.setText('  '.join(favorable_horses))
                elif hasattr(self, 'favorable_cordes_label'):
                    self.favorable_cordes_label.setText('')
                    if hasattr(self, 'favorable_cordes_label_detailed'):
                        self.favorable_cordes_label_detailed.setText('')
            except Exception as ce:
                print(f"Error computing/displaying favorable corde horses: {ce}")
                import traceback
                traceback.print_exc()
                if hasattr(self, 'favorable_cordes_label'):
                    self.favorable_cordes_label.setText('')
                if hasattr(self, 'favorable_cordes_label_detailed'):
                    self.favorable_cordes_label_detailed.setText('')
        except Exception as e:
            print(f"Error updating exclusive display: {e}")

        # Compute and display race statistics
        try:
            stats = compute_race_statistics(filtered_df, composite_df)
            if stats and hasattr(self, 'stats_label_detailed'):
                stats_text = ""
                if 'avg_odds' in stats:
                    horses = stats.get('avg_odds_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Cote Moyenne: {stats['avg_odds']:.2f} ({horses_str})\n"
                if 'median_odds' in stats:
                    horses = stats.get('median_odds_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Cote Mediane: {stats['median_odds']:.2f} ({horses_str})\n"
                if 'mean_composite' in stats:
                    horses = stats.get('mean_composite_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Composite Moyen: {stats['mean_composite']:.4f} ({horses_str})\n"
                if 'median_composite' in stats:
                    horses = stats.get('median_composite_horses', [])
                    horses_str = ' '.join(horses) if horses else 'e?'
                    stats_text += f"Composite Median: {stats['median_composite']:.4f} ({horses_str})"
                
                self.stats_label_detailed.setText(stats_text)
            elif hasattr(self, 'stats_label_detailed'):
                self.stats_label_detailed.setText('')
        except Exception as e:
            print(f"Error computing/displaying race statistics: {e}")
            if hasattr(self, 'stats_label_detailed'):
                self.stats_label_detailed.setText('')

        # Ensure bets are refreshed for this race automatically
        try:
            self.generate_bets()
        except Exception as e:
            print(f"Error auto-generating bets after filter change: {e}")

    
    
    def delete_selected_rows(self):
        current_table = self.db_table if self.tab_widget.currentIndex() == 1 else self.current_table
        selected_rows = set()

        for item in current_table.selectedItems():
            selected_rows.add(item.row())

        # If we have an active single-race or single-track+date filter, offer option to delete entire race
        single_race_available = False
        race_id_val = None
        race_date_val = None
        track_val = None
        delete_by_track = False
        if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
            try:
                unique_race_ids = self.last_filtered_df['REF_COURSE'].nunique() if 'REF_COURSE' in self.last_filtered_df.columns else 0
                unique_dates = self.last_filtered_df['RACE_DATE'].nunique() if 'RACE_DATE' in self.last_filtered_df.columns else 0
                if 'HIPPODROME' in self.last_filtered_df.columns:
                    unique_tracks = self.last_filtered_df['HIPPODROME'].nunique()
                else:
                    if 'HIPPODROME' in self.last_filtered_df.columns:
                        unique_tracks = self.last_filtered_df['HIPPODROME'].nunique()
                    else:
                        unique_tracks = self.last_filtered_df['track_name'].nunique() if 'track_name' in self.last_filtered_df.columns else 0

                # Prefer race_id if available, otherwise allow track_name+date deletion
                if unique_race_ids == 1 and unique_dates == 1:
                    single_race_available = True
                    race_id_val = str(self.last_filtered_df['REF_COURSE'].iloc[0])
                    race_date_val = str(self.last_filtered_df['RACE_DATE'].iloc[0])
                    delete_by_track = False
                elif unique_tracks == 1 and unique_dates == 1:
                    single_race_available = True
                    track_val = str(self.last_filtered_df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in self.last_filtered_df.columns else str(self.last_filtered_df['track_name'].iloc[0])
                    race_date_val = str(self.last_filtered_df['RACE_DATE'].iloc[0])
                    delete_by_track = True
            except Exception:
                single_race_available = False

        # If no rows selected but a single race/track filter is available, ask to delete entire race (or track/date)
        if not selected_rows and single_race_available:
            if delete_by_track:
                prompt = f"No rows selected. Delete ALL entries for track={track_val} on {race_date_val}?"
            else:
                prompt = f"No rows selected. Delete ALL entries for race_id={race_id_val} on {race_date_val}?"

            reply = QMessageBox.question(self, "Supprimer la Course ?", prompt, QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                # proceed to delete entire race/track
                deleted_count = 0
                try:
                    # determine distinct sources present in the filtered df
                    sources = []
                    if 'source_db' in self.last_filtered_df.columns:
                        sources = list(self.last_filtered_df['source_db'].dropna().unique())
                    if not sources and 'source' in self.last_filtered_df.columns:
                        sources = list(self.last_filtered_df['source'].dropna().unique())

                    # normalize to list of source strings
                    for src in set(sources):
                        db_name = None
                        table_name = None
                        if isinstance(src, str) and src.strip():
                            db_name = src
                            # try to infer table from source text
                            if 'Flat' in src:
                                table_name = 'flat_races'
                            else:
                                table_name = 'trot_races'
                        else:
                            # fallback: inspect sample row source column
                            sample_source = self.last_filtered_df['source'].dropna().iloc[0] if 'source' in self.last_filtered_df.columns and not self.last_filtered_df['source'].dropna().empty else ''
                            if 'Zone-Turf' in sample_source:
                                db_name = 'zone_turf_flat.db' if 'Flat' in sample_source else 'zone_turf_trot.db'
                            table_name = 'flat_races' if 'Flat' in sample_source else 'trot_races'

                        if not db_name:
                            continue

                        try:
                            conn = sqlite3.connect(db_name)
                            cursor = conn.cursor()
                            # attempt deletion in both possible tables to be safe
                            for tbl in [table_name, 'flat_races', 'trot_races']:
                                try:
                                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
                                    if cursor.fetchone():
                                        if delete_by_track:
                                            cursor.execute(f"DELETE FROM {tbl} WHERE track_name=? AND race_date=?", (track_val, race_date_val))
                                        else:
                                            cursor.execute(f"DELETE FROM {tbl} WHERE REF_COURSE=? AND race_date=?", (race_id_val, race_date_val))
                                        deleted_count += cursor.rowcount
                                        conn.commit()
                                except Exception:
                                    pass
                            conn.close()
                        except Exception:
                            pass

                    self.load_database_data()
                    if delete_by_track:
                        QMessageBox.information(self, "Succes", f"{deleted_count} entrees supprimees de la base de donnees pour l'hippodrome {track_val} le {race_date_val}")
                    else:
                        QMessageBox.information(self, "Succes", f"{deleted_count} entrees supprimees de la base de donnees pour la course {race_id_val} le {race_date_val}")
                    
                    # Re-apply filters and re-compute analysis after deletion
                    try:
                        self.apply_filters()
                    except Exception as e:
                        print(f"Error re-applying filters after deletion: {e}")
                    
                    return
                except Exception as e:
                    QMessageBox.critical(self, "Error", f"Failed to delete race entries: {e}")
                    return

        # If user selected rows, confirm deletion of selected rows (existing behavior)
        if not selected_rows:
            QMessageBox.warning(self, "Avertissement", "Aucune ligne selectionnee")
            return

        # Ask user whether they want to delete selected rows or entire filtered race if available
        if single_race_available:
            msg = QMessageBox(self)
            msg.setWindowTitle("Delete Rows")
            if delete_by_track:
                msg.setText(f"Delete {len(selected_rows)} selected rows?\nOr delete ALL entries for track={track_val} on {race_date_val}?")
            else:
                msg.setText(f"Delete {len(selected_rows)} selected rows?\nOr delete ALL entries for race_id={race_id_val} on {race_date_val}?")
            btn_selected = msg.addButton("Delete Selected", QMessageBox.AcceptRole)
            btn_race = msg.addButton("Delete Entire Race", QMessageBox.DestructiveRole)
            btn_cancel = msg.addButton(QMessageBox.Cancel)
            msg.exec_()

            clicked = msg.clickedButton()
            if clicked == btn_cancel:
                return
            elif clicked == btn_race:
                # delete entire race
                deleted_count = 0
                try:
                    # determine distinct sources present in the filtered df
                    sources = []
                    if 'source_db' in self.last_filtered_df.columns:
                        sources = list(self.last_filtered_df['source_db'].dropna().unique())
                    if not sources and 'source' in self.last_filtered_df.columns:
                        sources = list(self.last_filtered_df['source'].dropna().unique())

                    for src in set(sources):
                        db_name = None
                        table_name = None
                        if isinstance(src, str) and src.strip():
                            db_name = src
                            if 'Flat' in src:
                                table_name = 'flat_races'
                            else:
                                table_name = 'trot_races'
                        else:
                            sample_source = self.last_filtered_df['source'].dropna().iloc[0] if 'source' in self.last_filtered_df.columns and not self.last_filtered_df['source'].dropna().empty else ''
                            if 'Zone-Turf' in sample_source:
                                db_name = 'zone_turf_flat.db' if 'Flat' in sample_source else 'zone_turf_trot.db'
                            table_name = 'flat_races' if 'Flat' in sample_source else 'trot_races'

                        if not db_name:
                            continue

                        try:
                            conn = sqlite3.connect(db_name)
                            cursor = conn.cursor()
                            for tbl in [table_name, 'flat_races', 'trot_races']:
                                try:
                                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
                                    if cursor.fetchone():
                                        if delete_by_track:
                                            cursor.execute(f"DELETE FROM {tbl} WHERE track_name=? AND race_date=?", (track_val, race_date_val))
                                        else:
                                            cursor.execute(f"DELETE FROM {tbl} WHERE race_id=? AND race_date=?", (race_id_val, race_date_val))
                                        deleted_count += cursor.rowcount
                                        conn.commit()
                                except Exception:
                                    pass
                            conn.close()
                        except Exception:
                            pass

                    self.load_database_data()
                    QMessageBox.information(self, "Succes", f"{deleted_count} entrees supprimees de la base de donnees pour la course {race_id_val} le {race_date_val}")
                    
                    # Re-apply filters and re-compute analysis after deletion
                    try:
                        self.apply_filters()
                    except Exception as e:
                        print(f"Error re-applying filters after deletion: {e}")
                    
                    return
                except Exception as e:
                    QMessageBox.critical(self, "Error", f"Failed to delete race entries: {e}")
                    return

        # Proceed with deleting selected rows
        reply = QMessageBox.question(self, "Confirm Delete",
                                     f"Delete {len(selected_rows)} selected rows?",
                                     QMessageBox.Yes | QMessageBox.No)

        if reply == QMessageBox.Yes:
            # Get column indices and headers
            headers = [current_table.horizontalHeaderItem(i).text() for i in range(current_table.columnCount())]
            race_id_col = headers.index('REF_COURSE') if 'REF_COURSE' in headers else None
            
            # Try different column name variations for race_date
            race_date_col = None
            if 'race_date' in headers:
                race_date_col = headers.index('race_date')
            elif 'RACE_DATE' in headers:
                race_date_col = headers.index('RACE_DATE')
            elif 'race_DATE' in headers:
                race_date_col = headers.index('race_DATE')
            
            source_col = headers.index('source') if 'source' in headers else None
            source_db_col = headers.index('source_db') if 'source_db' in headers else None

            print(f"[DEBUG delete] headers: {headers}")
            print(f"[DEBUG delete] race_id_col: {race_id_col}, race_date_col: {race_date_col}")
            
            if race_id_col is None or race_date_col is None:
                QMessageBox.warning(self, "Avertissement", f"Impossible de trouver les colonnes requises (REF_COURSE, race_date). Disponibles : {headers}")
                return

            deleted_count = 0
            try:
                for row in selected_rows:
                    race_id = current_table.item(row, race_id_col).text() if current_table.item(row, race_id_col) else None
                    race_date = current_table.item(row, race_date_col).text() if current_table.item(row, race_date_col) else None

                    if race_id and race_date:
                        # Determine database and table from source_db column or source column
                        db_name = None
                        table_name = None

                        if source_db_col is not None and current_table.item(row, source_db_col):
                            db_name = current_table.item(row, source_db_col).text()
                            # Try to guess table name from data
                            if source_col is not None and current_table.item(row, source_col) and 'Flat' in current_table.item(row, source_col).text():
                                table_name = 'flat_races'
                            else:
                                table_name = 'trot_races'
                        elif source_col is not None:
                            source = current_table.item(row, source_col).text()
                            if 'Zone-Turf' in source:
                                db_name = 'zone_turf_flat.db' if 'Flat' in source else 'zone_turf_trot.db'
                            table_name = 'flat_races' if 'Flat' in source else 'trot_races'

                        if db_name and table_name:
                            conn = sqlite3.connect(db_name)
                            cursor = conn.cursor()
                            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                            if cursor.fetchone():
                                # Get the horse name from Cheval column if available
                                if 'Cheval' in headers:
                                    cheval_col = headers.index('Cheval')
                                    horse_name = current_table.item(row, cheval_col).text() if current_table.item(row, cheval_col) else None
                                    if horse_name:
                                        cursor.execute(f"DELETE FROM {table_name} WHERE REF_COURSE=? AND race_date=? AND Cheval=?", 
                                                     (race_id, race_date, horse_name))
                                    else:
                                        cursor.execute(f"DELETE FROM {table_name} WHERE REF_COURSE=? AND race_date=?", 
                                                     (race_id, race_date))
                                else:
                                    cursor.execute(f"DELETE FROM {table_name} WHERE REF_COURSE=? AND race_date=?", 
                                                 (race_id, race_date))
                                deleted_count += cursor.rowcount
                                conn.commit()
                            conn.close()

                self.load_database_data()
                QMessageBox.information(self, "Succes", f"{deleted_count} entrees supprimees de la base de donnees")
                
                # Re-apply filters and re-compute analysis for the race
                try:
                    self.apply_filters()
                except Exception as e:
                    print(f"Error re-applying filters after deletion: {e}")

            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to delete rows: {e}")

    def delete_filtered_entries(self):
        """Delete all database entries matching the current filtered race or track+date."""
        if not hasattr(self, 'last_filtered_df') or self.last_filtered_df is None or self.last_filtered_df.empty:
            QMessageBox.warning(self, "Avertissement", "Aucun filtre actif e supprimer")
            return

        # Determine whether we have race_id+date or track+date
        try:
            unique_race_ids = self.last_filtered_df['REF_COURSE'].nunique() if 'REF_COURSE' in self.last_filtered_df.columns else 0
            unique_dates = self.last_filtered_df['RACE_DATE'].nunique() if 'RACE_DATE' in self.last_filtered_df.columns else 0
            if 'HIPPODROME' in self.last_filtered_df.columns:
                unique_tracks = self.last_filtered_df['HIPPODROME'].nunique()
            else:
                unique_tracks = self.last_filtered_df['track_name'].nunique() if 'track_name' in self.last_filtered_df.columns else 0
        except Exception:
            QMessageBox.warning(self, "Avertissement", "Impossible d'interpreter le filtre actuel")
            return

        delete_by_track = False
        if unique_race_ids == 1 and unique_dates == 1:
            race_id_val = str(self.last_filtered_df['REF_COURSE'].iloc[0])
            race_date_val = str(self.last_filtered_df['RACE_DATE'].iloc[0])
        elif unique_tracks == 1 and unique_dates == 1:
            delete_by_track = True
            track_val = str(self.last_filtered_df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in self.last_filtered_df.columns else str(self.last_filtered_df['track_name'].iloc[0]) if 'track_name' in self.last_filtered_df.columns else ''
            race_date_val = str(self.last_filtered_df['RACE_DATE'].iloc[0])
        else:
            QMessageBox.warning(self, "Avertissement", "Veuillez filtrer pour une seule course (race_id+date) ou un seul hippodrome+date avant d'utiliser cette action.")
            return

        if delete_by_track:
            prompt = f"Delete ALL entries for track={track_val} on {race_date_val}?"
        else:
            prompt = f"Delete ALL entries for race_id={race_id_val} on {race_date_val}?"

        reply = QMessageBox.question(self, "Confirm Delete Filtered", prompt, QMessageBox.Yes | QMessageBox.No)
        if reply != QMessageBox.Yes:
            return

        deleted_count = 0
        # collect candidate sources
        sources = []
        if 'source_db' in self.last_filtered_df.columns:
            sources = list(self.last_filtered_df['source_db'].dropna().unique())
        if not sources and 'source' in self.last_filtered_df.columns:
            sources = list(self.last_filtered_df['source'].dropna().unique())

        for src in set(sources):
            db_name = None
            table_name = None
            if isinstance(src, str) and src.strip():
                db_name = src
                table_name = 'flat_races' if 'Flat' in src else 'trot_races'
            else:
                sample_source = self.last_filtered_df['source'].dropna().iloc[0] if 'source' in self.last_filtered_df.columns and not self.last_filtered_df['source'].dropna().empty else ''
                if 'Zone-Turf' in sample_source:
                    db_name = 'zone_turf_flat.db' if 'Flat' in sample_source else 'zone_turf_trot.db'
                table_name = 'flat_races' if 'Flat' in sample_source else 'trot_races'

            if not db_name:
                continue

            try:
                conn = sqlite3.connect(db_name)
                cursor = conn.cursor()
                for tbl in [table_name, 'flat_races', 'trot_races']:
                    try:
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
                        if cursor.fetchone():
                            if delete_by_track:
                                cursor.execute(f"DELETE FROM {tbl} WHERE track_name=? AND race_date=?", (track_val, race_date_val))
                            else:
                                cursor.execute(f"DELETE FROM {tbl} WHERE race_id=? AND race_date=?", (race_id_val, race_date_val))
                            deleted_count += cursor.rowcount
                            conn.commit()
                    except Exception:
                        pass
                conn.close()
            except Exception:
                pass

        self.load_database_data()
        if delete_by_track:
            QMessageBox.information(self, "Succes", f"{deleted_count} entrees supprimees de la base de donnees pour l'hippodrome {track_val} le {race_date_val}")
        else:
            QMessageBox.information(self, "Succes", f"{deleted_count} entrees supprimees de la base de donnees pour la course {race_id_val} le {race_date_val}")
        
        # Re-apply filters and re-compute analysis after deletion
        try:
            self.apply_filters()
        except Exception as e:
            print(f"Error re-applying filters after deletion: {e}")

    def toggle_control_panel(self, checked=None):
        """Show/hide the right-side control panel (compact mode)."""
        try:
            vis = self.vertical_widget.isVisible()
            self.vertical_widget.setVisible(not vis)
            # reflect in action if present
            try:
                self.control_panel_action.setChecked(not vis)
            except Exception:
                pass
        except Exception as e:
            print(f"Error toggling control panel: {e}")

    def toggle_dark_theme(self, checked=None):
        """Toggle dark theme and persist preference."""
        try:
            from PyQt5.QtWidgets import QApplication
            if checked is None:
                # toggle current
                try:
                    checked = not bool(self.settings.value('dark_theme', False, type=bool))
                except Exception:
                    checked = True
            if checked:
                # apply extended dark theme by default
                QApplication.instance().setStyleSheet(self.DARK_STYLESHEET_EXTENDED)
                self.settings.setValue('theme', 'Dark')
                self.settings.setValue('dark_theme', True)
            else:
                QApplication.instance().setStyleSheet("")
                self.settings.setValue('theme', 'Light')
                self.settings.setValue('dark_theme', False)
            try:
                self.dark_theme_action.setChecked(bool(checked))
            except Exception:
                pass
        except Exception as e:
            print(f"Error toggling dark theme: {e}")

    def apply_theme(self, theme_name):
        """Apply a named theme and persist preference."""
        try:
            from PyQt5.QtWidgets import QApplication
            tn = (theme_name or 'Light').lower()
            if tn.startswith('dark'):
                # Apply main dark stylesheet and dialog dark stylesheet
                QApplication.instance().setStyleSheet(self.DARK_STYLESHEET_EXTENDED + "\n" + self.DIALOG_STYLESHEET_DARK)
                self.settings.setValue('theme', 'Dark')
                try:
                    self.dark_theme_action.setChecked(True)
                except Exception:
                    pass
            elif tn.startswith('high'):
                QApplication.instance().setStyleSheet(self.HIGH_CONTRAST_DARK + "\n" + self.DIALOG_STYLESHEET_DARK)
                self.settings.setValue('theme', 'HighContrast')
                try:
                    self.dark_theme_action.setChecked(True)
                except Exception:
                    pass
            else:
                # Apply light dialog stylesheet on top of default
                QApplication.instance().setStyleSheet(self.DIALOG_STYLESHEET)
                self.settings.setValue('theme', 'Light')
                try:
                    self.dark_theme_action.setChecked(False)
                except Exception:
                    pass
        except Exception as e:
            print(f"Error applying theme {theme_name}: {e}")

    def apply_dialog_style(self, dialog):
        """Apply appropriate dialog stylesheet based on current theme."""
        try:
            current_theme = self.settings.value('theme', 'Light', type=str).lower()
            if 'dark' in current_theme or 'high' in current_theme:
                dialog.setStyleSheet(self.DIALOG_STYLESHEET_DARK)
            else:
                dialog.setStyleSheet(self.DIALOG_STYLESHEET)
        except Exception as e:
            print(f"Error applying dialog style: {e}")

    def set_compact_mode(self, enabled):
        """Enable/disable compact mode and persist preference."""
        try:
            # apply compact app stylesheet globally when enabled
            from PyQt5.QtWidgets import QApplication
            if enabled:
                # add compact application stylesheet
                existing = QApplication.instance().styleSheet() or ''
                QApplication.instance().setStyleSheet(existing + "\n" + self.COMPACT_APP_STYLESHEET)
                self.settings.setValue('compact_mode', True)
            else:
                # naive approach: clear compact stylesheet by restoring theme
                cur_theme = str(self.settings.value('theme', 'Light'))
                if cur_theme and cur_theme.lower().startswith('dark'):
                    QApplication.instance().setStyleSheet(self.DARK_STYLESHEET_EXTENDED)
                elif cur_theme and cur_theme.lower().startswith('high'):
                    QApplication.instance().setStyleSheet(self.HIGH_CONTRAST_DARK)
                else:
                    QApplication.instance().setStyleSheet("")
                self.settings.setValue('compact_mode', False)
            try:
                self.compact_action.setChecked(bool(enabled))
            except Exception:
                pass
        except Exception as e:
            print(f"Error setting compact mode: {e}")

    def on_corde_weight_changed(self, value):
        pass

    def on_metric_weight_changed(self, metric, value):
        """Generic handler for metric sliders. Persists value and refreshes composite/heatmap and regenerates bets."""
        try:
            perc = int(value)
            self.metric_weights[metric] = float(perc) / 100.0
            # persist
            try:
                self.settings.setValue(metric, perc)
            except Exception:
                pass

            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and self.analysis_widget.isVisible():
                # Refresh composite scores and heatmap with new weights
                self.refresh_composite_and_heatmap(self.last_filtered_df)
                
                # Update race statistics with new weights
                composite_df = self.current_composite_df if hasattr(self, 'current_composite_df') else pd.DataFrame()
                self.update_race_statistics_display(self.last_filtered_df, composite_df)
                
                # Regenerate betting combinations with new composite rankings
                try:
                    self.generate_bets()
                except Exception as e:
                    print(f"[WARN] Error regenerating bets after weight change: {e}")
        except Exception as e:
            print(f"Error in on_metric_weight_changed: {e}")
    
    def remove_duplicates(self):
        """
        Remove duplicate horse entries from database.
        A duplicate is a horse that has multiple entries for the same day, meeting, or race ID.
        """
        if not hasattr(self, 'full_df') or self.full_df.empty:
            QMessageBox.warning(self, "Avertissement", "Aucune donnee e traiter")
            return
        
        original_count = len(self.full_df)
        
        try:
            # Process each source separately
            sources = self.full_df['source'].unique() if 'source' in self.full_df.columns else []
            
            for source in sources:
                source_df = self.full_df[self.full_df['source'] == source]
                if source_df.empty:
                    continue
                
                # Determine database and table names
                if 'Flat' in source:
                    if 'Zone-Turf' in source:
                        db_name = 'zone_turf_flat.db'
                    else:
                        db_name = 'turfomania_flat.db'
                    table_name = 'flat_races'
                else:
                    if 'Zone-Turf' in source:
                        db_name = 'zone_turf_trot.db'
                    else:
                        db_name = 'zone_turf_trot.db'
                    table_name = 'trot_races'
                
                # Deduplicate: a duplicate is a horse with multiple entries for the same day/meeting/race
                dup_cols = []
                
                # Horse identifier
                horse_col = None
                for col in source_df.columns:
                    if col.lower() in ['horse_name', 'horsename', 'cheval', 'nom_cheval']:
                        horse_col = col
                        break
                
                if horse_col:
                    dup_cols.append(horse_col)
                
                # Date identifier (same day)
                if 'RACE_DATE' in source_df.columns:
                    dup_cols.append('RACE_DATE')
                elif 'race_date' in source_df.columns:
                    dup_cols.append('race_date')
                
                # Meeting/Race identifier
                if 'REF_COURSE' in source_df.columns:
                    dup_cols.append('REF_COURSE')
                elif 'RACE_ID' in source_df.columns:
                    dup_cols.append('RACE_ID')
                elif 'race_id' in source_df.columns:
                    dup_cols.append('race_id')
                
                # Only deduplicate if we have horse name + date + meeting/race
                if len(dup_cols) >= 3:
                    deduped = source_df.drop_duplicates(subset=dup_cols, keep='first')
                    deduped_clean = deduped.drop(['source', 'source_db'], axis=1, errors='ignore')
                    
                    conn = sqlite3.connect(db_name)
                    conn.execute(f"DELETE FROM {table_name}")
                    deduped_clean.to_sql(table_name, conn, if_exists='append', index=False)
                    conn.close()
            
            self.load_database_data()
            removed_count = original_count - len(self.full_df)
            self.populate_table(self.db_table, self.full_df)
            self.update_stats(self.full_df)
            self.status_label.setText(f"Removed {removed_count} duplicates")
            QMessageBox.information(self, "Succes", f"{removed_count} entrees dupliquees supprimees")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to update database: {e}")
    
    def generate_combos_flash_tickets(self, base, associates, desired_size, composite_df, prog_only_horses=None, excl_horses=None):
        """
        Generate combinations using Flash Tickets deterministic pool-based logic.
        Adaptive: for races with >10 starters, expand pools to include middle positions
        and encourage Prog Only / Exclusive horses.
        
        Args:
            base: list of mandatory horse numbers
            associates: list of optional horse numbers
            desired_size: number of horses per combination
            composite_df: DataFrame with horses ranked by composite score
            prog_only_horses: set of Prog Only horse numbers (optional)
            excl_horses: set of Exclusive horse numbers (optional)
        
        Returns:
            list of tuples (combinations) or empty list if generation failed
        """
        combos = []
        try:
            print(f"[DEBUG] generate_combos_flash_tickets called: desired_size={desired_size}, base={base}, associates={associates}")
            # Extract summary_order from composite DataFrame
            def _summary_order(df):
                out = []
                for col in ['N°', 'N', 'Num', 'Numero']:
                    if col in df.columns:
                        vals = df[col].tolist()
                        for x in vals:
                            try:
                                s = str(x).strip()
                                if s.endswith('.0'):
                                    s = s[:-2]
                                if not s or s.lower() in ('nan', 'none') or s == '0':
                                    continue
                                out.append(s)
                            except Exception:
                                continue
                        return out
                if 'Cheval' in df.columns:
                    for x in df['Cheval'].tolist():
                        try:
                            s = str(x).strip()
                            if not s or s.lower() in ('nan', 'none'):
                                continue
                            out.append(s)
                        except Exception:
                            continue
                    return out
                for x in df.index.astype(str).tolist():
                    try:
                        s = str(x).strip()
                        if not s or s.lower() in ('nan', 'none') or s == '0':
                            continue
                        out.append(s)
                    except Exception:
                        continue
                return out
            
            summary_order = _summary_order(composite_df)
            print(f"[DEBUG] composite_df columns: {composite_df.columns.tolist()}")
            print(f"[DEBUG] composite_df shape: {composite_df.shape}")
            print(f"[DEBUG] summary_order (horse numbers in ranking order): {summary_order}")
            
            # Detect race difficulty
            starters = len(summary_order)
            is_difficult = starters > 10
            print(f"[DEBUG] Starters: {starters}, Difficult race: {is_difficult}")
            
            # Define pools by ranking position - always use hard protocol (difficult race logic)
            # Hard protocol: broader pools to include middle positions (applied to all races)
            pool1_idx = list(range(0, min(4, len(summary_order))))   # top 4
            pool2_idx = list(range(4, min(10, len(summary_order))))  # middle positions (4-9)
            pool3_idx = list(range(6, min(13, len(summary_order))))  # positions 6-12
            pool4_idx = list(range(12, len(summary_order))) if len(summary_order) > 12 else list(range(8, len(summary_order)))  # positions 13+ or 9+
            print(f"[DEBUG] Using HARD protocol pools (all races): pool1={pool1_idx}, pool2_middle={pool2_idx}, pool3={pool3_idx}")
            
            import itertools as _it
            
            if desired_size <= 0:
                desired_size = 5
            
            # Convert prog_only_horses and excl_horses to lists if needed
            prog_only = list(prog_only_horses) if prog_only_horses else []
            excl = list(excl_horses) if excl_horses else []
            print(f"[DEBUG] Prog Only: {prog_only}, Exclusives: {excl}")
            
            # Generate combos based on desired_size
            if desired_size == 1:
                for idx in pool1_idx:
                    combos.append((summary_order[idx],))
            
            elif desired_size == 2:
                for i, j in _it.combinations(pool1_idx, 2):
                    combos.append((summary_order[i], summary_order[j]))
            
            elif desired_size == 3:
                # Hard protocol: Pattern A - 1 from top4 + 1 from middle + 1 from top10 (favor Prog Only/Excl)
                for i in pool1_idx:
                    for j in pool2_idx:
                        for k in range(min(10, len(summary_order))):
                            if k not in (i, j):
                                combos.append((summary_order[i], summary_order[j], summary_order[k]))
                
                # Hard protocol: Pattern B - 2 from middle (includes Prog Only/Excl focus) + 1 from anywhere
                if pool2_idx:
                    for i, j in _it.combinations(pool2_idx, 2):
                        for k in range(len(summary_order)):
                            if k not in (i, j):
                                combos.append((summary_order[i], summary_order[j], summary_order[k]))
                
                # Hard protocol: Pattern C - Ensure at least one from Prog Only or Exclusives if they exist
                if prog_only or excl:
                    critical = prog_only + excl
                    # Find indices in summary_order
                    crit_idx = [summary_order.index(h) for h in critical if h in summary_order]
                    if crit_idx:
                        for crit in crit_idx:
                            # Pair critical with top horses
                            for i in range(min(6, len(summary_order))):
                                for j in range(min(8, len(summary_order))):
                                    if i != crit and j != crit and i != j:
                                        combos.append((summary_order[i], summary_order[j], summary_order[crit]))
            
            elif desired_size == 4:
                # Hard protocol: Pattern A - 1 from top4 + 1 from middle + 1 from pos6-12 + 1 from anywhere
                for i in pool1_idx:
                    for j in pool2_idx:
                        for k in pool3_idx:
                            if k not in (i, j):
                                for m in range(len(summary_order)):
                                    if m not in (i, j, k):
                                        combos.append((summary_order[i], summary_order[j], summary_order[k], summary_order[m]))
                
                # Hard protocol: Pattern B - 2 from middle + 1 from pos6-12 + 1 from critical/rest
                if pool2_idx and len(pool2_idx) >= 2:
                    for i, j in _it.combinations(pool2_idx, 2):
                        for k in pool3_idx:
                            if k not in (i, j):
                                for m in range(len(summary_order)):
                                    if m not in (i, j, k):
                                        combos.append((summary_order[i], summary_order[j], summary_order[k], summary_order[m]))
                
                # Hard protocol: Pattern C - Prioritize Prog Only/Exclusives in 4-horse combos
                if prog_only or excl:
                    critical = prog_only + excl
                    crit_idx = [summary_order.index(h) for h in critical if h in summary_order]
                    if crit_idx:
                        for crit in crit_idx:
                            # Build combo: 1 critical + 3 diverse others
                            for i in range(min(5, len(summary_order))):
                                for j in range(5, min(11, len(summary_order))):
                                    for k in range(len(summary_order)):
                                        if i != crit and j != crit and k != crit and i != j and i != k and j != k:
                                            combos.append((summary_order[i], summary_order[j], summary_order[k], summary_order[crit]))
            
            else:
                # 5+ horses - Hard protocol: broader selection including middle + critical horses
                for i, j in _it.combinations(pool1_idx, 2):
                    for k in pool2_idx:
                        if k not in (i, j):
                            for m in pool3_idx:
                                if m not in (i, j, k):
                                    for n in pool4_idx:
                                        if n not in (i, j, k, m):
                                            combo = [summary_order[i], summary_order[j], summary_order[k], summary_order[m], summary_order[n]]
                                            if desired_size > 5:
                                                used_idx = {i, j, k, m, n}
                                                remaining_idx = [idx for idx in range(len(summary_order)) if idx not in used_idx]
                                                for idx in remaining_idx:
                                                    if len(combo) >= desired_size:
                                                        break
                                                    combo.append(summary_order[idx])
                                            combos.append(tuple(combo))
                    # Easy race: original tight logic
                    # 2 from top 4, 1 from top 6, 1 from top 8, 1+ from rest
                    for i, j in _it.combinations(pool1_idx, 2):
                        for k in pool2_idx:
                            if k not in (i, j):
                                for m in pool3_idx:
                                    if m not in (i, j, k):
                                        for n in pool4_idx:
                                            if n not in (i, j, k, m):
                                                combo = [summary_order[i], summary_order[j], summary_order[k], summary_order[m], summary_order[n]]
                                                # if user requested more than 5, append further horses
                                                if desired_size > 5:
                                                    used_idx = {i, j, k, m, n}
                                                    remaining_idx = [idx for idx in range(len(summary_order)) if idx not in used_idx]
                                                    for idx in remaining_idx:
                                                        if len(combo) >= desired_size:
                                                            break
                                                        combo.append(summary_order[idx])
                                                combos.append(tuple(combo))
            
            # Filter combos: ensure base horses are present if base exists
            if base:
                # DEBUG: Log what's happening
                print(f"[DEBUG] Flash Tickets - Before filter: {len(combos)} combos, base={base}")
                print(f"[DEBUG] summary_order={summary_order}")
                print(f"[DEBUG] First 10 combos before filter: {combos[:10]}")
                
                combos = [c for c in combos if set(base).issubset(set(c))]
                
                print(f"[DEBUG] After filter: {len(combos)} combos")
                if combos:
                    print(f"[DEBUG] First 5 filtered combos: {combos[:5]}")

                # Deduplicate canonical combo sets (remove permutations)
                def _norm_combo(c):
                    try:
                        return tuple(sorted(c, key=lambda x: int(x) if str(x).isdigit() else x))
                    except Exception:
                        return tuple(sorted(c))

                seen = set()
                uniq = []
                for c in combos:
                    norm = _norm_combo(c)
                    if norm not in seen:
                        seen.add(norm)
                        # Preserve the original generator order for the combo itself
                        uniq.append(tuple(c))
                combos = uniq
                print(f"[DEBUG] After dedupe: {len(combos)} unique combos (original ordering preserved)")
                if combos:
                    print(f"[DEBUG] First 5 unique combos (original order): {combos[:5]}")

            # Final cleaning: remove combos that contain invalid placeholders like '0' or empty strings
            def _valid_entry(x):
                try:
                    s = str(x).strip()
                    if not s:
                        return False
                    if s.lower() in ('nan', 'none'):
                        return False
                    if s == '0':
                        return False
                    return True
                except Exception:
                    return False

            cleaned = []
            for c in combos:
                if not c or len(c) < 1:
                    continue
                # ensure combo has at least desired_size elements and all are valid
                if len(c) >= desired_size and all(_valid_entry(x) for x in c[:desired_size]):
                    cleaned.append(tuple(str(x).strip() for x in c[:desired_size]))
            return cleaned
        
        except Exception as e:
            print(f"[ERROR] Flash Tickets generation failed: {e}")
            return []
    
    def generate_combos_standard(self, base, associates, desired_size, max_combos):
        """
        Generate combinations using standard reduction system (fallback).
        
        Args:
            base: list of mandatory horse numbers
            associates: list of optional horse numbers
            desired_size: number of horses per combination
            max_combos: maximum number of combinations to generate
        
        Returns:
            list of tuples (combinations)
        """
        combos = []
        try:
            # Shuffle associates to eliminate bias towards small horse numbers
            # This ensures combinations don't systematically favor horses numbered 1,2,3...
            associates_shuffled = associates.copy()
            random.shuffle(associates_shuffled)
            
            # Use reducing_system to generate initial combos
            combos = reducing_system(base, associates_shuffled, desired_size)
            
            # If no combos and base is complete, return base as single combo
            if len(combos) == 0 and len(base) == desired_size:
                combos = [tuple(base)]
            
            # If too many combos, apply greedy reducer
            if len(combos) > max_combos:
                combos = greedy_reducer(base, associates, desired_size, max_combinations=max_combos)

            # Remove any combos containing invalid placeholders like '0' or empty strings
            def _valid_entry(x):
                try:
                    s = str(x).strip()
                    if not s:
                        return False
                    if s.lower() in ('nan', 'none'):
                        return False
                    if s == '0':
                        return False
                    return True
                except Exception:
                    return False

            cleaned = []
            for c in combos:
                if c and len(c) == desired_size and all(_valid_entry(x) for x in c):
                    cleaned.append(tuple(str(x).strip() for x in c))
            return cleaned
        
        except Exception as e:
            print(f"[ERROR] Standard generation failed: {e}")
            return []
    
    def generate_bets(self):
        """Generate reduced betting combinations based on UI controls and show results."""
        try:
            # =====================================================
            # STEP 1: BUILD BASE (mandatory horses)
            # =====================================================
            manual = self.manual_base_input.text().strip() if hasattr(self, 'manual_base_input') else ''
            mcount = int(self.mandatory_count_spin.value()) if hasattr(self, 'mandatory_count_spin') else 2
            
            if manual:
                parts = [p.strip() for p in manual.replace(';',',').split(',') if p.strip()]
                base = []
                for p in parts:
                    if p.endswith('.0'):
                        p = p[:-2]
                    if p.isdigit() and p != '0':
                        base.append(p)
            else:
                prog_text = self.prognosis_label.text().strip() if hasattr(self, 'prognosis_label') else ''
                prog_list = [p for p in prog_text.split() if p.isdigit()]

                # If default behaviour (2 mandatory) choose two numbers randomly
                # but always include at least one of the first two prognosis horses
                if mcount == 2 and len(prog_list) > 0:
                    # top two from prognosis (may be 1 element if len==1)
                    top2 = prog_list[:2]
                    # pick one from the top2 to guarantee inclusion
                    try:
                        first = random.choice(top2)
                    except Exception:
                        first = top2[0]

                    # pick second from remaining prognosis horses if possible
                    remaining = [p for p in prog_list if p != first]
                    if remaining:
                        second = random.choice(remaining)
                    else:
                        # fallback to top2 other element if exists
                        second = top2[0] if top2 and top2[0] != first else first

                    # ensure unique ordering with first as priority
                    if first == second:
                        base = [first]
                    else:
                        base = [first, second]
                else:
                    # default deterministic behaviour: take first mcount prognosis items
                    base = prog_list[:mcount]

            # =====================================================
            # STEP 2: BUILD ASSOCIATES (optional pool)
            # =====================================================
            source = self.assoc_source_combo.currentText() if hasattr(self, 'assoc_source_combo') else 'Summary'
            associates = []
            comp = self.current_composite_df if hasattr(self, 'current_composite_df') else None
            
            if source == 'Summary' and comp is not None and not comp.empty:
                associates = compute_summary_horses(comp)
            elif source == 'Prognosis+Summary':
                associates = []
                if prog_text:
                    associates.extend([p for p in prog_text.split() if p.isdigit()])
                if comp is not None and not comp.empty:
                    for p in compute_summary_horses(comp):
                        if p not in associates:
                            associates.append(p)
            else:
                # All horses
                if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None:
                    df = self.last_filtered_df
                    for col in ['N°','N','Numero']:
                        if col in df.columns:
                            for raw in df[col].tolist():
                                if raw is None:
                                    continue
                                v = str(raw).strip()
                                if v.endswith('.0'):
                                    v = v[:-2]
                                if not v or v.lower() in ('nan','none'):
                                    continue
                                if not v.isdigit():
                                    v_clean = ''.join(ch for ch in v if ch.isdigit())
                                    if not v_clean:
                                        continue
                                    v = v_clean
                                if v == '0':
                                    continue
                                if v not in associates:
                                    associates.append(v)

            # =====================================================
            # STEP 3: AUTO-INCLUDE & PRIORITIZE (Exclusive/Prog Only/Trending/Favorable/Outsiders/Underperforming)
            # =====================================================
            try:
                import re
                import collections

                prog_set = set(re.findall(r"\d+", prog_text)) if prog_text else set()
                summary_text_label = self.summary_label.text().strip() if hasattr(self, 'summary_label') else ''
                summary_set = set(re.findall(r"\d+", summary_text_label))
                excl_text = self.exclusive_label.text().strip() if hasattr(self, 'exclusive_label') else ''
                excl_set = set(re.findall(r"\d+", excl_text))
                prog_only_text = self.prognosis_only_label.text().strip() if hasattr(self, 'prognosis_only_label') else ''
                prog_only_set = set(re.findall(r"\d+", prog_only_text))
                trending_text = self.trending_label.text().strip() if hasattr(self, 'trending_label') else ''
                trending_set = set(re.findall(r"\d+", trending_text))
                
                # Extract from new sources: favorable cordes, outsiders, underperforming favorites
                favorable_cordes_text = self.favorable_cordes_label.text().strip() if hasattr(self, 'favorable_cordes_label') else ''
                favorable_cordes_set = set(re.findall(r"\d+", favorable_cordes_text))
                
                # Extract outsiders from tables
                outsiders_set = set()
                if hasattr(self, 'outsiders_divergence_table'):
                    for r in range(self.outsiders_divergence_table.rowCount()):
                        if self.outsiders_divergence_table.item(r, 0):
                            horse_num = self.outsiders_divergence_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                outsiders_set.add(horse_num)
                if hasattr(self, 'outsiders_consistency_table'):
                    for r in range(self.outsiders_consistency_table.rowCount()):
                        if self.outsiders_consistency_table.item(r, 0):
                            horse_num = self.outsiders_consistency_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                outsiders_set.add(horse_num)
                
                # Extract underperforming favorites from table
                underperforming_set = set()
                if hasattr(self, 'underperforming_favorites_table'):
                    for r in range(self.underperforming_favorites_table.rowCount()):
                        if self.underperforming_favorites_table.item(r, 0):
                            horse_num = self.underperforming_favorites_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                underperforming_set.add(horse_num)
                
                # Extract horses with weight advantages
                weight_stable_set = set()
                if hasattr(self, 'weight_stab_table'):
                    for r in range(self.weight_stab_table.rowCount()):
                        if self.weight_stab_table.item(r, 0):
                            horse_num = self.weight_stab_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                weight_stable_set.add(horse_num)
                
                # Extract horses carrying lighter weight
                weight_light_set = set()
                if hasattr(self, 'light_weight_table'):
                    for r in range(self.light_weight_table.rowCount()):
                        if self.light_weight_table.item(r, 0):
                            horse_num = self.light_weight_table.item(r, 0).text().strip()
                            if horse_num.isdigit():
                                weight_light_set.add(horse_num)

                # Count presence across 10 sources
                sources = [prog_set, summary_set, excl_set, prog_only_set, trending_set, 
                          favorable_cordes_set, outsiders_set, underperforming_set,
                          weight_stable_set, weight_light_set]
                counter = collections.Counter()
                for s in sources:
                    for h in s:
                        counter[h] += 1

                # Priority: horses in >=4 sources (was >=3 for 5 sources, was >=4 for 8 sources, now >=4 for 10 sources)
                priority = [h for h, c in counter.items() if c >= 4]

                def _num_or_str(x):
                    try:
                        return int(x)
                    except Exception:
                        return x

                # Sort by frequency + summary position
                summary_order = None
                if comp is not None and not comp.empty:
                    for col in ['N°','N','Num','Numero']:
                        if col in comp.columns:
                            summary_order = [str(x) for x in comp[col].tolist()]
                            break
                
                if summary_order:
                    pos = {v: i for i, v in enumerate(summary_order)}
                    priority.sort(key=lambda x: (-counter.get(x, 0), pos.get(x, 9999), _num_or_str(x)))
                else:
                    priority.sort(key=lambda x: (-counter.get(x, 0), _num_or_str(x)))

                # Deduplicate priority, remove base
                seen_p = set()
                prio_list = []
                for p in priority:
                    if p in seen_p or p in base:
                        continue
                    seen_p.add(p)
                    prio_list.append(p)

                # Ensure Exclusive + Prog Only always in priority
                for h in list(excl_set.union(prog_only_set)):
                    if h and h not in prio_list and h not in base:
                        prio_list.append(h)

                # Prepend priority to associates
                associates = [a for a in associates if a not in prio_list]
                associates = prio_list + associates
            except Exception as e:
                print(f"[WARN] Priority extraction failed: {e}")

            # Remove base from associates
            associates = [a for a in associates if a not in base]

            # =====================================================
            # STEP 4: DETERMINE DESIRED SIZE
            # =====================================================
            bet_size = int(self.bet_size_spin.value()) if hasattr(self, 'bet_size_spin') else 5
            max_combos = int(self.max_combos_spin.value()) if hasattr(self, 'max_combos_spin') else 100
            desired_size = bet_size

            # Ensure desired_size >= base size
            if isinstance(base, list) and len(base) > 0:
                desired_size = max(desired_size, len(base))

            # =====================================================
            # STEP 5: CHOOSE & EXECUTE GENERATION METHOD
            # =====================================================
            combos = []
            use_flash = getattr(self, 'flash_auto_checkbox', None) and self.flash_auto_checkbox.isChecked()

            # Primary: standard method
            combos = self.generate_combos_standard(base, associates, desired_size, max_combos)
            
            # Fallback: Flash Tickets method if enabled and composite data available
            if not combos and use_flash and comp is not None and not comp.empty:
                combos = self.generate_combos_flash_tickets(base, associates, desired_size, comp, 
                                                           prog_only_horses=prog_only_set, 
                                                           excl_horses=excl_set)

            # =====================================================
            # STEP 6: DEDUPLICATE & DISPLAY
            # =====================================================
            try:
                seen = set()
                unique = []
                def _key_sort(x):
                    try:
                        return int(x) if str(x).isdigit() else str(x)
                    except Exception:
                        return str(x)
                for c in combos:
                    canon = tuple(sorted(c, key=_key_sort))
                    if canon not in seen:
                        seen.add(canon)
                        # Preserve the original ordering of the combo
                        unique.append(tuple(c))
                combos = unique
            except Exception:
                pass

            # Display in bets_table (if it exists)
            display_size = desired_size if desired_size > 0 else bet_size
            if hasattr(self, 'bets_table') and self.bets_table:
                self.bets_table.clear()
                self.bets_table.setColumnCount(display_size)
                self.bets_table.setRowCount(len(combos))
                headers = [f"H{i+1}" for i in range(display_size)]
                self.bets_table.setHorizontalHeaderLabels(headers)
                for i, combo in enumerate(combos):
                    for j in range(display_size):
                        val = combo[j] if j < len(combo) else ''
                        item = QTableWidgetItem(str(val))
                        self.bets_table.setItem(i, j, item)
                print(f"[DEBUG] generate_bets: Populated bets_table with {len(combos)} rows x {display_size} columns")
            else:
                print(f"[DEBUG] generate_bets: No bets_table available")

            # Update method indicator
            try:
                method = f"[WARNING]? Flash Tickets ({display_size} horses)" if use_flash else f"Standard ({display_size} horses)"
                self.method_label.setText(f"Method: {method} e? {len(combos)} combinations")
                if use_flash:
                    self.method_label.setStyleSheet("color: #f39c12; font-weight: bold; font-size: 11px; padding: 4px;")
                else:
                    self.method_label.setStyleSheet("color: #95a5a6; font-size: 11px; padding: 4px;")
            except Exception:
                if hasattr(self, 'method_label') and self.method_label:
                    self.method_label.setText(f"Generated {len(combos)} combinations")

            if hasattr(self, 'status_label') and self.status_label:
                self.status_label.setText(f"Generated {len(combos)} combinations")
            
        except Exception as e:
            print(f"Error in generate_bets: {e}")
            if hasattr(self, 'method_label') and self.method_label:
                self.method_label.setText(f"Error: {str(e)[:50]}")
                self.method_label.setStyleSheet("color: #e74c3c; font-weight: bold; font-size: 11px; padding: 4px;")
            if hasattr(self, 'status_label') and self.status_label:
                self.status_label.setText("Error generating bets")

    def save_bets_and_metadata(self):
        """Save generated combinations and current prognosis/summary/exclusive lists to a beautifully styled PDF file."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER, TA_LEFT
            
            # collect combos
            rows = []
            for r in range(self.bets_table.rowCount()):
                vals = []
                for c in range(self.bets_table.columnCount()):
                    it = self.bets_table.item(r, c)
                    vals.append(it.text() if it else '')
                rows.append(vals)

            prognosis_text = self.prognosis_label.text().strip() if hasattr(self, 'prognosis_label') else ''
            summary_text = self.summary_label.text().strip() if hasattr(self, 'summary_label') else ''
            exclusive_text = self.exclusive_label.text().strip() if hasattr(self, 'exclusive_label') else ''
            trending_text = self.trending_label.text().strip() if hasattr(self, 'trending_label') else ''
            favorable_cordes_text = self.favorable_cordes_label.text().strip() if hasattr(self, 'favorable_cordes_label') else ''
            prognosis_only_text = self.prognosis_only_label.text().strip() if hasattr(self, 'prognosis_only_label') else ''
            summary_prognosis_text = self.summary_prognosis_label.text().strip() if hasattr(self, 'summary_prognosis_label') else ''

            # Get race metadata from filtered data
            prize_name = ''
            ref_course = ''
            race_date = ''
            hippodrome = ''
            ext_conditions = ''
            
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                prize_name = str(df['PRIZE_NAME'].iloc[0]) if 'PRIZE_NAME' in df.columns else ''
                ref_course = str(df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in df.columns else ''
                race_date = str(df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in df.columns else ''
                hippodrome = str(df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in df.columns else ''
                ext_conditions = str(df['DESCRIPTIF'].iloc[0]) if 'DESCRIPTIF' in df.columns else ''

            # Generate dynamic filename for analysis PDF
            default_filename = "analysis.pdf"
            if race_date and ref_course:
                date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                default_filename = f"{date_clean}_{ref_course}_analysis.pdf"
            elif race_date:
                date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                default_filename = f"{date_clean}_analysis.pdf"
            
            # Save to Downloads folder
            downloads_folder = get_downloads_folder()
            path = str(downloads_folder / default_filename)

            # Create PDF document for ANALYSIS ONLY
            doc = SimpleDocTemplate(path, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
            story = []
            styles = getSampleStyleSheet()
            
            # Custom styles
            title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=14, spaceAfter=30, alignment=TA_CENTER)
            header_style = ParagraphStyle('CustomHeader', parent=styles['Heading2'], fontSize=11, spaceAfter=12, textColor=colors.darkblue)
            italic_style = ParagraphStyle('CustomItalic', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Oblique', spaceAfter=12, textColor=colors.grey)
            
            # Header section with race metadata
            header_text = f"{prize_name}" if prize_name else "Course Hippique"
            story.append(Paragraph(header_text, title_style))
            
            metadata_text = f"<b>Reference:</b> {ref_course} | <b>Date:</b> {race_date} | <b>Hippodrome:</b> {hippodrome}"
            story.append(Paragraph(metadata_text, styles['Normal']))
            story.append(Spacer(1, 12))
            
            # External conditions in italics
            if ext_conditions:
                story.append(Paragraph(f"<i>{ext_conditions}</i>", italic_style))
            
            # Horses table with composite scores
            if hasattr(self, 'current_composite_df') and self.current_composite_df is not None and not self.current_composite_df.empty:
                story.append(Paragraph("Classement des Chevaux (Score Composite)", header_style))
                
                # Sort by composite score descending
                df_sorted = self.current_composite_df.sort_values('Composite', ascending=False)
                
                # Create table data
                table_data = [['Pos.', 'N?', 'Cheval', 'COTE', 'Score Composite']]
                for i, (_, row) in enumerate(df_sorted.iterrows(), 1):
                    # Handle COTE/Cote case variations
                    cote_val = row.get('COTE', row.get('Cote', ''))
                    table_data.append([
                        str(i),
                        str(row.get('N?', '')),
                        str(row.get('Cheval', '')),
                        str(cote_val),
                        f"{row.get('Composite', 0):.2f}"
                    ])
                
                # Create and style table
                table = Table(table_data, colWidths=[0.8*inch, 0.8*inch, 2.5*inch, 1*inch, 1.2*inch])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
                ]))
                story.append(table)
                story.append(Spacer(1, 20))
            
            # Analysis sections
            story.append(Paragraph("Analyse", header_style))
            
            # Custom style for section content (fontsize 9)
            section_style = ParagraphStyle('SectionStyle', parent=styles['Normal'], fontSize=9, spaceAfter=4)
            
            sections = [
                ("Notre Prono Flash", prognosis_text),
                ("Synthese (Composite)", summary_text),
                ("[TARGET] Intersection Synthese+Prono Flash", summary_prognosis_text),
                ("Cordes Favorables", favorable_cordes_text),
                ("Dans le Prono Uniquement", prognosis_only_text),
                ("Exclusif (Synthese seulement)", exclusive_text),
                ("Populaires (Baisse de Cotes)", trending_text)
            ]
            
            for section_title, section_text in sections:
                # Always show section title
                if section_text:
                    story.append(Paragraph(f"<b>{section_title}:</b> {section_text}", section_style))
                else:
                    story.append(Paragraph(f"<b>{section_title}:</b> <i>(aucun resultat)</i>", section_style))
                story.append(Spacer(1, 2))
            
            # Build ANALYSIS PDF (no combinations)
            doc.build(story)
            
            # Create COMBINATIONS PDF SEPARATELY if combos exist
            if rows:
                # Generate combinations filename: race_id + race_date + combinations
                combo_filename = "combinations.pdf"
                if race_date and ref_course:
                    date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                    combo_filename = f"{date_clean}_{ref_course}_combinations.pdf"
                elif race_date:
                    date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                    combo_filename = f"{date_clean}_combinations.pdf"
                
                # Construct full path for combinations PDF (same directory as analysis PDF)
                import os
                combo_dir = os.path.dirname(path)
                combo_path = os.path.join(combo_dir, combo_filename)
                
                # Create combinations document
                doc_combos = SimpleDocTemplate(combo_path, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
                story_combos = []
                
                # Add header
                story_combos.append(Paragraph(f"Combinaisons Generees - {ref_course}", title_style))
                story_combos.append(Paragraph(f"Date: {race_date}", styles['Normal']))
                story_combos.append(Spacer(1, 20))
                
                # Create combinations table
                headers = [f"H{i+1}" for i in range(len(rows[0]))]
                combo_data = [headers] + rows
                
                combo_table = Table(combo_data)
                combo_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.lightcyan),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9)
                ]))
                story_combos.append(combo_table)
                
                # Build combinations PDF
                doc_combos.build(story_combos)
                
                save_msg = f"Analyse enregistree dans:\n{path}\n\nCombinaisons enregistrees dans:\n{combo_path}\n\nVoulez-vous imprimer l'analyse ?"
            else:
                save_msg = f"Analyse enregistree dans:\n{path}\n\nVoulez-vous imprimer l'analyse ?"
            
            # Offer to print ONLY the analysis document
            reply = QMessageBox.question(
                self,
                "Enregistre",
                save_msg,
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                try:
                    import subprocess
                    import platform
                    import os
                    
                    # Print ONLY the analysis PDF using proper PDF printing methods
                    if platform.system() == 'Windows':
                        # Use Windows native PDF printing via default application
                        os.startfile(path, 'print')
                    elif platform.system() == 'Darwin':
                        # macOS - use default print dialog
                        subprocess.run(['open', '-a', 'Preview', path], check=False)
                        # Alternative: subprocess.run(['lp', path], check=False)
                    else:
                        # Linux - use system print command
                        subprocess.run(['lp', path], check=False)
                    
                    QMessageBox.information(self, "Impression", "Le document analyse a ete envoye e l'imprimante par defaut")
                except Exception as e:
                    print(f"Error printing document: {e}")
                    QMessageBox.warning(self, "Erreur d'Impression", f"Impossible d'imprimer le document : {e}")
            
        except ImportError:
            QMessageBox.critical(self, "Erreur", "Le package ReportLab est requis pour l'exportation au format PDF. Veuillez l'installer avec la commandee: pip install reportlab")
        except Exception as e:
            print(f"Error saving bets: {e}")
            QMessageBox.critical(self, "Erreur", f"Impossible d'enregistrer le fichier PDFe: {e}")

    def save_analysis_as_pdf(self):
        """Export race analysis to a PDF document without combinations."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER, TA_LEFT
            
            # Get race metadata
            prize_name = ''
            ref_course = ''
            race_date = ''
            hippodrome = ''
            ext_conditions = ''
            
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                prize_name = str(df['PRIZE_NAME'].iloc[0]) if 'PRIZE_NAME' in df.columns else ''
                ref_course = str(df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in df.columns else ''
                race_date = str(df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in df.columns else ''
                hippodrome = str(df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in df.columns else ''
                ext_conditions = str(df['DESCRIPTIF'].iloc[0]) if 'DESCRIPTIF' in df.columns else ''
            
            # Generate filename
            default_filename = "analysis.pdf"
            if race_date and ref_course:
                date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                default_filename = f"{date_clean}_{ref_course}_analysis.pdf"
            elif race_date:
                date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                default_filename = f"{date_clean}_analysis.pdf"
            
            # Save to Downloads folder
            downloads_folder = get_downloads_folder()
            path = str(downloads_folder / default_filename)
            
            # Create PDF document
            doc = SimpleDocTemplate(path, pagesize=A4, rightMargin=36, leftMargin=36, topMargin=60, bottomMargin=30)
            story = []
            styles = getSampleStyleSheet()
            
            # Get header and footer
            header_text_racex, footer_text_racex, header_style, footer_style, icon_path = get_pdf_header_footer()
            
            # Add RaceX header with icon
            if icon_path:
                from reportlab.platypus import Image
                img = Image(icon_path, width=0.5*inch, height=0.5*inch)
                story.append(img)
            story.append(Paragraph(header_text_racex, header_style))
            story.append(Spacer(1, 6))
            
            # Custom styles
            title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=14, spaceAfter=12, alignment=TA_CENTER)
            header_style_custom = ParagraphStyle('CustomHeader', parent=styles['Heading2'], fontSize=10, spaceAfter=10, textColor=colors.darkblue)
            italic_style = ParagraphStyle('CustomItalic', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Oblique', spaceAfter=10, textColor=colors.grey)
            
            # Title
            header_text = f"{prize_name}" if prize_name else "Analyse de Course"
            story.append(Paragraph(header_text, title_style))
            
            # Metadata
            metadata_text = f"<b>Reference:</b> {ref_course} | <b>Date:</b> {race_date} | <b>Hippodrome:</b> {hippodrome}"
            story.append(Paragraph(metadata_text, styles['Normal']))
            story.append(Spacer(1, 6))
            
            # External conditions
            if ext_conditions:
                story.append(Paragraph(f"<i>{ext_conditions}</i>", italic_style))
                story.append(Spacer(1, 8))
            
            # Horses table with composite scores
            if hasattr(self, 'current_composite_df') and self.current_composite_df is not None and not self.current_composite_df.empty:
                story.append(Paragraph("Classement des Chevaux (Score Composite)", header_style_custom))
                
                # Sort by composite score descending
                df_sorted = self.current_composite_df.sort_values('Composite', ascending=False)
                
                # Create table data
                table_data = [['Pos.', 'N°', 'Cheval', 'COTE', 'Score Composite']]
                for i, (_, row) in enumerate(df_sorted.iterrows(), 1):
                    # Handle COTE/Cote case variations
                    cote_val = row.get('COTE', row.get('Cote', ''))
                    table_data.append([
                        str(i),
                        str(row.get('N°', '')),
                        str(row.get('Cheval', '')),
                        str(cote_val),
                        f"{row.get('Composite', 0):.2f}"
                    ])
                
                # Create and style table
                table = Table(table_data, colWidths=[0.6*inch, 0.6*inch, 2.0*inch, 0.8*inch, 0.9*inch])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
                ]))
                story.append(table)
                story.append(Spacer(1, 12))
            
            # Analysis sections
            story.append(Paragraph("Analyse", header_style_custom))
            
            # Custom style for section content (fontsize 9)
            section_style = ParagraphStyle('SectionStyle', parent=styles['Normal'], fontSize=9, spaceAfter=4)
            
            prognosis_text = self.prognosis_label.text().strip() if hasattr(self, 'prognosis_label') else ''
            summary_text = self.summary_label.text().strip() if hasattr(self, 'summary_label') else ''
            exclusive_text = self.exclusive_label.text().strip() if hasattr(self, 'exclusive_label') else ''
            trending_text = self.trending_label.text().strip() if hasattr(self, 'trending_label') else ''
            favorable_cordes_text = self.favorable_cordes_label.text().strip() if hasattr(self, 'favorable_cordes_label') else ''
            prognosis_only_text = self.prognosis_only_label.text().strip() if hasattr(self, 'prognosis_only_label') else ''
            summary_prognosis_text = self.summary_prognosis_label.text().strip() if hasattr(self, 'summary_prognosis_label') else ''
            
            sections = [
                ("Prono Flash", prognosis_text),
                ("Synthese (Composite)", summary_text),
                ("[TARGET] Intersection Synthese+Prono Flash", summary_prognosis_text),
                ("Cordes Favorables", favorable_cordes_text),
                ("Dans le Prono Uniquement", prognosis_only_text),
                ("Exclusif (Synthese seulement)", exclusive_text),
                ("Populaires (Baisse de Cotes)", trending_text)
            ]
            
            for section_title, section_text in sections:
                if section_text:
                    story.append(Paragraph(f"<b>{section_title}:</b> {section_text}", section_style))
                else:
                    story.append(Paragraph(f"<b>{section_title}:</b> <i>(aucun resultat)</i>", section_style))
                story.append(Spacer(1, 2))
            
            # Add RaceX footer
            story.append(Spacer(1, 24))
            story.append(Paragraph(footer_text_racex, footer_style))
            
            # Build PDF
            doc.build(story)
            
            # Offer to print the analysis now
            save_msg = f"L'analyse a ete exportee avec succes : {path}\n\nVoulez-vous l'imprimer maintenant ?"
            reply = QMessageBox.question(self, "Export PDF", save_msg, QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                try:
                    import subprocess
                    import platform
                    import os
                    # Print the PDF using platform-appropriate method
                    if platform.system() == 'Windows':
                        os.startfile(path, 'print')
                    elif platform.system() == 'Darwin':
                        subprocess.run(['open', '-a', 'Preview', path], check=False)
                    else:
                        subprocess.run(['lp', path], check=False)
                    QMessageBox.information(self, "Impression", "Le document d'analyse a ete envoye a l'imprimante par defaut")
                except Exception as e:
                    print(f"Error printing document: {e}")
                    QMessageBox.warning(self, "Erreur d'Impression", f"Impossible d'imprimer le document : {e}")
            
        except ImportError:
            QMessageBox.critical(self, "Erreur", "Le package ReportLab est requis pour l'exportation au format PDF. Veuillez l'installer avec la commande: pip install reportlab")
        except Exception as e:
            print(f"Error saving analysis as PDF: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Erreur", f"Impossible d'enregistrer le fichier PDF: {e}")

    def print_analysis(self):
        """Print the race analysis using the default printer."""
        try:
            import tempfile
            import subprocess
            import os
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER, TA_LEFT
            
            # Get race metadata
            prize_name = ''
            ref_course = ''
            race_date = ''
            hippodrome = ''
            ext_conditions = ''
            
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                prize_name = str(df['PRIZE_NAME'].iloc[0]) if 'PRIZE_NAME' in df.columns else ''
                ref_course = str(df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in df.columns else ''
                race_date = str(df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in df.columns else ''
                hippodrome = str(df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in df.columns else ''
                ext_conditions = str(df['DESCRIPTIF'].iloc[0]) if 'DESCRIPTIF' in df.columns else ''
            
            # Create temporary PDF file
            temp_fd, temp_path = tempfile.mkstemp(suffix='.pdf', prefix='analysis_')
            os.close(temp_fd)
            
            # Create PDF document
            doc = SimpleDocTemplate(temp_path, pagesize=A4, rightMargin=36, leftMargin=36, topMargin=50, bottomMargin=30)
            story = []
            styles = getSampleStyleSheet()
            
            # Get header and footer
            header_text_racex, footer_text_racex, header_style, footer_style, icon_path = get_pdf_header_footer()
            
            # Add RaceX header with icon
            if icon_path:
                from reportlab.platypus import Image
                img = Image(icon_path, width=0.5*inch, height=0.5*inch)
                story.append(img)
            story.append(Paragraph(header_text_racex, header_style))
            story.append(Spacer(1, 6))
            
            # Custom styles
            title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=14, spaceAfter=12, alignment=TA_CENTER)
            header_style_custom = ParagraphStyle('CustomHeader', parent=styles['Heading2'], fontSize=10, spaceAfter=10, textColor=colors.darkblue)
            italic_style = ParagraphStyle('CustomItalic', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Oblique', spaceAfter=10, textColor=colors.grey)
            
            # Title
            header_text = f"{prize_name}" if prize_name else "Analyse de Course"
            story.append(Paragraph(header_text, title_style))
            
            # Metadata
            metadata_text = f"<b>Reference:</b> {ref_course} | <b>Date:</b> {race_date} | <b>Hippodrome:</b> {hippodrome}"
            story.append(Paragraph(metadata_text, styles['Normal']))
            story.append(Spacer(1, 12))
            
            # External conditions
            if ext_conditions:
                story.append(Paragraph(f"<i>{ext_conditions}</i>", italic_style))
                story.append(Spacer(1, 12))
            
            # Horses table with composite scores
            if hasattr(self, 'current_composite_df') and self.current_composite_df is not None and not self.current_composite_df.empty:
                story.append(Paragraph("Classement des Chevaux (Score Composite)", header_style_custom))
                
                # Sort by composite score descending
                df_sorted = self.current_composite_df.sort_values('Composite', ascending=False)
                
                # Create table data
                table_data = [['Pos.', 'N°', 'Cheval', 'COTE', 'Score Composite']]
                for i, (_, row) in enumerate(df_sorted.iterrows(), 1):
                    cote_val = row.get('COTE', row.get('Cote', ''))
                    table_data.append([
                        str(i),
                        str(row.get('N°', '')),
                        str(row.get('Cheval', '')),
                        str(cote_val),
                        f"{row.get('Composite', 0):.2f}"
                    ])
                
                # Create and style table
                table = Table(table_data, colWidths=[0.6*inch, 0.6*inch, 2.0*inch, 0.8*inch, 0.9*inch])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
                ]))
                story.append(table)
                story.append(Spacer(1, 12))
            
            # Analysis sections
            story.append(Paragraph("Analyse", header_style_custom))
            
            # Custom style for section content (fontsize 9)
            section_style = ParagraphStyle('SectionStyle', parent=styles['Normal'], fontSize=9, spaceAfter=4)
            
            prognosis_text = self.prognosis_label.text().strip() if hasattr(self, 'prognosis_label') else ''
            summary_text = self.summary_label.text().strip() if hasattr(self, 'summary_label') else ''
            exclusive_text = self.exclusive_label.text().strip() if hasattr(self, 'exclusive_label') else ''
            trending_text = self.trending_label.text().strip() if hasattr(self, 'trending_label') else ''
            favorable_cordes_text = self.favorable_cordes_label.text().strip() if hasattr(self, 'favorable_cordes_label') else ''
            prognosis_only_text = self.prognosis_only_label.text().strip() if hasattr(self, 'prognosis_only_label') else ''
            summary_prognosis_text = self.summary_prognosis_label.text().strip() if hasattr(self, 'summary_prognosis_label') else ''
            
            sections = [
                ("Notre Prono Flash", prognosis_text),
                ("Synthese (Composite)", summary_text),
                ("Intersection Synthese+Prono Flash", summary_prognosis_text),
                ("Cordes Favorables", favorable_cordes_text),
                ("Dans le Prono Uniquement", prognosis_only_text),
                ("Exclusif (Synthese seulement)", exclusive_text),
                ("Populaires (Baisse de Cotes)", trending_text)
            ]
            
            for section_title, section_text in sections:
                if section_text:
                    story.append(Paragraph(f"<b>{section_title}:</b> {section_text}", section_style))
                else:
                    story.append(Paragraph(f"<b>{section_title}:</b> <i>(aucun resultat)</i>", section_style))
                story.append(Spacer(1, 2))
            
            # Add footer
            story.append(Spacer(1, 12))
            story.append(Paragraph(footer_text_racex, footer_style))
            
            # Build PDF
            doc.build(story)
            
            # Print the PDF
            try:
                if sys.platform == 'win32':
                    # Windows - use os.startfile with print verb (more reliable for PDFs)
                    os.startfile(temp_path, "print")
                elif sys.platform == 'darwin':
                    # macOS - use lp command
                    subprocess.run(['lp', temp_path], check=False)
                else:
                    # Linux - use lp command
                    subprocess.run(['lp', temp_path], check=False)
                
                QMessageBox.information(self, "Impression", "L'analyse a ete envoyee a l'imprimante par defaut")
            except Exception as e:
                print(f"Error printing: {e}")
                QMessageBox.warning(self, "Erreur d'Impression", f"Impossible d'imprimer le document : {e}")
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass
            
        except ImportError:
            QMessageBox.critical(self, "Erreur", "Le package ReportLab est requis pour l'impression. Veuillez l'installer avec la commande: pip install reportlab")
        except Exception as e:
            print(f"Error printing analysis: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Erreur", f"Impossible d'imprimer l'analyse : {e}")

    def save_combinations_only(self):
        """Save generated combinations to a separate PDF document."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER
            
            # Collect combos from bets_table
            rows = []
            for r in range(self.bets_table.rowCount()):
                vals = []
                for c in range(self.bets_table.columnCount()):
                    it = self.bets_table.item(r, c)
                    vals.append(it.text() if it else '')
                rows.append(vals)
            
            # Check if there are combinations to export
            if not rows:
                QMessageBox.warning(self, "Aucune Combinaison", "Veuillez d'abord generer des combinaisons avant d'exporter")
                return
            
            # Get race metadata from filtered data
            ref_course = ''
            race_date = ''
            
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                ref_course = str(df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in df.columns else ''
                race_date = str(df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in df.columns else ''
            
            # Generate filename for combinations PDF
            default_filename = "combinations.pdf"
            if race_date and ref_course:
                date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                default_filename = f"{date_clean}_{ref_course}_combinations.pdf"
            elif race_date:
                date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
                default_filename = f"{date_clean}_combinations.pdf"
            
            # Save to Downloads folder
            downloads_folder = get_downloads_folder()
            path = str(downloads_folder / default_filename)
            
            # Create PDF document
            doc = SimpleDocTemplate(path, pagesize=A4, rightMargin=36, leftMargin=36, topMargin=60, bottomMargin=30)
            story = []
            styles = getSampleStyleSheet()
            
            # Get header and footer
            header_text_racex, footer_text_racex, header_style, footer_style, icon_path = get_pdf_header_footer()
            
            # Add RaceX header with icon
            if icon_path:
                from reportlab.platypus import Image
                img = Image(icon_path, width=0.5*inch, height=0.5*inch)
                story.append(img)
            story.append(Paragraph(header_text_racex, header_style))
            story.append(Spacer(1, 6))
            
            # Custom styles
            title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=14, spaceAfter=15, alignment=TA_CENTER)
            header_style_custom = ParagraphStyle('CustomHeader', parent=styles['Heading2'], fontSize=10, spaceAfter=10, textColor=colors.darkblue)
            
            # Header with race info
            header_text = f"Combinaisons Generees - {ref_course}"
            story.append(Paragraph(header_text, title_style))
            story.append(Paragraph(f"Date: {race_date}", styles['Normal']))
            story.append(Spacer(1, 12))
            
            # Get method info if available
            method_text = ''
            if hasattr(self, 'method_label'):
                method_text = self.method_label.text().strip()
            
            if method_text:
                story.append(Paragraph(f"<b>Methode:</b> {method_text}", styles['Normal']))
                story.append(Spacer(1, 12))
            
            # Create combinations table in 3-column layout
            num_combos = len(rows)
            combos_per_col = (num_combos + 2) // 3  # Distribute combinations across 3 columns
            
            # Build table data for 3-column layout
            combo_data = []
            
            # Create header row with column labels
            header_cols = ["Combinaisons (1)", "Combinaisons (2)", "Combinaisons (3)"]
            combo_data.append(header_cols)
            
            # Fill rows with combinations
            for row_idx in range(combos_per_col):
                row_items = []
                
                # Column 1
                if row_idx < len(rows):
                    col1_text = " - ".join([str(x) for x in rows[row_idx] if x])
                    row_items.append(col1_text)
                else:
                    row_items.append("")
                
                # Column 2
                if row_idx + combos_per_col < len(rows):
                    col2_text = " - ".join([str(x) for x in rows[row_idx + combos_per_col] if x])
                    row_items.append(col2_text)
                else:
                    row_items.append("")
                
                # Column 3
                if row_idx + 2 * combos_per_col < len(rows):
                    col3_text = " - ".join([str(x) for x in rows[row_idx + 2 * combos_per_col] if x])
                    row_items.append(col3_text)
                else:
                    row_items.append("")
                
                combo_data.append(row_items)
            
            combo_table = Table(combo_data, colWidths=[1.8*inch, 1.8*inch, 1.8*inch])
            combo_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightcyan),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
                ('TOPPADDING', (0, 1), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 8)
            ]))
            story.append(combo_table)
            
            # Add summary
            story.append(Spacer(1, 12))
            story.append(Paragraph(f"<b>Total de combinaisons:</b> {num_combos}", styles['Normal']))
            
            # Add RaceX footer
            story.append(Spacer(1, 24))
            story.append(Paragraph(footer_text_racex, footer_style))
            
            # Build PDF
            doc.build(story)
            
            QMessageBox.information(self, "Enregistre", f"Combinaisons enregistrees dans:\n{path}")
            
        except ImportError:
            QMessageBox.critical(self, "Erreur", "Le package ReportLab est requis pour l'exportation au format PDF. Veuillez l'installer avec la commande : pip install reportlab")
        except Exception as e:
            print(f"Error saving combinations: {e}")
            QMessageBox.critical(self, "Erreur", f"Impossible d'enregistrer les combinaisons : {e}")

    def save_analysis_as_png(self):
        """Save analysis data as PNG with compact formatted table, matching PDF export structure."""
        self.statusBar().showMessage("Exporting PNG...")
        QApplication.processEvents()
        
        try:
            import matplotlib.pyplot as plt
            from datetime import datetime
            
            # Get race metadata
            prize_name = ''
            ref_course = ''
            race_date = ''
            hippodrome = ''
            
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                df = self.last_filtered_df
                prize_name = str(df['PRIZE_NAME'].iloc[0]) if 'PRIZE_NAME' in df.columns else ''
                ref_course = str(df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in df.columns else ''
                race_date = str(df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in df.columns else ''
                hippodrome = str(df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in df.columns else ''
            
            # Get analysis text
            prognosis_text = self.prognosis_label.text().strip() if hasattr(self, 'prognosis_label') else ''
            summary_text = self.summary_label.text().strip() if hasattr(self, 'summary_label') else ''
            exclusive_text = self.exclusive_label.text().strip() if hasattr(self, 'exclusive_label') else ''
            trending_text = self.trending_label.text().strip() if hasattr(self, 'trending_label') else ''
            favorable_cordes_text = self.favorable_cordes_label.text().strip() if hasattr(self, 'favorable_cordes_label') else ''
            prognosis_only_text = self.prognosis_only_label.text().strip() if hasattr(self, 'prognosis_only_label') else ''
            summary_prognosis_text = self.summary_prognosis_label.text().strip() if hasattr(self, 'summary_prognosis_label') else ''
            
            # Generate composite score table
            composite_data = None
            if hasattr(self, 'last_filtered_df') and self.last_filtered_df is not None and not self.last_filtered_df.empty:
                try:
                    composite_data = compute_composite_score(self.last_filtered_df.copy(), 
                                                           weights=self.metric_weights if hasattr(self, 'metric_weights') else None)
                except Exception:
                    pass
            
            # Generate filename
            default_filename = "analysis.png"
            if race_date and ref_course:
                date_clean = race_date.replace('/', '').replace('-', '')
                default_filename = f"{date_clean}_{ref_course}_analysis.png"
            elif race_date:
                date_clean = race_date.replace('/', '').replace('-', '')
                default_filename = f"{date_clean}_analysis.png"
            
            # Save to Downloads folder
            downloads_folder = get_downloads_folder()
            path = str(downloads_folder / default_filename)
            
            # Create figure in A4 format (210mm × 297mm = 8.27 × 11.69 inches)
            fig = plt.figure(figsize=(8.27, 11.69), dpi=150)
            fig.patch.set_facecolor('white')
            
            # Create axes for text content
            ax = fig.add_subplot(111)
            ax.axis('off')
            
            y_position = 0.98
            
            # Header section with RaceX branding
            header_text_racex = "🐴 RaceX v1.0 - Advanced Betting Analysis for Horse Racing"
            ax.text(0.5, y_position, header_text_racex, ha='center', va='top', fontsize=10, fontweight='bold',
                   transform=ax.transAxes, color='#1e3a8a')
            y_position -= 0.03
            
            # Race analysis header
            header_text = f"RaceX - Analyse: {prize_name if prize_name else 'Course'}"
            ax.text(0.5, y_position, header_text, ha='center', va='top', fontsize=12, fontweight='bold',
                   transform=ax.transAxes, color='#1e3a8a')
            y_position -= 0.03
            
            # Metadata section (compact)
            metadata_text = f"Ref: {ref_course} | Date: {race_date} | Hippodrome: {hippodrome}"
            ax.text(0.5, y_position, metadata_text, ha='center', va='top', fontsize=9,
                   transform=ax.transAxes, color='#4b5563')
            y_position -= 0.025
            
            timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            ax.text(0.5, y_position, f"Généré: {timestamp}", ha='center', va='top', fontsize=8,
                   transform=ax.transAxes, color='#999999', style='italic')
            y_position -= 0.04
            
            # Analysis sections (compact)
            sections = [
                ("Prono Flash", prognosis_text),
                ("Synthèse", summary_text),
                ("Intersection", summary_prognosis_text),
                ("Synthèse Only", exclusive_text),
                ("Prono Only", prognosis_only_text),
                ("Populaires", trending_text),
                ("Cordes Fav.", favorable_cordes_text),
            ]
            
            for title, text in sections:
                if text:
                    section_line = f"• {title}: {text}"
                    ax.text(0.05, y_position, section_line, ha='left', va='top', fontsize=8,
                           transform=ax.transAxes, wrap=True)
                    y_position -= 0.018
            
            # Separator before table (compact)
            y_position -= 0.015
            ax.text(0.05, y_position, "─" * 95, ha='left', va='top', fontsize=7,
                   transform=ax.transAxes, fontfamily='monospace', color='#3b82f6')
            y_position -= 0.025
            
            # Composite score table section
            if composite_data is not None and not composite_data.empty:
                ax.text(0.5, y_position, "CLASSEMENT COMPOSITE", 
                       ha='center', va='top', fontsize=10, fontweight='bold',
                       transform=ax.transAxes, color='#1e3a8a')
                y_position -= 0.028
                
                # Prepare table data
                table_df = composite_data.copy()
                
                # Sort by Composite score in ascending order (lowest first)
                if 'Composite' in table_df.columns:
                    table_df = table_df.sort_values('Composite', ascending=True)
                
                # Select display columns
                display_cols = []
                for col in ['Cheval', 'N°', 'N', 'COTE', 'Cote', 'Composite']:
                    if col in table_df.columns:
                        display_cols.append(col)
                
                if 'Cheval' not in display_cols and 'Cheval' in table_df.columns:
                    display_cols.insert(0, 'Cheval')
                if 'Composite' not in display_cols and 'Composite' in table_df.columns:
                    display_cols.append('Composite')
                
                # Limit to top 12 for compactness
                table_df = table_df[display_cols].head(12).copy()
                
                # Only create table if there's data
                if not table_df.empty:
                    # Format columns for display
                    table_data = []
                    for _, row in table_df.iterrows():
                        row_data = []
                        for col in display_cols:
                            val = row[col]
                            if col in ['N°', 'N']:
                                try:
                                    val = int(float(val)) if pd.notna(val) else ''
                                except (ValueError, TypeError):
                                    val = ''
                            elif pd.notna(val) and col != 'Cheval':
                                try:
                                    val = float(val)
                                    val = f"{val:.2f}"
                                except (ValueError, TypeError):
                                    val = str(val)
                            else:
                                val = str(val) if pd.notna(val) else ''
                            row_data.append(val)
                        table_data.append(row_data)
                    
                    # Add header
                    table_data.insert(0, display_cols)
                    
                    # Create matplotlib table with proper bounds checking
                    cell_height = 0.018
                    table_height = len(table_data) * cell_height
                    
                    # Ensure y_position doesn't go below bottom margin
                    table_bottom = max(y_position - table_height - 0.01, 0.05)
                    table_top = y_position
                    actual_table_height = table_top - table_bottom
                    
                    try:
                        table = ax.table(cellText=table_data, cellLoc='center', loc='upper center',
                                       bbox=[0.05, table_bottom, 0.9, actual_table_height])
                        
                        table.auto_set_font_size(False)
                        table.set_fontsize(7)
                        
                        # Style table
                        for i in range(len(table_data)):
                            for j in range(len(display_cols)):
                                try:
                                    cell = table[(i, j)]
                                    if i == 0:  # Header row
                                        cell.set_facecolor('#3b82f6')
                                        cell.set_text_props(weight='bold', color='white')
                                    else:
                                        cell.set_facecolor('#f9fafb' if i % 2 == 1 else 'white')
                                    cell.set_edgecolor('#e0e0e0')
                                    cell.set_linewidth(0.5)
                                except Exception:
                                    pass  # Skip if cell doesn't exist
                        
                        y_position -= table_height + 0.08
                        
                        # Footer with count
                        ax.text(0.5, y_position, f"Total: {len(table_data)-1} chevaux", ha='center', va='top', 
                               fontsize=8, transform=ax.transAxes, style='italic', color='#4b5563')
                    except Exception as table_error:
                        print(f"[WARNING] Failed to create table: {table_error}")
                        ax.text(0.5, y_position, "Erreur lors de la création du tableau", 
                               ha='center', va='top', fontsize=8, transform=ax.transAxes, color='red')
                        y_position -= 0.05
            
            # Add footer section at bottom
            footer_text = "RaceX v1.0 | Developed by Georges BODIONG | deebodiong@gmail.com | +226 74 91 15 38 / 60 35 44 00"
            ax.text(0.5, 0.01, footer_text, ha='center', va='bottom', fontsize=7,
                   transform=ax.transAxes, color='#999999', style='italic')
            
            # Save as PNG
            plt.tight_layout()
            plt.savefig(path, dpi=150, bbox_inches='tight', facecolor='white', edgecolor='none')
            plt.close(fig)
            
            QMessageBox.information(self, "Succès", f"Analyse enregistrée:\n{path}")
            self.statusBar().showMessage(f"PNG exporté: {path}")
            print(f"[DEBUG] PNG exported to {path}")
            
        except Exception as e:
            print(f"[ERROR] save_analysis_as_png failed: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Erreur", f"Impossible d'enregistrer l'image PNG:\n{str(e)}")
            self.statusBar().showMessage(f"Export échoué: {e}")

    def clear_database(self):
        reply = QMessageBox.question(self, "Confirmation", 
                                   "Effacer toutes les donnees de tous les bases de donnees ?",
                                   QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            try:
                cleared_count = 0
                db_config = [
                    ('zone_turf_flat.db', 'flat_races'),
                    ('zone_turf_trot.db', 'trot_races'),
                    # Legacy database support
                    ('flat_zone.db', 'flat_races'),
                    ('trot_zone.db', 'trot_races')
                ]
                
                for db_name, table_name in db_config:
                    conn = sqlite3.connect(db_name)
                    cursor = conn.cursor()
                    try:
                        cursor.execute(f"DELETE FROM {table_name}")
                        cleared_count += cursor.rowcount
                        conn.commit()
                    except:
                        pass
                    conn.close()
                
                # Refresh the database view; this will clear the db_table if empty
                self.load_database_data()

                # Ensure the UI is cleared for both current scrape and database view
                try:
                    self.current_table.setRowCount(0)
                    self.current_table.setColumnCount(0)
                    self.current_table.setHorizontalHeaderLabels([])
                except Exception:
                    pass

                try:
                    self.db_table.setRowCount(0)
                    self.db_table.setColumnCount(0)
                    self.db_table.setHorizontalHeaderLabels([])
                except Exception:
                    pass

                # Update stats and status
                self.stats_label.setText("Aucune donnee")
                self.status_label.setText("Bases de donnees effacees")

                QMessageBox.information(self, "Succes", f"{cleared_count} entrees supprimees des bases de donnees")
                
            except Exception as e:
                QMessageBox.critical(self, "Erreur", f"Impossible d'effacer la base de donnees : {e}")
    
    def export_data_from_menu(self):
        """Export current database view to CSV file via menu."""
        if not hasattr(self, 'full_df') or self.full_df.empty:
            QMessageBox.warning(self, "Aucune Donnee", "Aucune donnee e exporter")
            return
        
        try:
            # Save to Downloads folder
            downloads_folder = get_downloads_folder()
            timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
            default_filename = f"racex_data_{timestamp}.csv"
            file_path = str(downloads_folder / default_filename)
            
            if file_path:
                if str(file_path).endswith('.xlsx'):
                    self.full_df.to_excel(file_path, index=False)
                else:
                    self.full_df.to_csv(file_path, index=False)
                
                QMessageBox.information(self, "Succes", f"Donnees exportees vers {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Erreur d'Export", f"echec de l'export des donnees : {e}")
    
    def open_bet_generator_window(self):
        """Open the advanced bet generator window."""
        try:
            # Get current composite and filtered data
            composite_df = self.current_composite_df if hasattr(self, 'current_composite_df') else pd.DataFrame()
            filtered_df = self.current_filtered_df if hasattr(self, 'current_filtered_df') else pd.DataFrame()
            last_filtered_df = self.last_filtered_df if hasattr(self, 'last_filtered_df') else pd.DataFrame()
            
            # Create and show window, passing parent app reference
            self.bet_gen_window = BetGeneratorWindow(
                parent=self,
                current_composite_df=composite_df,
                current_filtered_df=filtered_df,
                last_filtered_df=last_filtered_df,
                parent_app=self  # Pass reference to parent app
            )
            self.bet_gen_window.show()
            self.bet_gen_window.raise_()
            self.bet_gen_window.activateWindow()
        except Exception as e:
            print(f"[ERROR] Failed to open bet generator: {e}")
            QMessageBox.critical(self, "Erreur", f"Impossible d'ouvrir le generateur de paris : {e}")
    
    def show_help(self):
        """Show comprehensive user guide."""
        help_text = """📖 GUIDE D'UTILISATION - RaceX

1️⃣ CHARGEMENT DES DONNEES
─────────────────────────────────────────
• Entrez l'URL de Zone-Turf dans le champ URL
• Cliquez sur "Scraper les Courses" pour extraire les données
• Les données sont automatiquement sauvegardees
NB: Pour avoir l'URL, rendez vous sur le site 
https://zone-turf.fr/programme.  Choisissez une reunion de 
plat ou obstacles, puis copiez l'URL de ladite reunion

2️⃣ FILTRAGE DES COURSES
─────────────────────────────────────────
• Selectionnez une DATE dans le premier filtre
• Choisissez un ID de COURSE dans le second filtre (e.g. R1C3)
• Les filtres activent automatiquement l'analyse

3️⃣ ANALYSE DE COURSE (Statistiques)
─────────────────────────────────────────────
• 📋 Synthese: Classement des chevaux par score composite
• 📈 Populaires: Chevaux en baisse de cote
• 🎯 Pronostics: Predictions du systeme
• 🎵 Cordes: Cordes favorables par hippodrome

4️⃣ VISUALISATION HEATMAP
─────────────────────────────────────────
• Affichage matriciel: Chevaux (lignes) vs Metriques (colonnes)
• Selectionnez les metriques a afficher via les cases a cocher
• Triez par metrique selectionnee (dropdown)
• Ajustez vmin/vmax pour changer l'echelle de couleur
• Vert = bon score, Rouge = mauvais score

5️⃣ GENERATION DE COMBINAISONS
─────────────────────────────────────────
• Cliquez sur "🎲 Generateur de Combinaisons"
a partir du menu "Paris"
• Onglet CLASSIQUE: Methode standard de priorites
• Onglet OUTSIDERS: Focus sur chevaux divergents
• Onglet SMART MIX: Combinaison optimale
• Parametrez: chevaux obligatoires, mise, max combinaisons
• Cliquez GENERER pour les combinaisons finales

6️⃣ EXPORT ET IMPRESSION
─────────────────────────────────────────
• 📷 Exporter Analyse (PNG): Screenshot de l'analyse
• 📄 Exporter Analyse (PDF): Rapport analyse detaille
• 📊 Exporter Combinaisons (PDF): Combinaisons generees
• 🖨️ Imprimer l'Analyse: Impression directe
• Les fichiers sont generes dans le dossier courant

7️⃣ PARAMETRES
─────────────────────────────────────────────
• Acces via Menu Parametres → Poids des Metriques
• Ajustez l'influence de chaque metrique (0-100%)
• Les modifications s'appliquent immediatement
• Les parametres sont sauvegarde automatiquement

⚙️ CONSEILS PRATIQUES
─────────────────────────────────────────
• Analysez plusieurs courses pour identifier les motifs
• Combinez plusieurs sources d'analyse pour plus de certitude
• Verifiez les tendances (chevaux populaires vs implicites)
• N'oubliez pas les facteurs externes (meteo, conditions...)
"""
        self.show_scrollable_dialog("Guide d'Utilisation", help_text)

    def show_tips(self):
        """Show analysis tips and strategies."""
        tips_text = """💡 CONSEILS D'ANALYSE - Optimiser vos Pronostics

🏇 SOURCING DES CHEVAUX (10 sources)
──────────────────────────────────────────────────
1. Prognosis: Predictions Zone-Turf officielles
2. Summary: Resume des analyses
3. Exclusive: Pronostics exclusifs Zone-Turf
4. Prognosis Only: Predictions pure Zone-Turf
5. Trending: Chevaux en hausse/baisse
6. Favorable Cordes: Cordes favorables par piste
7. Outsiders: Chevaux divergents + consistants
8. Underperforming: Favoris en sous-performance
9. Weight Stable: Chevaux au poids stable
10. Light Weight: Chevaux au poids leger

PRIORITE: Un cheval dans ≥4 sources → Auto-inclusion forte

📊 METRIQUES CLES
──────────────────────────────────────────────────
• IC (20%): Indice de Classe - Plus bas = meilleur
• S_COEFF (20%): Succes/Coefficient de victoire
• IF (15%): Indice de Forme - Plus bas = meilleur
• n_weight (10%): Stabilite du poids
• COTE (20%): Cote PMU - Plus bas = favori
• Corde (10%): Position de depart favorable
• VALEUR (5%): Valeur relative du cheval

LES 3 METRIQUES DOMINENT: IC, S_COEFF, COTE (60% ensemble)

🔍 STRATEGIE D'ANALYSE
──────────────────────────────────────────────────
1. Identifiez les FAVORIS: Cotes basses + composites hauts
2. Cherchez les OUTSIDERS: Composites hauts malgre cotes hautes
3. Verifiez la COHERENCE: Plusieurs sources d'accord?
4. Analysez la COMPETITION: Peu de bons chevaux = peu de doute
5. Considerez les FACTEURS EXTERNES: Conditions de course

⚡ SIGNAUX FORTS
──────────────────────────────────────────────────
✅ POSITIFS:
   • Cheval dans 5+ sources d'analyse
   • Tendance positive + Weight Stable
   • Cote basse = confiance PMU
   • Composite score > 0.70

⚠️ NEGATIFS:
   • Seul favori dans la course (trop de doutes)
   • IC mauvais + poids instable
   • Cote tres haute avec score moyen
   • Absence de source d'analyse

💰 GESTION DU RISQUE
──────────────────────────────────────────────────
• Limitez les combinaisons (max 20-30)
• Privilegiez les chevaux "solides" (≥4 sources)
• Utilisez le MODE SMART MIX pour optimiser
• Diversifiez: 50% certains, 50% outsiders
• Testez sur l'historique avant de jouer

📈 AMELIORATION CONTINUE
──────────────────────────────────────────────────
• Enregistrez vos predictions et resultats
• Mesurez la performance par metrique
• Ajustez les poids des metriques selon vos resultats
• Identifiez les biases (surfavoriser certains types)
• Apprenez de vos erreurs, repetez vos succes

🎓 EXEMPLES COURANTS
──────────────────────────────────────────────────
CAS 1: Favori avec score moyen
→ Evitez! Probable signal faible. Cherchez outsiders.

CAS 2: Outsider score eleve + 5 sources
→ BON SIGNAL! Valeur cachee. A inclure absolument.

CAS 3: Deux chevaux score similaire
→ Regardez les SOURCES. Celui avec plus de consensus = safer.

CAS 4: Cheval weight instable + composite bas
→ DOUTEUSE. Peut indiquer preparation incomplete.

CAS 5: Tendance positive + Light Weight + 4+ sources
→ EXCELLENTE OPPORTUNITE! Cheval montant de forme.
"""
        self.show_scrollable_dialog("Conseils d'Analyse", tips_text)

    def show_metrics(self):
        """Show detailed metrics explanations."""
        metrics_text = """📊 METRIQUES EXPLIQUEES - Comprendre Chaque Score

════════════════════════════════════════════════════════════════════
1. IC (Indice de Classe) - 20%
════════════════════════════════════════════════════════════════════
📋 Definition:
   IC = Poids - Cote PMU
   Represente la charge que le cheval porte a handicap

📈 Interpretation:
   • IC BAS (ex: -2) = Meilleur! Peu de poids malgre la cote
   • IC MOYEN (ex: 0-2) = Normal pour le handicap
   • IC ELEVE (ex: 5+) = Cheval lourd pour sa qualite

💡 Utilisation:
   "Plus bas IC = plus facile pour le cheval"
   Cherchez les chevaux avec IC negatif ou tres bas

════════════════════════════════════════════════════════════════════
2. S_COEFF (Succes Coefficient) - 20%
════════════════════════════════════════════════════════════════════
📋 Definition:
   S_COEFF = Taux de succes/victoires du cheval
   Combine performance historique avec coefficient

📈 Interpretation:
   • S_COEFF > 0.70 = Cheval fiable et gagnant
   • S_COEFF 0.40-0.70 = Moyen, parfois capable
   • S_COEFF < 0.40 = Rarement gagnant

💡 Utilisation:
   "Plus eleve S_COEFF = plus de victoires"
   Privilegiez les chevaux avec coefficient > 0.60

════════════════════════════════════════════════════════════════════
3. IF (Indice de Forme) - 15%
════════════════════════════════════════════════════════════════════
📋 Definition:
   IF = Score de condition physique actuelle
   Mesure comment le cheval a performé recemment

📈 Interpretation:
   • IF BAS (-5 a -1) = Excellent! Tres bon etat
   • IF MOYEN (-1 a 2) = Normal
   • IF ELEVE (2+) = Mauvaise forme, douter

💡 Utilisation:
   "Plus bas IF = meilleure condition physique"
   Les chevaux positifs en forme sont les meilleurs paris

════════════════════════════════════════════════════════════════════
4. n_weight (Weight Change) - 10%
════════════════════════════════════════════════════════════════════
📋 Definition:
   n_weight = Poids actuel - Poids precedente course
   Mesure la stabilite de poids du cheval

📈 Interpretation:
   • n_weight PROCHE 0 = Stable et fiable
   • n_weight +/- 0.5 kg = Variation normale
   • n_weight > 1.0 kg = Instable, probleme possible

💡 Utilisation:
   "Cherchez la stabilite du poids"
   Les chevaux avec weight change proche de 0 sont plus predictibles

════════════════════════════════════════════════════════════════════
5. COTE (PMU Odds) - 20%
════════════════════════════════════════════════════════════════════
📋 Definition:
   COTE = Cote de pari du PMU
   Represente la confiance du marche (parieurs)

📈 Interpretation:
   • COTE BASSE (1.2-2.5) = Favori, confiance PMU
   • COTE MOYENNE (2.5-5.0) = Outsider modere
   • COTE HAUTE (5.0+) = Outsider, peu de confiance

💡 Utilisation:
   "Basse cote = consensus sur le favori"
   Combinez avec d'autres metriques pour confirmer

════════════════════════════════════════════════════════════════════
6. Corde (Track Position) - 10%
════════════════════════════════════════════════════════════════════
📋 Definition:
   Corde = Position de depart sur la piste
   Certaines cordes sont favorables selon l'hippodrome

📈 Interpretation:
   • CORDE FAVORISANTE = Valeur +1 a +2
   • CORDE NEUTRE = Valeur 0
   • CORDE DEFAVORISANTE = Valeur -1 a -2

💡 Utilisation:
   "L'hippodrome a des cordes favorisantes"
   Les donnees Zone-Turf identifient ces avantages

════════════════════════════════════════════════════════════════════
7. VALEUR (Horse Value) - 5%
════════════════════════════════════════════════════════════════════
📋 Definition:
   VALEUR = Valeur relative du cheval vs competition
   Combine tous les facteurs pour un score unique

📈 Interpretation:
   • VALEUR HAUTE (0.7-1.0) = Bon rapport qualite/cote
   • VALEUR MOYENNE (0.4-0.7) = Normal
   • VALEUR BASSE (0.0-0.4) = Mauvais rapport

💡 Utilisation:
   "Score synthetique de valeur du cheval"
   Utilisez pour confirmer d'autres metriques

════════════════════════════════════════════════════════════════════
COMPOSITE SCORE (Synthese)
════════════════════════════════════════════════════════════════════
📋 Definition:
   Composite = Moyenne pondere de toutes les metriques
   Score 0-1 representant la valeur globale du cheval

📈 Interpretation:
   • SCORE 0.80-1.0 = Favori de qualite
   • SCORE 0.60-0.80 = Bon candidat
   • SCORE 0.40-0.60 = Moyen, risque eleve
   • SCORE < 0.40 = Tres faible, eviter

💡 Utilisation:
   "Cherchez les chevaux avec composite > 0.65"
   Combinez avec confirmation d'autres sources

════════════════════════════════════════════════════════════════════
HEATMAP COLOR GUIDE
════════════════════════════════════════════════════════════════════
🟢 VERT (0.8-1.0): Excellent score, tres bon pour cette metrique
🟡 JAUNE (0.4-0.8): Score moyen, acceptable
🔴 ROUGE (0.0-0.4): Mauvais score, point faible

Plus la couleur est VERTE, mieux c'est pour le cheval!

════════════════════════════════════════════════════════════════════
COMBINAISON OPTIMALE
════════════════════════════════════════════════════════════════════
Meilleur cheval = COMPOSITE ELEVE + IC BAS + S_COEFF ELEVE + IF BAS
  ↓↓↓
  Cherchez les points VERTS a travers les metriques cles
"""
        self.show_scrollable_dialog("Metriques Expliquees", metrics_text)

    def show_scrollable_dialog(self, title, text):
        """Show a dialog with scrollable text area."""
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        dlg.setGeometry(100, 100, 950, 750)
        
        layout = QVBoxLayout(dlg)
        
        text_edit = QTextEdit()
        text_edit.setPlainText(text)
        text_edit.setReadOnly(True)
        text_edit.setFont(QFont('Courier New', 9))
        
        layout.addWidget(text_edit)
        
        close_btn = QPushButton("Fermer")
        close_btn.clicked.connect(dlg.accept)
        layout.addWidget(close_btn)
        
        dlg.exec_()

    def show_about(self):
        """Show about dialog with app information."""
        about_text = """RaceX - Version 1.0

Votre outil d'intelligence et d'analyse des courses hippiques

✨ Fonctionnalites principales:
─────────────────────────────────────────────────
✓ Extraction des donnees de courses depuis Zone-Turf
✓ Analyse complete avec 7 metriques (IC, S_COEFF, IF, n_weight, COTE, Corde, VALEUR)
✓ 10 sources d'analyse combinables (priorite ≥4 sources)
✓ Generation automatique de pronostics + combinaisons
✓ Visualisation interactive en heatmap
✓ Export PDF et impression
✓ Parametrage dynamique des poids metriques

📊 Capacites d'analyse:
─────────────────────────────────────────────────
• Analyse par course avec statistiques detaillees
• Identification des chevaux en surperformance/sous-performance
• Detection des outsiders avec divergence forte
• Analyse tendancielle (chevaux en hausse/baisse)
• Prise en compte des cordes favorables par hippodrome
• Scoring par poids stable et leger

🏇 Generation de combinaisons:
─────────────────────────────────────────────────
• Mode Classique: Methode standard de priorites
• Mode Outsiders: Focus sur chevaux divergents
• Mode Smart Mix: Combinaison optimale intelligente

💾 Donnees supportees:
─────────────────────────────────────────────────
• Courses Plat: zone_turf_flat.db
• Courses Trot: zone_turf_trot.db
• Autres sources: turfomania, PMU_RACING

👨‍💼 Developpeur:
Georges Bodiong

🛠️ Technologie:
PyQt5 - Matplotlib - Pandas - SQLite

📅 Release: 2026

Pour plus d'informations, consultez le Guide d'Utilisation
dans le menu Aide."""
        
        QMessageBox.information(self, "À Propos de RaceX", about_text)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Configure UTF-8 and Unicode support
    import locale
    try:
        locale.setlocale(locale.LC_ALL, '')  # Use system locale
    except Exception:
        pass  # Fallback if system locale unavailable
    
    # Set application font to support Unicode (French characters, emojis)
    from PyQt5.QtGui import QFont, QFontDatabase
    
    # Try to load emoji fonts for better emoji support
    emoji_fonts = [
        'Segoe UI Emoji',    # Windows native emoji font
        'Apple Color Emoji', # macOS emoji font
        'Noto Color Emoji',  # Linux emoji font
        'Segoe UI Symbol',   # Windows symbols
    ]
    
    # Create main font that includes emoji support
    unicode_font = QFont()
    # Combine emoji fonts with regular fonts
    font_families = emoji_fonts + ['Segoe UI', 'Arial', 'DejaVu Sans', 'Liberation Sans']
    unicode_font.setFamilies(font_families)
    unicode_font.setPointSize(10)
    app.setFont(unicode_font)
    
    # Create and show splash screen
    splash_pixmap = QPixmap('assets/race.png')
    if not splash_pixmap.isNull():
        splash_pixmap = splash_pixmap.scaledToWidth(400, Qt.SmoothTransformation)
        splash = QSplashScreen(splash_pixmap)
        splash.show()
        app.processEvents()
        splash.showMessage("Loading...", Qt.AlignBottom | Qt.AlignCenter, Qt.white)
    
    # Create main window
    window = RaceScraperApp()
    window.show()
    
    # Close splash screen
    if not splash_pixmap.isNull():
        splash.finish(window)
    
    sys.exit(app.exec_())
