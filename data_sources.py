"""
Data Source Manager for multiple racing websites.
Provides a unified interface for scraping race data from different sources.
"""

from abc import ABC, abstractmethod
import pandas as pd
from model_functions import get_trot_race


class RaceDataSource(ABC):
    """Abstract base class for race data sources"""
    
    @abstractmethod
    def scrape_flat(self, url, progress_callback=None):
        """Scrape flat race data"""
        pass
    
    @abstractmethod
    def scrape_trot(self, url, progress_callback=None):
        """Scrape trot race data"""
        pass


class ZoneTurfDataSource(RaceDataSource):
    """Data source for zone-turf.fr"""
    
    def __init__(self):
        self.name = "Zone-Turf"
        self.base_url = "https://www.zone-turf.fr"
    
    def scrape_flat(self, url, progress_callback=None, cancel_check=None):
        """Scrape flat races from zone-turf"""
        print(f"[INFO] Scraping flat races from {self.name}: {url}")
        # Lazy import to avoid optional selenium deps on app startup.
        from flat_zone import scrape_zone_turf as scrape_zone_turf_flat
        return scrape_zone_turf_flat(url, progress_callback=progress_callback, cancel_check=cancel_check)
    
    def scrape_trot(self, url, progress_callback=None, cancel_check=None):
        """Scrape trot races from zone-turf"""
        print(f"[INFO] Scraping trot races from {self.name}: {url}")
        # Lazy import to avoid optional selenium deps on app startup.
        from zone_trot import scrape_zone_turf_trot
        return scrape_zone_turf_trot(url, progress_callback=progress_callback, cancel_check=cancel_check)


class DataSourceManager:
    """Manages data sources (currently Zone-Turf only)"""
    
    def __init__(self):
        self.sources = {
            'zone-turf': ZoneTurfDataSource(),
        }
        self.default_source = 'zone-turf'
    
    def get_source(self, source_name):
        """Get a data source by name"""
        return self.sources.get(source_name, self.sources[self.default_source])
    
    def get_available_sources(self):
        """Get list of available source names"""
        return list(self.sources.keys())
    
    def scrape_races(self, url, race_type, source_name='zone-turf', progress_callback=None, cancel_check=None):
        """
        Scrape races from specified source
        
        Args:
            url: URL to scrape
            race_type: 'flat' or 'trot'
            source_name: Name of the data source ('zone-turf' or 'turfomania')
            progress_callback: Optional callable(percent, message) for progress updates
            cancel_check: Optional callable that returns True if cancellation requested
        
        Returns:
            DataFrame with race data
        """
        source = self.get_source(source_name)
        
        if race_type.lower() == 'flat':
            # pass progress_callback and cancel_check through to the data source
            return source.scrape_flat(url, progress_callback=progress_callback, cancel_check=cancel_check)
        elif race_type.lower() == 'trot':
            return source.scrape_trot(url, progress_callback=progress_callback, cancel_check=cancel_check)
        else:
            raise ValueError(f"Invalid race type: {race_type}")


# Global instance
data_source_manager = DataSourceManager()
