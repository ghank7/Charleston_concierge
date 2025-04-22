"""
Event Scrapers Module

This module provides a base class for all event scrapers and functions
for managing and running scrapers.
"""

import os
import sys
import inspect
import importlib
import importlib.util
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union


class BaseEventScraper(ABC):
    """Base class for all event scrapers"""
    
    def __init__(self, db_connection=None):
        """Initialize the scraper with optional database connection"""
        self.db_connection = db_connection
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of this data source"""
        pass
        
    @abstractmethod
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30) -> List[str]:
        """Get event URLs for a specific date range"""
        pass
        
    @abstractmethod
    def scrape_event(self, event_url: str) -> Optional[Dict[str, Any]]:
        """Scrape details for a single event"""
        pass
        
    def scrape_events(self, start_date=None, end_date=None, days=30) -> List[Dict[str, Any]]:
        """Scrape all events in a date range"""
        event_urls = self.get_events_for_date_range(start_date, end_date, days)
        print(f"Found {len(event_urls)} event URLs to scrape")
        
        events = []
        for url in event_urls:
            event_data = self.scrape_event(url)
            if event_data:
                events.append(event_data)
        
        print(f"Successfully scraped {len(events)} events")
        return events
        
    def save_to_database(self, events: List[Dict[str, Any]]) -> int:
        """Save events to the database"""
        if not self.db_connection:
            print("No database connection provided, can't save events")
            return 0
            
        count = 0
        for event in events:
            try:
                # Add source attribution if not present
                if 'source' not in event:
                    event['source'] = self.source_name
                    
                # Use the database connection to add the event
                if hasattr(self.db_connection, 'add_event_from_dict'):
                    # Prefer the dictionary method if available
                    self.db_connection.add_event_from_dict(event)
                    count += 1
                elif hasattr(self.db_connection, 'add_event'):
                    # Fall back to the individual parameters method
                    self.db_connection.add_event(
                        name=event.get('Name', event.get('name')),
                        date=event.get('Date', event.get('date')),
                        time=event.get('Time', event.get('time')),
                        location=event.get('Location', event.get('location')),
                        description=event.get('Description', event.get('description')),
                        url=event.get('URL', event.get('url')),
                        image_url=event.get('Image_URL', event.get('image_url')),
                        source=event.get('Source', event.get('source')),
                        business_id=event.get('Business_ID', event.get('business_id'))
                    )
                    count += 1
                else:
                    print("Database connection doesn't have an add_event method")
                    break
            except Exception as e:
                print(f"Error saving event to database: {e}")
                
        return count


def discover_scrapers() -> Dict[str, type]:
    """Discover all available scraper classes"""
    scrapers = {}
    
    # Get the directory with source modules
    sources_dir = os.path.join(os.path.dirname(__file__), 'sources')
    if not os.path.exists(sources_dir):
        print(f"Error: Sources directory not found at {sources_dir}")
        return scrapers
    
    # Discover Python modules in the sources directory
    for filename in os.listdir(sources_dir):
        if filename.endswith('.py') and not filename.startswith('__'):
            module_name = filename[:-3]  # Remove .py extension
            module_path = os.path.join(sources_dir, filename)
            
            try:
                # Import the module
                spec = importlib.util.spec_from_file_location(f"data_collection.sources.{module_name}", module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # Find scraper classes in the module
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, BaseEventScraper) and 
                        obj is not BaseEventScraper):
                        scrapers[name] = obj
            except Exception as e:
                print(f"Error loading scraper module {module_name}: {e}")
    
    return scrapers


def get_available_scrapers() -> Dict[str, type]:
    """Get all available scrapers"""
    return discover_scrapers()


def get_scraper(scraper_name: str, db_connection=None) -> Optional[BaseEventScraper]:
    """Get a specific scraper by name"""
    scrapers = discover_scrapers()
    if scraper_name not in scrapers:
        print(f"Scraper '{scraper_name}' not found. Available scrapers: {list(scrapers.keys())}")
        return None
        
    # Instantiate the scraper with the database connection
    return scrapers[scraper_name](db_connection)


def run_scraper(scraper_name: str, db_connection=None, start_date=None, end_date=None, days=30, save_to_db=True) -> List[Dict[str, Any]]:
    """Run a specific scraper by name"""
    scraper = get_scraper(scraper_name, db_connection)
    if not scraper:
        return []
        
    # Scrape events
    events = scraper.scrape_events(start_date, end_date, days)
    
    # Save to database if requested
    if save_to_db and db_connection:
        saved = scraper.save_to_database(events)
        print(f"Saved {saved} events to database")
        
    return events


def run_all_scrapers(db_connection=None, start_date=None, end_date=None, days=30, save_to_db=True) -> Dict[str, List[Dict[str, Any]]]:
    """Run all available scrapers"""
    scrapers = discover_scrapers()
    results = {}
    
    for name, scraper_class in scrapers.items():
        print(f"Running scraper: {name}")
        try:
            # Instantiate the scraper
            scraper = scraper_class(db_connection)
            
            # Scrape events
            events = scraper.scrape_events(start_date, end_date, days)
            
            # Save to database if requested
            if save_to_db and db_connection:
                saved = scraper.save_to_database(events)
                print(f"Saved {saved} events to database")
                
            results[name] = events
        except Exception as e:
            print(f"Error running scraper {name}: {e}")
            results[name] = []
    
    return results 