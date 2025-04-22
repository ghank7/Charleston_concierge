"""
Event Importer

A standardized pipeline for importing events from various scrapers into the database.
This script handles importing events from different sources, data cleaning,
deduplication, and business entity matching.
"""

import os
import sys
import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple

# Add the project root to path for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# Import database manager and scrapers
from utils.database_manager import CharlestonDB
from data_collection.sources.holycitysinner import HolyCitySinnerScraper
from data_collection.sources.cvb import CharlestonCVBScraper


class EventImporter:
    """Class that handles importing events from various sources into the database"""
    
    def __init__(self, db_connection=None):
        """Initialize the importer with a database connection"""
        self.db = db_connection if db_connection else CharlestonDB()
        self.conn = None
        self.cursor = None
        
        # Track import statistics
        self.imported_count = 0
        self.duplicate_count = 0
        self.business_match_count = 0
        
        # Cache for existing events and businesses
        self.existing_events = set()
        self.businesses = []
        self.business_by_name = {}
        self.business_by_location = {}
        self.business_keywords = {}
        
    def connect_to_db(self):
        """Establish database connection"""
        if not self.conn:
            try:
                self.conn = self.db.connect()
                self.cursor = self.conn.cursor()
                print("Connected to database")
                return True
            except Exception as e:
                print(f"Error connecting to database: {e}")
                return False
        return True
    
    def load_existing_data(self):
        """Load existing events and businesses from database"""
        if not self.connect_to_db():
            return False
            
        # Get existing events to avoid duplicates
        try:
            self.cursor.execute("SELECT name, date FROM events")
            self.existing_events = set((name, date) for name, date in self.cursor.fetchall())
            print(f"Loaded {len(self.existing_events)} existing events")
            
            # Get business data for matching
            self.cursor.execute("SELECT id, name, location FROM businesses")
            self.businesses = self.cursor.fetchall()
            print(f"Loaded {len(self.businesses)} businesses for matching")
            
            # Create lookup dictionaries for efficient matching
            self.business_by_name = {name.lower(): business_id for business_id, name, _ in self.businesses if name}
            self.business_by_location = {location.lower(): business_id for business_id, _, location in self.businesses if location}
            
            # Create a combined lookup for partial matching
            self.business_keywords = {}
            for business_id, name, location in self.businesses:
                if name:
                    words = [w for w in re.split(r'\W+', name.lower()) if len(w) > 3]
                    for word in words:
                        if word not in self.business_keywords:
                            self.business_keywords[word] = []
                        self.business_keywords[word].append(business_id)
            
            return True
        except Exception as e:
            print(f"Error loading existing data: {e}")
            return False
    
    def import_events_from_scraper(self, scraper_name: str) -> int:
        """
        Import events from a specific scraper
        
        Args:
            scraper_name: Name of the scraper to use ("hcs", "cvb", etc.)
            
        Returns:
            Number of events imported
        """
        print(f"Importing events from {scraper_name}...")
        
        # Reset statistics
        self.imported_count = 0
        self.duplicate_count = 0
        self.business_match_count = 0
        
        # Get events from the appropriate scraper
        events = self._get_events_from_scraper(scraper_name)
        
        if not events:
            print(f"No events found from {scraper_name}")
            return 0
            
        print(f"Found {len(events)} events from {scraper_name}")
        
        # Load existing data for deduplication and matching
        if not self.load_existing_data():
            print("Failed to load existing data, aborting import")
            return 0
            
        # Get the next available event ID
        self.cursor.execute("SELECT MAX(id) FROM events")
        result = self.cursor.fetchone()
        next_id = (result[0] + 1) if result[0] is not None else 0
        
        # Prepare for import
        events_to_import = []
        
        # Process each event
        for event in events:
            # Clean data
            name = event.get('Name') if event.get('Name') else None
            if not name:
                continue  # Skip if no name
                
            date = event.get('Date') if event.get('Date') else None
            time = event.get('Time') if event.get('Time') else None
            location = event.get('Location') if event.get('Location') else None
            description = event.get('Description') if event.get('Description') else None
            url = event.get('URL') if event.get('URL') else None
            image_url = event.get('Image_URL') if event.get('Image_URL') else None
            source = event.get('Source') if event.get('Source') else scraper_name
            
            # Skip if this event already exists
            if (name, date) in self.existing_events:
                self.duplicate_count += 1
                continue
            
            # Find matching business
            business_id = self._find_matching_business(name, location)
            
            # Count successful business matches
            if business_id is not None:
                self.business_match_count += 1
            
            # Event now ready for import
            events_to_import.append((
                next_id + self.imported_count,
                name,
                date,
                time,
                location,
                description,
                url,
                image_url,
                source,
                business_id
            ))
            
            self.imported_count += 1
        
        # Batch insert events
        if events_to_import:
            self.cursor.executemany("""
            INSERT INTO events (id, name, date, time, location, description, url, image_url, source, business_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, events_to_import)
            
            # Commit changes
            self.conn.commit()
            
            # Print summary
            print(f"Successfully imported {self.imported_count} events")
            print(f"Skipped {self.duplicate_count} duplicate events")
            print(f"Established {self.business_match_count} business relationships ({self.business_match_count/max(1, self.imported_count)*100:.1f}%)")
        else:
            print("No new events to import")
        
        return self.imported_count
    
    def import_events_from_csv(self, csv_path: str) -> int:
        """
        Import events from a CSV file
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Number of events imported
        """
        print(f"Importing events from CSV: {csv_path}")
        
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return 0
            
        # Load event data from CSV
        try:
            df = pd.read_csv(csv_path)
            print(f"Loaded {len(df)} events from CSV")
            
            # Convert DataFrame to list of dictionaries
            events = df.to_dict('records')
            
            # Reset statistics
            self.imported_count = 0
            self.duplicate_count = 0
            self.business_match_count = 0
            
            # Load existing data for deduplication and matching
            if not self.load_existing_data():
                print("Failed to load existing data, aborting import")
                return 0
                
            # Get the next available event ID
            self.cursor.execute("SELECT MAX(id) FROM events")
            result = self.cursor.fetchone()
            next_id = (result[0] + 1) if result[0] is not None else 0
            
            # Prepare for import
            events_to_import = []
            
            # Process each event
            for event in events:
                # Skip if no name
                if pd.isna(event.get('Name')) or not event.get('Name'):
                    continue
                    
                # Clean data
                name = event['Name'] if not pd.isna(event['Name']) else None
                date = event['Date'] if not pd.isna(event['Date']) else None
                time = event['Time'] if not pd.isna(event['Time']) else None
                location = event['Location'] if not pd.isna(event['Location']) else None
                description = event['Description'] if not pd.isna(event['Description']) else None
                url = event['URL'] if not pd.isna(event['URL']) else None
                image_url = event['Image_URL'] if not pd.isna(event['Image_URL']) else None
                source = event['Source'] if not pd.isna(event['Source']) else os.path.basename(csv_path)
                
                # Skip if this event already exists
                if (name, date) in self.existing_events:
                    self.duplicate_count += 1
                    continue
                
                # Find matching business
                business_id = self._find_matching_business(name, location)
                
                # Count successful business matches
                if business_id is not None:
                    self.business_match_count += 1
                
                # Event now ready for import
                events_to_import.append((
                    next_id + self.imported_count,
                    name,
                    date,
                    time,
                    location,
                    description,
                    url,
                    image_url,
                    source,
                    business_id
                ))
                
                self.imported_count += 1
            
            # Batch insert events
            if events_to_import:
                self.cursor.executemany("""
                INSERT INTO events (id, name, date, time, location, description, url, image_url, source, business_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, events_to_import)
                
                # Commit changes
                self.conn.commit()
                
                # Print summary
                print(f"Successfully imported {self.imported_count} events")
                print(f"Skipped {self.duplicate_count} duplicate events")
                print(f"Established {self.business_match_count} business relationships ({self.business_match_count/max(1, self.imported_count)*100:.1f}%)")
            else:
                print("No new events to import")
            
            return self.imported_count
            
        except Exception as e:
            print(f"Error importing events from CSV: {e}")
            return 0
    
    def import_events_from_all_sources(self) -> int:
        """
        Import events from all available sources
        
        Returns:
            Total number of events imported
        """
        total_imported = 0
        
        # Import from Holy City Sinner
        total_imported += self.import_events_from_scraper("hcs")
        
        # Import from Charleston CVB
        total_imported += self.import_events_from_scraper("cvb")
        
        # Add more sources as needed
        
        print(f"Total events imported from all sources: {total_imported}")
        return total_imported
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")
    
    def _get_events_from_scraper(self, scraper_name: str) -> List[Dict[str, Any]]:
        """
        Get events from the specified scraper
        
        Args:
            scraper_name: Name of the scraper to use
            
        Returns:
            List of event dictionaries
        """
        if scraper_name.lower() == "hcs":
            scraper = HolyCitySinnerScraper()
            return scraper.get_all_events()
            
        elif scraper_name.lower() == "cvb":
            scraper = CharlestonCVBScraper()
            return scraper.get_events_for_date_range()
            
        else:
            print(f"Unknown scraper: {scraper_name}")
            return []
    
    def _find_matching_business(self, event_name: str, event_location: str) -> Optional[int]:
        """
        Find a matching business ID for an event
        
        Args:
            event_name: Name of the event
            event_location: Location of the event
            
        Returns:
            Business ID if found, None otherwise
        """
        business_id = None
        
        # Method 1: Direct match on location
        if event_location and event_location.lower() in self.business_by_location:
            business_id = self.business_by_location[event_location.lower()]
        
        # Method 2: Check if location contains a business name
        if not business_id and event_location:
            location_lower = event_location.lower()
            for business_name, b_id in self.business_by_name.items():
                if business_name in location_lower:
                    business_id = b_id
                    break
        
        # Method 3: Check if any business names are in the event name
        if not business_id and event_name:
            name_lower = event_name.lower()
            for business_name, b_id in self.business_by_name.items():
                if business_name in name_lower:
                    business_id = b_id
                    break
        
        # Method 4: Check for keyword matches in location or event name
        if not business_id:
            potential_matches = {}
            
            # Find keywords in location
            if event_location:
                location_words = re.split(r'\W+', event_location.lower())
                for word in location_words:
                    if word in self.business_keywords:
                        for b_id in self.business_keywords[word]:
                            potential_matches[b_id] = potential_matches.get(b_id, 0) + 2  # Higher weight for location
            
            # Find keywords in name
            if event_name:
                name_words = re.split(r'\W+', event_name.lower())
                for word in name_words:
                    if word in self.business_keywords:
                        for b_id in self.business_keywords[word]:
                            potential_matches[b_id] = potential_matches.get(b_id, 0) + 1
            
            # Find best match
            if potential_matches:
                best_match = max(potential_matches.items(), key=lambda x: x[1])
                if best_match[1] >= 2:  # Require at least 2 points for a match
                    business_id = best_match[0]
        
        return business_id


def import_all_events():
    """
    Import events from all sources
    
    Returns:
        Total number of events imported
    """
    print("=" * 50)
    print("  Importing Events from All Sources")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    importer = EventImporter()
    total_imported = importer.import_events_from_all_sources()
    importer.close()
    
    print(f"\nImport completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total events imported: {total_imported}")
    return total_imported


def import_events_from_source(source_name: str):
    """
    Import events from a specific source
    
    Args:
        source_name: Name of the source ("hcs", "cvb", etc.)
        
    Returns:
        Number of events imported
    """
    print("=" * 50)
    print(f"  Importing Events from {source_name.upper()}")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    importer = EventImporter()
    count = importer.import_events_from_scraper(source_name)
    importer.close()
    
    print(f"\nImport completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total events imported: {count}")
    return count


if __name__ == "__main__":
    # If run with arguments, import from the specified source
    if len(sys.argv) > 1:
        source = sys.argv[1].lower()
        import_events_from_source(source)
    else:
        # Otherwise import from all sources
        import_all_events() 