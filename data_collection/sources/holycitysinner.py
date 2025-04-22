"""
HolyCitySinner Event Scraper

This module provides a scraper for extracting events from the Holy City Sinner website.
"""

import re
import logging
import time
import random
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union

import requests
from bs4 import BeautifulSoup

from data_collection.scrapers import BaseEventScraper
from data_collection.utils import (
    clean_text,
    get_soup,
    get_headers,
    extract_date_range,
    parse_datetime,
    generate_date_range
)

# Set up logger
logger = logging.getLogger(__name__)


class HolyCitySinnerScraper(BaseEventScraper):
    """Scraper for HolyCitySinner website events"""
    
    def __init__(self, db_connection=None):
        """Initialize the scraper"""
        super().__init__(db_connection)
        self.base_url = "https://holycitysinner.com"
        # Use the actual events page URLs from the site
        self.events_url = "https://holycitysinner.com/calendar"
        self.weekend_events_url = "https://holycitysinner.com/lifestyle/charleston-weekend-events/"
        self.mobile_events_url = "https://holycitysinner.com/entertainment/things-to-do-in-charleston-events-parties-plans-mobile/"
        self.headers = get_headers()
        
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "Holy City Sinner"
    
    def get_all_events(self, start_date=None, end_date=None, days=30) -> List[Dict]:
        """
        Get events from all available sources on the website
        
        Args:
            start_date: Start date for events (string format: 'YYYY-MM-DD')
            end_date: End date for events (string format: 'YYYY-MM-DD') 
            days: Number of days to look ahead if no end_date is provided
            
        Returns:
            List of event dictionaries
        """
        all_events = []
        
        # Get current date info
        if not start_date:
            today = datetime.now()
            start_date = today.strftime('%Y-%m-%d')
        else:
            today = datetime.strptime(start_date, '%Y-%m-%d')
        
        if not end_date:
            end_date = (today + timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Set a max date threshold (events more than 1 year in the future probably have incorrect dates)
        max_date_threshold = (today + timedelta(days=365)).strftime('%Y-%m-%d')
        
        print(f"Fetching HolyCitySinner events from {start_date} to {end_date}")
        
        # 1. Try web scraping approach for calendar page
        print("Attempting to scrape events from calendar page...")
        web_events = self.get_events_for_date_range(start_date, end_date, days)
        if web_events:
            print(f"Found {len(web_events)} events from calendar scraping")
            all_events.extend(web_events)
        
        # 2. Try the weekend events page
        print("Attempting to fetch weekend events...")
        weekend_events = self.fetch_weekend_events()
        if weekend_events:
            # Deduplicate events by URL
            existing_urls = {event.get('URL') for event in all_events}
            unique_weekend_events = [e for e in weekend_events if e.get('URL') not in existing_urls]
            print(f"Adding {len(unique_weekend_events)} unique events from weekend events")
            all_events.extend(unique_weekend_events)
            
        # 3. Try the mobile events page
        print("Attempting to fetch events from mobile events page...")
        try:
            mobile_soup = get_soup(self.mobile_events_url)
            if mobile_soup:
                content_elem = mobile_soup.select_one('article') or mobile_soup.select_one('.entry-content')
                if content_elem:
                    mobile_events = self._extract_events_from_text(content_elem.text, self.mobile_events_url)
                    if mobile_events:
                        # Deduplicate events
                        existing_urls = {event.get('URL') for event in all_events}
                        unique_mobile_events = [e for e in mobile_events if e.get('URL') not in existing_urls]
                        print(f"Adding {len(unique_mobile_events)} unique events from mobile events page")
                        all_events.extend(unique_mobile_events)
        except Exception as e:
            print(f"Error fetching mobile events: {e}")
        
        # Filter out events with suspicious dates (far in the future)
        filtered_events = []
        for event in all_events:
            event_date = event.get('Date')
            if event_date and event_date <= max_date_threshold:
                filtered_events.append(event)
            else:
                print(f"Filtering out event with suspicious date: {event.get('Name')} - {event_date}")
        
        print(f"Total valid unique events found: {len(filtered_events)}")
        return filtered_events
    
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30) -> List[Dict]:
        """
        Get events for the specified date range by scraping the website
        
        Args:
            start_date: Start date for events (string format: 'YYYY-MM-DD')
            end_date: End date for events (string format: 'YYYY-MM-DD')
            days: Number of days to look ahead if no end_date is provided
            
        Returns:
            List of event dictionaries
        """
        # If no dates provided, use next 30 days
        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            # Calculate end date
            end_date = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
            
        print(f"Fetching HolyCitySinner events from {start_date} to {end_date}")
        
        try:
            # Try to fetch the main events page
            soup = get_soup(self.events_url)
            if not soup:
                print(f"Error fetching events page: {self.events_url}")
                return []
                
            # Find event links - various patterns that might indicate event pages
            event_links = []
            
            # Look for links that might be event pages
            for link in soup.select('a'):
                href = link.get('href')
                if not href:
                    continue
                
                # Various patterns that might indicate an event link
                # 1. Links with /events/ in them
                # 2. Links with /event/ in them
                # 3. Links that point to mylonews.trumba.com
                # 4. Links that contain "calendar" and have date parameters
                if (('/events/' in href or '/event/' in href) or 
                    ('trumba.com' in href) or 
                    ('calendar' in href and '?date=' in href)):
                    
                    # Make the URL absolute if it's relative
                    event_url = self._make_absolute_url(href)
                    if event_url not in event_links:
                        event_links.append(event_url)
            
            print(f"Found {len(event_links)} potential event links")
            
            # If no events found, try alternate approach
            if len(event_links) == 0 or len(event_links) < 5:  # If we found very few, still check alternates
                print("Trying alternate event search approach...")
                # Try looking at other pages that might list events
                alternate_urls = [
                    "https://holycitysinner.com/entertainment/things-to-do-in-charleston-events-parties-plans-mobile/",
                    "https://holycitysinner.com/lifestyle/charleston-weekend-events/",
                    "https://holycitysinner.com/news",
                    "https://holycitysinner.com/entertainment",
                    "https://holycitysinner.com/food-bev"
                ]
                
                for url in alternate_urls:
                    try:
                        alt_soup = get_soup(url)
                        if alt_soup:
                            for link in alt_soup.select('a'):
                                href = link.get('href')
                                if href and (('/events/' in href or '/event/' in href) or 
                                            ('trumba.com' in href)):
                                    event_url = self._make_absolute_url(href)
                                    if event_url not in event_links:
                                        event_links.append(event_url)
                    except Exception as e:
                        print(f"Error with alternate URL {url}: {e}")
            
            # If still no events found, directly parse the calendar page text for events
            if len(event_links) == 0:
                print("No event links found. Trying to parse calendar page text directly...")
                content_elem = soup.select_one('article') or soup.select_one('.entry-content')
                if content_elem:
                    direct_events = self._extract_events_from_text(content_elem.text, self.events_url)
                    return direct_events
            
            # Scrape each potential event page
            events = []
            for url in event_links:
                # Add delay to avoid being blocked
                time.sleep(random.uniform(1.0, 3.0))
                
                event_data = self.scrape_event(url)
                if event_data:
                    # Validate the event date is within our range
                    event_date = event_data.get('Date')
                    if event_date:
                        if start_date <= event_date <= end_date:
                            events.append(event_data)
                        else:
                            print(f"Skipping event outside date range: {event_data.get('Name')} on {event_date}")
                    else:
                        # If no date, still include the event as it might be relevant
                        events.append(event_data)
            
            print(f"Successfully scraped {len(events)} events")
            return events
            
        except Exception as e:
            print(f"Error fetching events: {e}")
            return []
    
    def fetch_weekend_events(self) -> List[Dict]:
        """
        Fetch weekend events from the Charleston Weekend Events page
        
        Returns:
            List of event dictionaries
        """
        try:
            soup = get_soup(self.weekend_events_url)
            if not soup:
                print(f"Error fetching weekend events page: {self.weekend_events_url}")
                return []
                
            # Extract content
            content_elem = soup.select_one('article') or soup.select_one('.entry-content')
            if not content_elem:
                print("Could not find content element on weekend events page")
                return []
                
            content_text = content_elem.text
            
            # Extract events from the text
            events = self._extract_events_from_text(content_text, self.weekend_events_url)
            
            return events
            
        except Exception as e:
            print(f"Error fetching weekend events: {e}")
            return []
    
    def _extract_events_from_text(self, text: str, source_url: str) -> List[Dict]:
        """
        Extract events from text content
        
        Args:
            text: Text content containing event information
            source_url: URL of the source page
            
        Returns:
            List of event dictionaries
        """
        events = []
        
        # Look for date patterns
        date_matches = list(re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?', text))
        
        # If no dates found, return empty list
        if not date_matches:
            return []
            
        # Get current year for filling in missing year values
        current_year = datetime.now().year
            
        # Process each date match
        for i, match in enumerate(date_matches):
            try:
                # Get the date
                month, day, year = match.groups()
                # If year is missing, use current year
                year = year or str(current_year)
                
                try:
                    date_obj = datetime.strptime(f"{month} {day}, {year}", "%B %d, %Y")
                    # If the date is more than 6 months in the past, it's probably next year
                    if (datetime.now() - date_obj).days > 180:
                        date_obj = date_obj.replace(year=date_obj.year + 1)
                    
                    date_str = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    print(f"Invalid date: {month} {day}, {year}")
                    continue
                
                # Get content after this date until the next date or end of text
                if i < len(date_matches) - 1:
                    content = text[match.end():date_matches[i+1].start()]
                else:
                    content = text[match.end():]
                    
                # Split the content into paragraphs
                paragraphs = [p.strip() for p in content.split('\n') if p.strip()]
                
                # Process each paragraph as a potential event
                for j, paragraph in enumerate(paragraphs):
                    # Skip short paragraphs
                    if len(paragraph) < 20:
                        continue
                        
                    # Extract location - often in format "at Location" or "at the Location"
                    location = None
                    location_match = re.search(r'at\s+(?:the\s+)?([A-Z][^\.]+)', paragraph)
                    if location_match:
                        location = location_match.group(1).strip()
                        
                    # Extract time
                    time_match = re.search(r'(\d{1,2}(?::\d{2})?\s*[ap]m)', paragraph, re.IGNORECASE)
                    time_str = time_match.group(1) if time_match else ""
                    
                    # Extract title - use the first sentence or first 100 characters
                    title = paragraph.split('.')[0]
                    if len(title) > 100:
                        title = title[:100] + "..."
                        
                    events.append({
                        'Name': title,
                        'Date': date_str,
                        'Time': time_str,
                        'Location': location,
                        'Description': paragraph,
                        'URL': source_url,
                        'Image_URL': "",
                        'Source': self.source_name
                    })
                    
            except Exception as e:
                print(f"Error processing date match: {e}")
                
        return events
    
    def scrape_event(self, event_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape details for a single event
        
        Args:
            event_url: URL of the event page
            
        Returns:
            Dictionary with event data or None if scraping failed
        """
        # Check if event_url is actually a dictionary (already parsed event data)
        if isinstance(event_url, dict):
            print(f"Event already parsed: {event_url.get('Name', 'Unknown')}")
            return event_url
            
        print(f"Scraping potential event: {event_url}")
        
        try:
            # Extract date from URL if it has occ_dtstart parameter
            date_text = None
            time_text = None
            
            # Check for date in URL (format: occ_dtstart=YYYY-MM-DDThh:mm)
            if 'occ_dtstart=' in event_url:
                date_match = re.search(r'occ_dtstart=(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})', event_url)
                if date_match:
                    date_text = date_match.group(1)
                    hour = int(date_match.group(2).split(':')[0])
                    minutes = date_match.group(2).split(':')[1]
                    am_pm = 'AM' if hour < 12 else 'PM'
                    if hour > 12:
                        hour -= 12
                    time_text = f"{hour}:{minutes} {am_pm}"
            
            soup = get_soup(event_url)
            if not soup:
                print(f"Error fetching page: {event_url}")
                return None
                
            # For events, we don't need to check if it's an event page - we know it is from the URL
            # Just get the event details
            # First get the event name (title)
            name = None
            for selector in ['h1.entry-title', 'h1', '.post-title', '.title', '.mp-event-name']:
                element = soup.select_one(selector)
                if element:
                    name = clean_text(element.text)
                    break
            
            # If we still don't have a name, use the page title
            if not name:
                title_elem = soup.find('title')
                if title_elem:
                    name = clean_text(title_elem.text).replace(" - Holy City Sinner", "")
            
            if not name:
                # If all else fails, try to extract name from URL
                url_name = event_url.split('/events/')[1].split('/?')[0] if '/events/' in event_url else ""
                if url_name:
                    # Convert hyphen-case to title case
                    name = ' '.join(word.capitalize() for word in url_name.replace('-', ' ').split())
                else:
                    name = "Unknown Event"
            
            # Get the description
            description = None
            for selector in ['.entry-content', '.post-content', 'article p', '.mp-event-description']:
                element = soup.select_one(selector)
                if element:
                    description = clean_text(element.text)
                    break
                
            if not description:
                # Try to find paragraphs in the main content
                paragraphs = soup.select('p')
                if paragraphs:
                    # Filter out short paragraphs and navigation elements
                    content_paragraphs = [clean_text(p.text) for p in paragraphs if len(clean_text(p.text)) > 100]
                    if content_paragraphs:
                        description = " ".join(content_paragraphs)
                    elif paragraphs:
                        description = clean_text(paragraphs[0].text)
            
            # Extract location information
            location = None
            
            # Look for venue information in specific selectors
            for selector in ['.event-venue', '.mp-event-location', '.location', '.venue']:
                element = soup.select_one(selector)
                if element:
                    location = clean_text(element.text)
                    break
            
            # If no location found, search in content
            if not location and description:
                venue_patterns = [
                    r'(?:at|venue|location):\s*([^\.]+)',
                    r'(?:at|venue|location)[:\s]+([^\.]+)',
                    r'(?:will take place|held) at\s+([^\.]+)',
                    r'(?:will be|is|at) (?:the|in|at)\s+([A-Z][^\.]+)'  # Capitalized venue names
                ]
                
                for pattern in venue_patterns:
                    match = re.search(pattern, description, re.IGNORECASE)
                    if match:
                        location = clean_text(match.group(1))
                        # Limit to reasonable length for a venue name
                        if len(location) > 100:
                            location = location[:100]
                        break
            
            # If we don't already have date/time from URL
            if not date_text and description:
                # Look for date patterns
                date_patterns = [
                    # Month name followed by day and optional year
                    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?',
                    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?',
                    # Day followed by month name
                    r'(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?(January|February|March|April|May|June|July|August|September|October|November|December)(?:,?\s+(\d{4}))?',
                    # MM/DD/YYYY
                    r'(\d{1,2})/(\d{1,2})/(\d{4})',
                    # YYYY-MM-DD
                    r'(\d{4})-(\d{1,2})-(\d{1,2})'
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, description, re.IGNORECASE)
                    if match:
                        groups = match.groups()
                        current_year = datetime.now().year
                        
                        # Process according to which pattern matched
                        if len(groups) == 3 and groups[0] in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'] or groups[0] in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']:
                            # First pattern - Month day, year
                            month, day, year = groups
                            year = year or current_year  # Use current year if not specified
                            month_num = datetime.strptime(month[:3], '%b').month
                            date_text = f"{year}-{month_num:02d}-{int(day):02d}"
                        elif len(groups) == 3 and groups[1] in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']:
                            # Third pattern - Day month, year
                            day, month, year = groups
                            year = year or current_year  # Use current year if not specified
                            month_num = datetime.strptime(month[:3], '%b').month
                            date_text = f"{year}-{month_num:02d}-{int(day):02d}"
                        elif len(groups) == 3 and all(g and g.isdigit() for g in groups):
                            if '/' in match.group(0):
                                # MM/DD/YYYY
                                month, day, year = groups
                                date_text = f"{year}-{int(month):02d}-{int(day):02d}"
                            else:
                                # YYYY-MM-DD
                                year, month, day = groups
                                date_text = f"{year}-{int(month):02d}-{int(day):02d}"
                        break
            
            # If we don't already have time from URL
            if not time_text and description:
                # Look for time patterns
                time_patterns = [
                    r'(\d{1,2}:\d{2}\s*[ap]m)',
                    r'(\d{1,2}\s*[ap]m)',
                    r'(?:at|from|starting at)\s+(\d{1,2}(?::\d{2})?\s*[ap]m)'
                ]
                for pattern in time_patterns:
                    match = re.search(pattern, description, re.IGNORECASE)
                    if match:
                        time_text = clean_text(match.group(1))
                        break
            
            # Get the event image
            image_url = None
            for selector in ['.wp-post-image', 'article img', '.post-thumbnail img', '.event-image img', '.mp-event-image img']:
                element = soup.select_one(selector)
                if element and element.has_attr('src'):
                    image_url = element['src']
                    # Make URL absolute if needed
                    image_url = self._make_absolute_url(image_url)
                    break
                    
            # If no specific image found, try to get any relevant image
            if not image_url:
                images = soup.select('img')
                for img in images:
                    if img.has_attr('src') and not 'logo' in img['src'].lower() and not 'icon' in img['src'].lower():
                        image_url = self._make_absolute_url(img['src'])
                        break
            
            # Create event dict with fields matching import_hcs_events.py expectations
            event = {
                'Name': name,
                'Description': description,
                'Date': date_text,
                'Time': time_text,
                'Location': location,
                'URL': event_url,
                'Image_URL': image_url,
                'Source': self.source_name
            }
            
            # Only return if we have the minimum required fields
            if name and date_text:  # For Holy City Sinner events, we should have dates
                return event
            else:
                print(f"Skipping incomplete event data from: {event_url}")
                return None
            
        except Exception as e:
            print(f"Error scraping event {event_url}: {e}")
            return None
    
    def _make_absolute_url(self, url: str) -> str:
        """
        Make a URL absolute by adding the base URL if needed
        
        Args:
            url: URL to make absolute
            
        Returns:
            Absolute URL
        """
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"{self.base_url}{url}"
        else:
            return f"{self.base_url}/{url}"
    
    def save_events_to_csv(self, events: List[Dict], filename: str = 'hcs_events.csv'):
        """
        Save events to a CSV file
        
        Args:
            events: List of event dictionaries
            filename: Name of the CSV file to create
            
        Returns:
            True if successful, False otherwise
        """
        if not events:
            print("No events to save")
            return False
            
        try:
            # Get the project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            # Create data directory if it doesn't exist
            data_dir = os.path.join(project_root, 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Set the full path for the CSV file
            csv_path = os.path.join(data_dir, filename)
            
            # Convert events to DataFrame and save to CSV
            df = pd.DataFrame(events)
            df.to_csv(csv_path, index=False)
            
            print(f"Successfully saved {len(events)} events to {csv_path}")
            return True
            
        except Exception as e:
            print(f"Error saving events to CSV: {e}")
            return False


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test the scraper
    scraper = HolyCitySinnerScraper()
    events = scraper.get_all_events()
    
    print(f"Found {len(events)} total events")
    for i, event in enumerate(events[:5]):  # Print the first 5 events
        print(f"Event {i+1}:")
        print(f"Name: {event.get('Name')}")
        print(f"Date: {event.get('Date')}")
        print(f"Location: {event.get('Location')}")
        print(f"URL: {event.get('URL')}")
        print("-" * 50)
    
    # Save events to CSV
    scraper.save_events_to_csv(events) 