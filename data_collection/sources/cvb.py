"""
Charleston CVB Scraper

This module implements a scraper for Charleston CVB events.
"""

import re
import time
import random
import datetime
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any, Union

# Import from parent package
from data_collection.scrapers import BaseEventScraper
from data_collection.utils import (
    clean_text,
    get_soup,
    get_headers
)


class CharlestonCVBScraper(BaseEventScraper):
    """Scraper for Charleston CVB events"""
    
    def __init__(self, db_connection=None):
        """Initialize the scraper"""
        super().__init__(db_connection)
        self.base_url = "https://www.charlestoncvb.com/events/"
        self.search_url = "https://www.charlestoncvb.com/events/search.php"
        self.headers = get_headers()
        
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "Charleston CVB"
    
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30) -> List[Dict]:
        """Get events for a specific date range"""
        
        # If no dates provided, use next 30 days
        if not start_date:
            start_date = datetime.datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            # Calculate end date
            end_date = (datetime.datetime.now() + datetime.timedelta(days=days)).strftime('%Y-%m-%d')
            
        print(f"Fetching events from {start_date} to {end_date}")
        
        # Prepare the search form data
        form_data = {
            'start_date': start_date,
            'end_date': end_date,
            'Search': 'Search'
        }
        
        # Make the search request
        try:
            response = requests.post(self.search_url, data=form_data, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching events: {response.status_code}")
                return []
                
            # Parse the search results
            soup = BeautifulSoup(response.text, 'html.parser')
            event_links = []
            
            # Find event links - they are in listing-card__title-link classes
            for link in soup.select('.listing-card__title-link'):
                event_url = link.get('href')
                if event_url and '/events/' in event_url:
                    # Make the URL absolute if it's relative
                    event_url = self._make_absolute_url(event_url)
                    event_links.append(event_url)
            
            print(f"Found {len(event_links)} events")
            
            # If no events found, try alternate scraping approach
            if len(event_links) == 0:
                print("Trying alternate event search approach...")
                # Try browsing directly to the events page
                response = requests.get(self.base_url, headers=self.headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Look for any event listings on the main events page
                    for link in soup.select('a'):
                        href = link.get('href')
                        if href and '/events/' in href and 'search.php' not in href:
                            # Make the URL absolute if it's relative
                            href = self._make_absolute_url(href)
                            if href not in event_links:
                                event_links.append(href)
                    
                    print(f"Found {len(event_links)} events using alternate method")
            
            # Scrape each event
            event_data = []
            for url in event_links:
                # Add delay to avoid being blocked
                time.sleep(random.uniform(1.0, 3.0))
                
                event = self.scrape_event(url)
                if event:
                    event_data.append(event)
            
            print(f"Successfully scraped {len(event_data)} CVB events")
            return event_data
        
        except Exception as e:
            print(f"Error fetching events: {e}")
            # Provide a fallback with direct navigation to some known event paths
            print("Using fallback event scraping method...")
            
            # Directly navigate to known sections that typically list events
            fallback_urls = [
                "https://www.charlestoncvb.com/events/",
                "https://www.charlestoncvb.com/events/festivals/",
                "https://www.charlestoncvb.com/events/arts-and-culture/"
            ]
            
            event_links = []
            for url in fallback_urls:
                try:
                    response = requests.get(url, headers=self.headers)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        for link in soup.select('a'):
                            href = link.get('href')
                            if href and '/events/' in href and 'search.php' not in href:
                                # Make the URL absolute if it's relative
                                href = self._make_absolute_url(href)
                                if href not in event_links:
                                    event_links.append(href)
                except Exception as inner_e:
                    print(f"Error with fallback URL {url}: {inner_e}")
            
            print(f"Found {len(event_links)} events using fallback method")
            
            # Scrape each event
            event_data = []
            for url in event_links:
                # Add delay to avoid being blocked
                time.sleep(random.uniform(1.0, 3.0))
                
                event = self.scrape_event(url)
                if event:
                    event_data.append(event)
            
            return event_data
    
    def scrape_event(self, event_url: str) -> Optional[Dict[str, Any]]:
        """Scrape details for a single event"""
        print(f"Scraping event: {event_url}")
        
        # Add delay to avoid being blocked
        time.sleep(random.uniform(1.0, 3.0))
        
        try:
            response = requests.get(event_url, headers=self.headers)
            if response.status_code != 200:
                print(f"Error fetching event: {response.status_code}")
                return None
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get event name (from title)
            event_name = None
            for selector in ['.detail-header__title', 'h1', '.event-title', '.title']:
                element = soup.select_one(selector)
                if element:
                    event_name = clean_text(element.text)
                    break
            
            # If we still don't have a name, use the page title
            if not event_name:
                title_elem = soup.find('title')
                if title_elem:
                    event_name = clean_text(title_elem.text).replace(" | Explore Charleston", "")
                else:
                    event_name = "Unknown Event"
            
            # Get the description
            description = None
            for selector in ['.detail-tabs__content', '.event-description', '.description', 'article p']:
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
                    else:
                        description = clean_text(paragraphs[0].text)
            
            # Extract address/location
            location = "Unknown Location"
            venue_name = "Unknown Venue"
            
            # Try multiple selectors for location
            for selector in ['.detail-top__address', '.event-location', '.location', 'address']:
                element = soup.select_one(selector)
                if element:
                    location = clean_text(element.text)
                    break
                    
            # Get venue name from various possible elements
            for selector in ['.detail-header__subtitle a', '.venue-name', '.location-name']:
                element = soup.select_one(selector)
                if element:
                    venue_name = clean_text(element.text)
                    break
            
            # Get event date and time
            date_text = ""
            time_text = ""
            
            # Try multiple date selectors
            for selector in ['.detail-top__date', '.event-date', '.date', 'time']:
                element = soup.select_one(selector)
                if element:
                    date_text = clean_text(element.text)
                    break
                    
            # If no date found, try to extract from the content
            if not date_text:
                # Look for date patterns
                date_patterns = [
                    # Month name followed by day and optional year
                    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?',
                    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{4}))?',
                    # MM/DD/YYYY
                    r'(\d{1,2})/(\d{1,2})/(\d{4})',
                    # YYYY-MM-DD
                    r'(\d{4})-(\d{1,2})-(\d{1,2})'
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, description or "", re.IGNORECASE)
                    if match:
                        groups = match.groups()
                        # Process according to which pattern matched
                        if len(groups) == 3 and groups[0] in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'] or groups[0] in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']:
                            # First pattern - Month day, year
                            month, day, year = groups
                            year = year or datetime.datetime.now().year  # Use current year if not specified
                            date_text = f"{year}-{datetime.datetime.strptime(month[:3], '%b').month:02d}-{int(day):02d}"
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
            
            # Get time from various selectors
            for selector in ['.detail-top__time', '.event-time', '.time']:
                element = soup.select_one(selector)
                if element:
                    time_text = clean_text(element.text)
                    break
                    
            # If no time found, try to extract from content
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
            
            # Event image
            image_url = ""
            for selector in ['.detail-image img', '.event-image img', 'img.event', '.featured-image img']:
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
                    
            # Create event dict with standard field names to match the database schema
            event = {
                'Name': event_name,
                'Description': description,
                'Date': date_text,
                'Time': time_text,
                'Location': location,
                'URL': event_url,
                'Image_URL': image_url,
                'Source': self.source_name
            }
            
            return event
            
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
            return f"{self.base_url.rstrip('/')}{url}"
        else:
            return f"{self.base_url.rstrip('/')}/{url}"


if __name__ == "__main__":
    # Test the scraper
    scraper = CharlestonCVBScraper()
    events = scraper.get_events_for_date_range()
    
    print(f"Found {len(events)} total events")
    for i, event in enumerate(events[:5]):  # Print the first 5 events
        print(f"Event {i+1}:")
        print(f"Name: {event.get('Name')}")
        print(f"Date: {event.get('Date')}")
        print(f"Location: {event.get('Location')}")
        print(f"URL: {event.get('URL')}")
        print("-" * 50) 