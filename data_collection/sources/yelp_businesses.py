"""
Yelp Business Scraper

This module provides a scraper for collecting business data from the Yelp Fusion API
specifically for businesses in Charleston, SC.
"""

import os
import json
import time
import requests
import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union

from data_collection.scrapers import BaseEventScraper

# Setup logging
logger = logging.getLogger(__name__)

class YelpBusinessScraper(BaseEventScraper):
    """Scraper for collecting business data from Yelp Fusion API"""
    
    # Charleston, SC coordinates for search
    CHARLESTON_LAT = 32.7765
    CHARLESTON_LNG = -79.9311
    
    # Categories of interest for Charleston
    CATEGORIES = [
        "restaurants", "bars", "nightlife", "food", "arts", 
        "hotelstravel", "tours", "localflavor", "shopping", 
        "galleries", "museums", "theater", "musicvenues",
        "landmarks", "beaches", "parks", "active"
    ]
    
    def __init__(self, db_connection=None, api_key=None):
        """
        Initialize the Yelp Business Scraper
        
        Args:
            db_connection: Optional database connection
            api_key: Yelp API key. If not provided, will try to get from environment variable YELP_API_KEY
        """
        super().__init__(db_connection)
        self.api_key = api_key or os.environ.get('YELP_API_KEY')
        if not self.api_key:
            raise ValueError("Yelp API key is required. Provide it as an argument or set YELP_API_KEY environment variable.")
        
        self.base_url = "https://api.yelp.com/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
    
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "Yelp"
    
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30) -> List[str]:
        """
        This method is implemented to satisfy the BaseEventScraper ABC,
        but for businesses, we'll just return the category list as our "urls" to scrape
        """
        return self.CATEGORIES
    
    def search_businesses(self, category, location="Charleston, SC", limit=50, offset=0) -> List[Dict]:
        """
        Search for businesses in a specific category and location
        
        Args:
            category: Business category to search for
            location: Location to search in
            limit: Maximum number of results to return (max 50 per request)
            offset: Offset for pagination
            
        Returns:
            List of business data dictionaries
        """
        endpoint = f"{self.base_url}/businesses/search"
        
        params = {
            "categories": category,
            "location": location,
            "latitude": self.CHARLESTON_LAT,
            "longitude": self.CHARLESTON_LNG,
            "limit": min(limit, 50),  # Yelp API max is 50 per request
            "offset": offset,
            "sort_by": "best_match",
            "radius": 16000  # 10 miles in meters
        }
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            
            # Handle API rate limits
            if response.status_code == 429:  # Too Many Requests
                logger.warning("Rate limit hit, sleeping for 10 seconds")
                time.sleep(10)
                return self.search_businesses(category, location, limit, offset)
                
            # Check for successful response
            if response.status_code == 200:
                data = response.json()
                return data.get("businesses", [])
            else:
                logger.error(f"Error searching businesses: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Exception during Yelp API call: {e}")
            return []
    
    def get_business_details(self, business_id: str) -> Optional[Dict]:
        """
        Get detailed information for a specific business
        
        Args:
            business_id: Yelp business ID
            
        Returns:
            Business details dictionary or None if error
        """
        endpoint = f"{self.base_url}/businesses/{business_id}"
        
        try:
            response = requests.get(endpoint, headers=self.headers)
            
            # Handle API rate limits
            if response.status_code == 429:  # Too Many Requests
                logger.warning("Rate limit hit, sleeping for 10 seconds")
                time.sleep(10)
                return self.get_business_details(business_id)
                
            # Check for successful response
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error getting business details: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Exception during Yelp API call: {e}")
            return None
    
    def get_business_reviews(self, business_id: str, limit=3) -> List[Dict]:
        """
        Get reviews for a specific business
        
        Args:
            business_id: Yelp business ID
            limit: Maximum number of reviews to return
            
        Returns:
            List of review dictionaries
        """
        endpoint = f"{self.base_url}/businesses/{business_id}/reviews"
        
        params = {
            "limit": min(limit, 3)  # Yelp API max is 3 per request
        }
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            
            # Handle API rate limits
            if response.status_code == 429:  # Too Many Requests
                logger.warning("Rate limit hit, sleeping for 10 seconds")
                time.sleep(10)
                return self.get_business_reviews(business_id, limit)
                
            # Check for successful response
            if response.status_code == 200:
                data = response.json()
                return data.get("reviews", [])
            else:
                logger.error(f"Error getting business reviews: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Exception during Yelp API call: {e}")
            return []
    
    def scrape_event(self, category: str) -> Optional[Dict[str, Any]]:
        """
        Scrape businesses for a given category
        Note: We're overriding the scrape_event method to handle business categories
        instead of individual event URLs.
        
        Args:
            category: Business category to scrape
            
        Returns:
            Dictionary with category and list of businesses or None if error
        """
        try:
            logger.info(f"Scraping businesses in category: {category}")
            
            # Fetch businesses in this category
            all_businesses = []
            offset = 0
            limit = 50  # Max per request
            
            # Paginate through results (Yelp limits to 50 per request, max 1000 total)
            while True:
                businesses = self.search_businesses(category=category, limit=limit, offset=offset)
                if not businesses:
                    break
                    
                all_businesses.extend(businesses)
                offset += limit
                
                # Yelp API limits to 1000 results max
                if offset >= 1000 or len(businesses) < limit:
                    break
                    
                # Add a delay to avoid rate limiting
                time.sleep(1)
            
            logger.info(f"Found {len(all_businesses)} businesses in category {category}")
            
            # Fetch details and reviews for each business
            enriched_businesses = []
            for business in all_businesses:
                business_id = business.get("id")
                
                # Get detailed business info
                details = self.get_business_details(business_id)
                if details:
                    # Get reviews
                    reviews = self.get_business_reviews(business_id)
                    
                    # Combine data
                    business_data = details
                    business_data["reviews"] = reviews
                    
                    # Format for database storage
                    formatted_business = self._format_business_for_db(business_data, category)
                    enriched_businesses.append(formatted_business)
                    
                # Add a delay to avoid rate limiting
                time.sleep(1)
            
            return {
                "category": category,
                "businesses": enriched_businesses
            }
            
        except Exception as e:
            logger.error(f"Error scraping businesses for category {category}: {e}")
            return None
    
    def _format_business_for_db(self, business: Dict, category: str) -> Dict:
        """
        Format Yelp business data for database storage
        
        Args:
            business: Yelp business data
            category: Primary category being searched
            
        Returns:
            Formatted business dictionary
        """
        # Get all categories
        categories = []
        if "categories" in business:
            categories = [cat["title"] for cat in business["categories"]]
        
        # Get location string
        location = ""
        if "location" in business:
            location_parts = business["location"].get("display_address", [])
            location = ", ".join(location_parts)
        
        # Get photos
        photos = business.get("photos", [])
        image_url = photos[0] if photos else ""
        
        # Get reviews text
        reviews_text = ""
        for review in business.get("reviews", []):
            reviews_text += f"Rating: {review.get('rating', 'N/A')}/5 - {review.get('text', 'No text')}\n"
        
        # Create formatted business dictionary
        return {
            "name": business.get("name", ""),
            "yelp_id": business.get("id", ""),
            "location": location,
            "latitude": business.get("coordinates", {}).get("latitude", 0),
            "longitude": business.get("coordinates", {}).get("longitude", 0),
            "rating": business.get("rating", 0),
            "review_count": business.get("review_count", 0),
            "price": business.get("price", ""),
            "phone": business.get("phone", ""),
            "website": business.get("url", ""),
            "image_url": image_url,
            "categories": categories,
            "primary_category": category,
            "hours": json.dumps(business.get("hours", [])),
            "description": business.get("alias", "").replace("-", " ").title(),
            "reviews": reviews_text,
            "source": self.source_name
        }
    
    def scrape_businesses(self, categories=None) -> List[Dict]:
        """
        Scrape businesses for all or specific categories
        
        Args:
            categories: Optional list of categories to scrape. If None, uses the default categories.
            
        Returns:
            List of business dictionaries
        """
        categories_to_scrape = categories or self.CATEGORIES
        
        all_businesses = []
        for category in categories_to_scrape:
            result = self.scrape_event(category)
            if result and "businesses" in result:
                all_businesses.extend(result["businesses"])
                
        return all_businesses
    
    def save_businesses_to_csv(self, businesses: List[Dict], filename: str = "charleston_yelp_businesses.csv") -> bool:
        """
        Save scraped businesses to a CSV file
        
        Args:
            businesses: List of business dictionaries
            filename: Name of the CSV file to create
            
        Returns:
            True if successful, False otherwise
        """
        if not businesses:
            logger.warning("No businesses to save")
            return False
            
        try:
            # Get the project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            # Create data directory if it doesn't exist
            data_dir = os.path.join(project_root, 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Set the full path for the CSV file
            csv_path = os.path.join(data_dir, filename)
            
            # Convert businesses to DataFrame and save to CSV
            df = pd.DataFrame(businesses)
            
            # Convert categories list to string for CSV storage
            if "categories" in df.columns:
                df["categories"] = df["categories"].apply(lambda x: "|".join(x) if isinstance(x, list) else x)
                
            df.to_csv(csv_path, index=False)
            
            logger.info(f"Successfully saved {len(businesses)} businesses to {csv_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving businesses to CSV: {e}")
            return False
    
    def save_to_database(self, businesses: List[Dict]) -> int:
        """
        Save businesses to the database
        
        Args:
            businesses: List of business dictionaries
            
        Returns:
            Number of businesses saved
        """
        if not self.db_connection:
            logger.warning("No database connection provided, can't save businesses")
            return 0
            
        count = 0
        for business in businesses:
            try:
                # Check if database connection has add_business method
                if hasattr(self.db_connection, "add_business"):
                    # Extract main business details
                    business_id = self.db_connection.add_business(
                        name=business.get("name", ""),
                        location=business.get("location", ""),
                        description=business.get("description", ""),
                        url=business.get("website", ""),
                        website=business.get("website", ""),
                        image_url=business.get("image_url", ""),
                        phone=business.get("phone", ""),
                        email=""  # Yelp API doesn't provide email
                    )
                    
                    # Add categories if available
                    if "categories" in business and hasattr(self.db_connection, "add_business_category"):
                        categories = business["categories"]
                        if isinstance(categories, str):
                            categories = categories.split("|")
                            
                        for category in categories:
                            try:
                                self.db_connection.add_business_category(business_id, category)
                            except Exception as e:
                                logger.error(f"Error adding category {category} to business {business['name']}: {e}")
                    
                    count += 1
                    
                else:
                    logger.warning("Database connection doesn't have an add_business method")
                    break
                    
            except Exception as e:
                logger.error(f"Error saving business {business.get('name', 'Unknown')} to database: {e}")
                
        return count


# Test function to run the scraper directly
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Check if API key is available
    api_key = os.environ.get('YELP_API_KEY')
    if not api_key:
        print("YELP_API_KEY environment variable not set. Please set it to run this scraper.")
        sys.exit(1)
    
    # Create and run the scraper
    scraper = YelpBusinessScraper(api_key=api_key)
    
    # Scrape only a few categories as a test
    test_categories = ["restaurants", "arts", "nightlife"]
    businesses = scraper.scrape_businesses(categories=test_categories)
    
    print(f"Scraped {len(businesses)} businesses")
    
    # Save to CSV
    scraper.save_businesses_to_csv(businesses, "yelp_businesses_test.csv")
    
    # Print first business as a sample
    if businesses:
        print("\nSample business:")
        first_business = businesses[0]
        for k, v in first_business.items():
            print(f"{k}: {v}") 