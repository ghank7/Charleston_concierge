"""
Lowcountry Local First Scraper

This module provides a scraper for collecting business data from the Lowcountry Local First
member directory at https://www.lowcountrylocalfirst.org/member-directory
"""

import os
import json
import time
import requests
import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import random
from urllib.parse import urljoin
import csv

from data_collection.scrapers import BaseEventScraper

# Setup logging
logger = logging.getLogger(__name__)

class LowcountryLocalFirstScraper(BaseEventScraper):
    """Scraper for collecting business data from Lowcountry Local First member directory"""
    
    # Base URL for the directory
    BASE_URL = "https://www.lowcountrylocalfirst.org/member-directory"
    
    # The API endpoint that returns business listings (found in the directory.js)
    MEMBER_LIST_API = "https://www.lowcountrylocalfirst.org/members/member-list-html"
    
    # Categories available in the directory
    CATEGORIES = [
        "Advertising, Marketing and Media",
        "Architecture, Construction and Design",
        "Business Supplies",
        "Community Business Academy Graduate",
        "Education",
        "Entertainment and Arts",
        "Event Planners and Venues",
        "Farms",
        "Financial Institutions and Services",
        "Food and Beverage",
        "Green Businesses",
        "Health, Beauty and Wellness",
        "Home and Garden",
        "IT and Web Services",
        "Lodging and Transportation",
        "Manufacturing and Product Development",
        "Nonprofit Organizations",
        "Personal Services",
        "Pet Services and Supplies",
        "Professional Services",
        "Real Estate",
        "Retail, Gifts, Clothing and Accessories"
    ]
    
    def __init__(self, db_connection=None):
        """
        Initialize the Lowcountry Local First Scraper
        
        Args:
            db_connection: Optional database connection
        """
        super().__init__(db_connection)
        self.session = requests.Session()
        # Add common headers to mimic a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': self.BASE_URL,
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        })
    
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "LowcountryLocalFirst"
    
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30) -> List[str]:
        """
        This method is implemented to satisfy the BaseEventScraper ABC,
        but for business data, we'll just return the category list as our "urls" to scrape
        """
        return self.CATEGORIES
    
    def fetch_directory_data(self, page=1, category=None) -> Dict:
        """
        Fetch the business listings via the AJAX API endpoint
        
        Args:
            page: Page number to fetch
            category: Optional category to filter by
            
        Returns:
            JSON response containing business listings
        """
        try:
            logger.info(f"Fetching directory data for page {page} with category filter: {category}")
            
            # The API expects a POST request with specific parameters
            data = {
                'directoryID': '1',  # Default directory ID based on the website
                'pageNumber': page,
                'searchText': '',
            }
            
            # Add category filter if specified
            if category:
                member_type_id = self.get_category_id(category)
                if member_type_id:
                    data['memberTypeIDs'] = member_type_id
            
            # Make the POST request to the API
            response = self.session.post(
                self.MEMBER_LIST_API,
                data=data
            )
            
            # Add a delay to avoid overwhelming the server
            time.sleep(random.uniform(1.0, 2.0))
            
            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse JSON response: {response.text[:100]}...")
                    return {"Status": "Error", "Members": [], "TotalCount": 0}
            else:
                logger.error(f"Error fetching directory data: {response.status_code}")
                return {"Status": "Error", "Members": [], "TotalCount": 0}
                
        except Exception as e:
            logger.error(f"Exception during directory data fetch: {str(e)}")
            return {"Status": "Error", "Members": [], "TotalCount": 0}
    
    def get_category_id(self, category_name: str) -> Optional[str]:
        """
        Get the category ID based on the category name.
        This would need to be implemented by finding the mapping on the website.
        For now, return None as we're not filtering by category in this implementation.
        """
        # This would need to be implemented by mapping the category names to their IDs
        # which would require analyzing the select options on the actual website
        return None
    
    def parse_business_from_json(self, business_json: Dict) -> Dict:
        """
        Parse business information from the JSON response
        
        Args:
            business_json: JSON object representing a business
            
        Returns:
            Dictionary with normalized business data
        """
        try:
            # Extract basic info
            business = {
                'name': business_json.get('Name', ''),
                'description': business_json.get('Description', ''),
                'category': business_json.get('MemberTypeDisplay', ''),
                'website': business_json.get('Website', ''),
                'email': business_json.get('Email', ''),
                'phone': business_json.get('Phone', ''),
                'location': '',
                'image_url': business_json.get('LogoUrl', ''),
                'source': self.source_name
            }
            
            # Extract address info
            shipping_address = business_json.get('ShippingAddress', {})
            if shipping_address:
                address_parts = []
                if shipping_address.get('Line1'):
                    address_parts.append(shipping_address.get('Line1', ''))
                if shipping_address.get('Line2'):
                    address_parts.append(shipping_address.get('Line2', ''))
                if shipping_address.get('City'):
                    address_parts.append(shipping_address.get('City', ''))
                if shipping_address.get('State'):
                    address_parts.append(shipping_address.get('State', ''))
                if shipping_address.get('PostalCode'):
                    address_parts.append(shipping_address.get('PostalCode', ''))
                
                business['location'] = ', '.join(filter(None, address_parts))
            
            return business
            
        except Exception as e:
            logger.error(f"Error parsing business from JSON: {e}")
            return {
                'name': business_json.get('Name', 'Unknown'),
                'source': self.source_name
            }
    
    def scrape_event(self, category: str) -> Optional[Dict[str, Any]]:
        """
        Scrape businesses for a given category
        
        Args:
            category: Category to scrape
            
        Returns:
            Dictionary containing the category and list of businesses
        """
        try:
            logger.info(f"Scraping businesses for category: {category}")
            
            # Fetch the first page and get total count
            response_data = self.fetch_directory_data(page=1, category=category)
            
            if response_data.get('Status') != 'OK':
                logger.error(f"API returned error status: {response_data.get('Status')}")
                return None
            
            total_count = response_data.get('TotalCount', 0)
            page_size = 10  # Default page size used by the website
            total_pages = (total_count + page_size - 1) // page_size
            
            logger.info(f"Found {total_count} businesses across {total_pages} pages")
            
            businesses = []
            
            # Parse businesses from the first page
            for business_json in response_data.get('Members', []):
                business = self.parse_business_from_json(business_json)
                businesses.append(business)
            
            # Fetch and parse remaining pages
            for page in range(2, total_pages + 1):
                logger.info(f"Fetching page {page} of {total_pages}")
                response_data = self.fetch_directory_data(page=page, category=category)
                
                if response_data.get('Status') != 'OK':
                    logger.error(f"API returned error status on page {page}: {response_data.get('Status')}")
                    continue
                
                for business_json in response_data.get('Members', []):
                    business = self.parse_business_from_json(business_json)
                    businesses.append(business)
                
                # Add a longer delay between pages to be respectful
                time.sleep(random.uniform(2.0, 3.0))
            
            logger.info(f"Found {len(businesses)} businesses in category {category}")
            
            return {
                "category": category,
                "businesses": businesses
            }
            
        except Exception as e:
            logger.error(f"Error scraping businesses for category {category}: {e}")
            return None
    
    def scrape_businesses(self, categories=None, max_businesses=None, max_pages=None, save_to_file=True):
        """Main method to scrape all businesses from the directory"""
        logger.info("Starting scraping process for Lowcountry Local First businesses")
        
        # If specific categories are provided, validate them
        if categories:
            logger.info(f"Filtering by categories: {', '.join(categories)}")
            valid_categories = [c for c in categories if c in self.CATEGORIES]
            if not valid_categories:
                logger.warning(f"No valid categories specified. Valid options are: {', '.join(self.CATEGORIES)}")
                return []
            categories_to_scrape = valid_categories
        else:
            categories_to_scrape = self.CATEGORIES
        
        all_businesses = []
        businesses_saved = 0
        businesses_count = 0
        
        # First, get all businesses without category filters to get a full list
        uncategorized_response = self.fetch_directory_data(page=1)
        
        if uncategorized_response.get('Status') != 'OK':
            logger.error(f"Failed to get initial business list: {uncategorized_response.get('Status')}")
        else:
            total_count = uncategorized_response.get('TotalCount', 0)
            page_size = 10  # Default page size used by the website
            total_pages = min((total_count + page_size - 1) // page_size, max_pages or float('inf'))
            
            logger.info(f"Found {total_count} total businesses across approximately {total_pages} pages")
            
            # Process the first page
            for business_json in uncategorized_response.get('Members', []):
                # Check if we've reached the maximum number of businesses
                if max_businesses and businesses_count >= max_businesses:
                    logger.info(f"Reached maximum number of businesses ({max_businesses})")
                    break
                
                business = self.parse_business_from_json(business_json)
                all_businesses.append(business)
                businesses_count += 1
            
            # Process remaining pages
            for page in range(2, int(total_pages) + 1):
                # Check if we've reached the maximum number of businesses
                if max_businesses and businesses_count >= max_businesses:
                    logger.info(f"Reached maximum number of businesses ({max_businesses})")
                    break
                
                logger.info(f"Fetching page {page} of {total_pages}")
                response_data = self.fetch_directory_data(page=page)
                
                if response_data.get('Status') != 'OK':
                    logger.error(f"API returned error status on page {page}: {response_data.get('Status')}")
                    continue
                
                for business_json in response_data.get('Members', []):
                    # Check if we've reached the maximum number of businesses
                    if max_businesses and businesses_count >= max_businesses:
                        logger.info(f"Reached maximum number of businesses ({max_businesses})")
                        break
                    
                    business = self.parse_business_from_json(business_json)
                    all_businesses.append(business)
                    businesses_count += 1
                
                # Add a longer delay between pages to be respectful
                time.sleep(random.uniform(2.0, 3.0))
        
        logger.info(f"Completed scraping. Found {len(all_businesses)} businesses total.")
        
        # If we have a database connection, save businesses
        if self.db_connection:
            businesses_saved = self.save_to_database(all_businesses)
            logger.info(f"Successfully saved {businesses_saved} businesses to database.")
        
        # Save to file if requested
        if save_to_file:
            self.save_to_json(all_businesses)
            self.save_to_csv(all_businesses)
            
        return all_businesses
    
    def save_to_csv(self, businesses: List[Dict], filename: str = "lowcountry_local_first_businesses.csv") -> bool:
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
            
            # Prepare data for CSV
            csv_data = []
            for business in businesses:
                csv_data.append({
                    'Name': business.get('name', ''),
                    'Address': business.get('location', ''),
                    'Phone': business.get('phone', ''),
                    'Email': business.get('email', ''),
                    'Website': business.get('website', ''),
                    'Category': business.get('category', ''),
                    'Description': business.get('description', ''),
                    'Image_URL': business.get('image_url', ''),
                    'Source': business.get('source', self.source_name)
                })
            
            # Save to CSV
            df = pd.DataFrame(csv_data)
            df.to_csv(csv_path, index=False)
            
            logger.info(f"Successfully saved {len(businesses)} businesses to {csv_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving businesses to CSV: {e}")
            return False
    
    def save_to_database(self, businesses: List[Dict]) -> int:
        """
        Save scraped businesses to the database
        
        Args:
            businesses: List of business dictionaries
            
        Returns:
            Number of businesses saved
        """
        if not self.db_connection:
            logger.warning("No database connection available. Skipping database save.")
            return 0
        
        count = 0
        for business in businesses:
            try:
                # Check if add_event_from_dict method exists for compatibility
                if hasattr(self.db_connection, 'add_event_from_dict'):
                    self.db_connection.add_event_from_dict(business)
                    count += 1
                # Legacy method - add directly to businesses table
                elif hasattr(self.db_connection, 'add_business'):
                    self.db_connection.add_business(
                        name=business.get("name", ""),
                        description=business.get("description", ""),
                        location=business.get("location", ""),
                        website=business.get("website", ""),
                        category=business.get("category", ""),
                        image_url=business.get("image_url", ""),
                        email=business.get("email", ""),
                        phone=business.get("phone", "")
                    )
                    count += 1
                else:
                    logger.error("No compatible database method found for saving business")
            except Exception as e:
                logger.error(f"Error saving business {business.get('name')} to database: {str(e)}")
        
        return count
    
    def save_to_json(self, businesses, filename="lowcountry_businesses.json"):
        """Save scraped businesses to a JSON file"""
        try:
            # Get the project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            # Create data directory if it doesn't exist
            data_dir = os.path.join(project_root, 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Set the full path for the JSON file
            json_path = os.path.join(data_dir, filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(businesses, f, indent=4, ensure_ascii=False)
                
            logger.info(f"Successfully saved {len(businesses)} businesses to {json_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving businesses to JSON: {e}")
            return False

# Test function to run the scraper directly
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run the scraper
    scraper = LowcountryLocalFirstScraper()
    
    # Scrape a small number of businesses as a test
    test_categories = ["Food and Beverage", "Retail, Gifts, Clothing and Accessories"]
    businesses = scraper.scrape_businesses(categories=test_categories, max_businesses=10)
    
    print(f"Scraped {len(businesses)} businesses")
    
    # Save to CSV
    scraper.save_to_csv(businesses, "lowcountry_local_first_test.csv")
    
    # Print first business as a sample
    if businesses:
        print("\nSample business:")
        first_business = businesses[0]
        for k, v in first_business.items():
            print(f"{k}: {v}") 