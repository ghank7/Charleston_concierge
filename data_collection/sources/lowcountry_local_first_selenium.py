import time
import re
import json
import os
import csv
import requests
import random
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from ..selenium_scraper import BaseSeleniumScraper

class LowcountryLocalFirstSeleniumScraper(BaseSeleniumScraper):
    """Scraper for Lowcountry Local First using Selenium with Safari WebDriver."""
    
    def __init__(self, db_connection=None):
        print("Initializing LowcountryLocalFirstSeleniumScraper...")
        super().__init__(db_connection)
        self.base_url = "https://www.lowcountrylocalfirst.org/member-directory"
        self.alternative_urls = [
            "https://www.lowcountrylocalfirst.org/member-directory/",
            "https://www.lowcountrylocalfirst.org/members/",
            "https://www.lowcountrylocalfirst.org/directory/",
            "https://www.lowcountrylocalfirst.org/business-directory/"
        ]
        self.api_endpoint = "/members/directory-customer-list"
        self.directory_id = "670"
        self.categories = {}
        print(f"Scraper initialized with base URL: {self.base_url}")
        
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "LowcountryLocalFirstSelenium"
        
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30):
        """Required method implementation - not used for business scraping"""
        return []
        
    def scrape_event(self, event_url):
        """Required method implementation - not used for business scraping"""
        return None
    
    def _get_categories(self):
        """Get available categories from the directory page."""
        print("Getting categories from directory page...")
        if not self.driver:
            print("Initializing driver and loading page...")
            self.get_page(self.base_url)
            print(f"Page loaded: {self.driver.current_url}")
        
        try:
            # The site appears to have changed - we'll look for any filter UI
            # Check for any filter dropdown or select element
            categories = {}
            
            # Look for various potential selectors
            selectors = [
                "select[name='category']", 
                ".directory-filters select", 
                "#dir-cat",
                ".filter-dropdown"
            ]
            
            print("Searching for category elements with selectors:", selectors)
            for selector in selectors:
                try:
                    print(f"Trying selector: {selector}")
                    category_element = self.wait_for_element(selector, timeout=3)
                    if category_element:
                        print(f"Found category element with selector: {selector}")
                        # Create a Select object for the dropdown if it's a select element
                        if category_element.tag_name.lower() == "select":
                            category_select = Select(category_element)
                            # Get all options
                            for option in category_select.options:
                                if option.get_attribute("value") and option.get_attribute("value") != "-1":
                                    categories[option.text.strip()] = option.get_attribute("value")
                                    print(f"Found category: {option.text.strip()} -> {option.get_attribute('value')}")
                        break
                except Exception as e:
                    print(f"Error trying selector {selector}: {e}")
            
            # If no categories found, use default "All"
            if not categories:
                print("No category dropdown found, using default 'All' category")
                categories["All"] = ""
                
            self.categories = categories
            print(f"Found {len(categories)} categories: {list(categories.keys())}")
            return categories
        except Exception as e:
            print(f"Error getting categories: {e}")
            # Use a default "All" category if we can't find categories
            self.categories = {"All": ""}
            return {"All": ""}
    
    def get_category_id(self, category_name):
        """Get the category ID for a given category name."""
        print(f"Getting category ID for '{category_name}'...")
        if not self.categories:
            self._get_categories()
        
        # Check for direct match
        if category_name in self.categories:
            print(f"Found direct match for category '{category_name}'")
            return self.categories[category_name]
        
        # Try case-insensitive match
        for cat, cat_id in self.categories.items():
            if cat.lower() == category_name.lower():
                print(f"Found case-insensitive match: '{cat}' for '{category_name}'")
                return cat_id
        
        # Try partial match
        matching_cats = [
            (cat, cat_id) for cat, cat_id in self.categories.items()
            if category_name.lower() in cat.lower()
        ]
        
        if matching_cats:
            print(f"Using partial match: '{matching_cats[0][0]}' for '{category_name}'")
            return matching_cats[0][1]
        
        print(f"No category found for '{category_name}', available categories: {list(self.categories.keys())}")
        return None
    
    def _select_category(self, category_id):
        """Select a category in the directory filter."""
        print(f"Selecting category with ID: '{category_id}'...")
        try:
            # Find the category dropdown
            category_select = Select(self.find_element("select#dir-cat"))
            print("Found category dropdown")
            
            # Select the category
            category_select.select_by_value(category_id)
            print(f"Selected category with value: {category_id}")
            
            # Give time for the results to load
            print("Waiting for results to load...")
            time.sleep(3)
            
            return True
        except Exception as e:
            print(f"Error selecting category: {e}")
            return False
    
    def _extract_business_data(self, card_element):
        """Extract business data from a card element."""
        print("Extracting business data using legacy method...")
        try:
            business = {}
            
            # Business name
            name_element = card_element.find_element(By.CSS_SELECTOR, ".listing-title a")
            business["name"] = name_element.text.strip() if name_element else "Unknown"
            
            # Business URL
            business["website"] = name_element.get_attribute("href") if name_element else ""
            
            # Address/Location
            location_element = card_element.find_element(By.CSS_SELECTOR, ".geodir_post_meta .geodir-field-address")
            business["location"] = location_element.text.strip() if location_element else ""
            
            # Phone
            try:
                phone_element = card_element.find_element(By.CSS_SELECTOR, ".geodir_post_meta .geodir-field-phone")
                business["phone"] = phone_element.text.strip() if phone_element else ""
            except NoSuchElementException:
                business["phone"] = ""
            
            # Image
            try:
                img_element = card_element.find_element(By.CSS_SELECTOR, ".geodir-image img")
                business["image_url"] = img_element.get_attribute("src") if img_element else ""
            except NoSuchElementException:
                business["image_url"] = ""
            
            # Category
            try:
                category_element = card_element.find_element(By.CSS_SELECTOR, ".geodir-field-categories")
                business["category"] = category_element.text.strip() if category_element else ""
            except NoSuchElementException:
                business["category"] = ""
            
            # Add source
            business["source"] = "LowcountryLocalFirstSelenium"
            
            print(f"Extracted business: {business['name']}")
            return business
        except Exception as e:
            print(f"Error extracting business data: {e}")
            return None
    
    def _get_businesses_from_page(self):
        """Get businesses from the current page."""
        print("Getting businesses from current page...")
        businesses = []
        
        try:
            # Wait for the business cards to load - try multiple potential selectors
            card_selectors = [
                ".directory-item",
                ".member-card", 
                ".geodir-category-listing",
                ".member-listing",
                ".directory-listing",
                ".listing-row",
                ".business-listing",
                ".llf-member"
            ]
            
            print(f"Searching for business cards with selectors: {card_selectors}")
            # Take screenshot of the current page state
            screenshot_path = f"page_screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            print(f"Taking screenshot of page: {screenshot_path}")
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Screenshot saved to {screenshot_path}")
            except Exception as e:
                print(f"Error saving screenshot: {e}")
            
            # Save page source for debugging
            html_path = f"page_source_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            try:
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                print(f"Page source saved to {html_path}")
            except Exception as e:
                print(f"Error saving page source: {e}")
                
            cards_found = False
            for selector in card_selectors:
                try:
                    print(f"Trying selector: {selector}")
                    # Try with a short timeout for each selector
                    if self.wait_for_element(selector, timeout=3):
                        business_cards = self.find_elements(selector)
                        if business_cards and len(business_cards) > 0:
                            cards_found = True
                            print(f"Found {len(business_cards)} businesses with selector: {selector}")
                            
                            # Extract data from each card
                            for i, card in enumerate(business_cards):
                                try:
                                    print(f"Processing card {i+1}/{len(business_cards)}...")
                                    business_data = self._extract_business_data_dynamic(card)
                                    if business_data:
                                        businesses.append(business_data)
                                        print(f"Successfully extracted business: {business_data.get('name', 'Unknown')}")
                                    else:
                                        print(f"Failed to extract data from card {i+1}")
                                except Exception as e:
                                    print(f"Error processing card {i+1}: {e}")
                            break
                except Exception as e:
                    print(f"Error checking selector {selector}: {e}")
            
            if not cards_found:
                print("No business cards found with any of the selectors")
                # Look for any divs or items that might be business listings
                try:
                    print("Looking for any potential business elements...")
                    elements = self.find_elements("div")
                    print(f"Found {len(elements)} div elements on the page")
                    # Log first 10 div classes to help identify patterns
                    for i, elem in enumerate(elements[:10]):
                        try:
                            class_attr = elem.get_attribute("class")
                            if class_attr:
                                print(f"Div {i+1} classes: {class_attr}")
                        except:
                            pass
                except Exception as e:
                    print(f"Error inspecting page elements: {e}")
            
            print(f"Total businesses found on this page: {len(businesses)}")
            return businesses
        except Exception as e:
            print(f"Error getting businesses from page: {e}")
            return []
    
    def _go_to_next_page(self):
        """Navigate to the next page of results."""
        print("Attempting to go to next page...")
        try:
            # Try different selectors for next page button
            next_page_selectors = [
                ".pagination a.next", 
                "a.next-page", 
                "a[rel='next']",
                ".pagination-next a",
                "a:contains('Next')",
                ".pagination .next a"
            ]
            
            for selector in next_page_selectors:
                try:
                    print(f"Looking for next page button with selector: {selector}")
                    next_button = self.find_element(selector)
                    if next_button and next_button.is_displayed():
                        print("Found next page button, clicking...")
                        next_button.click()
                        time.sleep(3)  # Wait for page to load
                        return True
                except Exception as e:
                    print(f"Error with selector {selector}: {e}")
            
            print("No next page button found or next page is not available")
            return False
        except Exception as e:
            print(f"Error navigating to next page: {e}")
            return False
    
    def _fetch_businesses_api(self, category=None, limit=None):
        """Fetch businesses using direct API calls instead of Selenium.
        
        This is a fallback method if Selenium approach fails.
        
        Args:
            category: Optional category to filter businesses
            limit: Maximum number of businesses to fetch
            
        Returns:
            List of business dictionaries
        """
        print(f"Fetching businesses via API endpoint: {self.api_endpoint}")
        businesses = []
        
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
                'Accept': 'text/html,application/json,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.lowcountrylocalfirst.org/',
                'X-Requested-With': 'XMLHttpRequest',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            })
            
            page = 1
            total_count = 0
            
            while True:
                print(f"Fetching page {page} from API...")
                
                # Prepare request data
                data = {
                    'directoryID': self.directory_id,
                    'pageNumber': page,
                    'searchText': '',
                }
                
                # Add category filter if specified
                if category and self.get_category_id(category):
                    data['memberTypeIDs'] = self.get_category_id(category)
                
                # Make the API request
                response = session.post(self.api_endpoint, data=data)
                
                # Add a delay to avoid overwhelming the server
                time.sleep(random.uniform(1.0, 2.0))
                
                if response.status_code != 200:
                    print(f"API request failed with status code: {response.status_code}")
                    break
                
                try:
                    # Try to parse the JSON response
                    result = response.json()
                    
                    # Check for successful response
                    if result.get('Status') != 'Success':
                        print(f"API returned error status: {result.get('Status')}")
                        break
                    
                    # Extract business data
                    members = result.get('Members', [])
                    if not members:
                        print("No members found in response")
                        break
                    
                    print(f"Found {len(members)} businesses on page {page}")
                    
                    # Parse business data
                    for member in members:
                        business = self._parse_business_from_json(member)
                        if business:
                            businesses.append(business)
                            
                            # Check if we've reached the limit
                            if limit and len(businesses) >= limit:
                                print(f"Reached limit of {limit} businesses")
                                return businesses[:limit]
                    
                    # Check if there are more pages
                    total_count = result.get('TotalCount', 0)
                    items_per_page = len(members)
                    if items_per_page == 0 or page * items_per_page >= total_count:
                        print("No more pages available")
                        break
                    
                    page += 1
                    
                    # Safety check to avoid infinite loops
                    if page > 20:
                        print("Reached maximum page limit (20)")
                        break
                    
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON response: {response.text[:100]}...")
                    break
                except Exception as e:
                    print(f"Error processing API response: {e}")
                    break
            
            print(f"API fetch completed. Total businesses: {len(businesses)}")
            return businesses
            
        except Exception as e:
            print(f"Error fetching businesses via API: {e}")
            return businesses
    
    def _parse_business_from_json(self, business_json):
        """Parse business data from JSON response.
        
        Args:
            business_json: JSON object for a business
            
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
                'phone': business_json.get('Phone', ''),
                'location': '',
                'image_url': business_json.get('LogoUrl', ''),
                'source': 'LowcountryLocalFirstSelenium'
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
            print(f"Error parsing business from JSON: {e}")
            return None
            
    def scrape_businesses(self, category=None, limit=None):
        """Scrape businesses from the Lowcountry Local First directory.
        
        Args:
            category: Optional category to filter businesses.
            limit: Maximum number of businesses to scrape.
            
        Returns:
            List of business dictionaries.
        """
        print(f"Starting to scrape businesses (category: {category}, limit: {limit})...")
        all_businesses = []
        
        try:
            # Try to use Selenium first
            try:
                # Try the main URL first
                print(f"Trying main URL: {self.base_url}")
                if not self.driver:
                    print("Initializing driver and loading base page...")
                    self.get_page(self.base_url)
                    
                    # Check if we got a 404 or error page
                    if "404" in self.driver.title or "not found" in self.driver.title.lower():
                        print(f"Main URL failed with 404. Current URL: {self.driver.current_url}")
                        
                        # Try alternative URLs
                        for alt_url in self.alternative_urls:
                            print(f"Trying alternative URL: {alt_url}")
                            self.get_page(alt_url)
                            
                            # Check if this URL works
                            if "404" not in self.driver.title and "not found" not in self.driver.title.lower():
                                print(f"Found working URL: {alt_url}")
                                self.base_url = alt_url
                                break
                        else:
                            print("All Selenium URLs failed. Falling back to API method...")
                            return self._fetch_businesses_api(category, limit)
                    
                    print(f"Successfully loaded page: {self.driver.current_url}")
                
                # If a category is specified, try to select it
                if category:
                    print(f"Category specified: {category}, getting category ID...")
                    category_id = self.get_category_id(category)
                    if category_id:
                        print(f"Category ID found: {category_id}, selecting category...")
                        self._select_category(category_id)
                    else:
                        print(f"No category ID found for '{category}', using all categories")
                
                page_num = 1
                while True:
                    print(f"Processing page {page_num}...")
                    # Get businesses from current page
                    page_businesses = self._get_businesses_from_page()
                    
                    if page_businesses:
                        print(f"Found {len(page_businesses)} businesses on page {page_num}")
                        all_businesses.extend(page_businesses)
                        print(f"Total businesses scraped so far: {len(all_businesses)}")
                    else:
                        print(f"No businesses found on page {page_num}")
                    
                    # Check if we've reached the limit
                    if limit and len(all_businesses) >= limit:
                        print(f"Reached limit of {limit} businesses")
                        all_businesses = all_businesses[:limit]
                        break
                    
                    # Try to go to next page
                    if not self._go_to_next_page():
                        print("No more pages available")
                        break
                        
                    page_num += 1
                    
                    # Safety limit to avoid infinite loops
                    if page_num > 20:
                        print("Reached maximum page limit (20)")
                        break
                
                print(f"Selenium scraping completed. Total businesses scraped: {len(all_businesses)}")
                
            except Exception as e:
                print(f"Selenium scraping failed: {e}")
                print("Falling back to API method...")
                all_businesses = self._fetch_businesses_api(category, limit)
                
            return all_businesses
        except Exception as e:
            print(f"Error scraping businesses: {e}")
            return all_businesses
        finally:
            print("Closing browser...")
            self._close_driver()
    
    def save_to_csv(self, businesses, output_file="lowcountry_businesses.csv"):
        """Save scraped businesses to a CSV file.
        
        Args:
            businesses: List of business dictionaries.
            output_file: Path to the output CSV file.
            
        Returns:
            Path to the saved CSV file.
        """
        print(f"Saving {len(businesses)} businesses to CSV: {output_file}...")
        try:
            if not businesses:
                print("No businesses to save")
                return None
                
            # Ensure we have a common set of fields for all businesses
            fields = ['name', 'website', 'location', 'description', 'phone', 'image_url', 'category', 'source']
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fields)
                writer.writeheader()
                
                for business in businesses:
                    # Ensure all required fields are present
                    row = {field: business.get(field, '') for field in fields}
                    writer.writerow(row)
            
            print(f"Successfully saved {len(businesses)} businesses to {output_file}")
            return output_file
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            return None
    
    def save_to_database(self, businesses):
        """Save scraped businesses to the database.
        
        Args:
            businesses: List of business dictionaries.
            
        Returns:
            Number of businesses saved.
        """
        print(f"Saving {len(businesses)} businesses to database...")
        if not self.db_connection:
            print("No database connection provided")
            return 0
            
        count = 0
        for business in businesses:
            try:
                print(f"Saving business: {business.get('name', 'Unknown')}")
                if hasattr(self.db_connection, 'add_business'):
                    # Check if we need to format data fields
                    name = business.get('name', '')
                    location = business.get('location', '')
                    description = business.get('description', '')
                    category = business.get('category', '')
                    image_url = business.get('image_url', '')
                    website = business.get('website', '')
                    
                    # Convert empty strings to None if needed
                    if not name: name = None
                    
                    # Add business to database
                    self.db_connection.add_business(
                        name=name,
                        location=location,
                        description=description,
                        category=category,
                        image_url=image_url,
                        website=website
                    )
                    count += 1
                    print(f"Business '{name}' saved to database")
                else:
                    print("Database connection doesn't have an add_business method")
            except Exception as e:
                print(f"Error saving business to database: {e}")
                
        print(f"Successfully saved {count} businesses to database")
        return count
        
    def _extract_business_data_dynamic(self, card_element):
        """Extract business data from a card element using multiple possible selectors."""
        print("Extracting business data with dynamic method...")
        try:
            business = {}
            
            # Business name - try multiple selectors
            name_selectors = [
                ".listing-title a", ".directory-item-title a", 
                ".business-name", "h3 a", "h4 a", ".title a",
                ".member-name", "h1 a", "h2 a", "h5 a", "h6 a",
                ".name a", ".business-title a"
            ]
            
            # Try each name selector
            name_element = None
            name_url = None
            
            print("Looking for business name...")
            for selector in name_selectors:
                try:
                    name_element = card_element.find_element(By.CSS_SELECTOR, selector)
                    if name_element:
                        business["name"] = name_element.text.strip()
                        name_url = name_element.get_attribute("href")
                        business["website"] = name_url
                        print(f"Found business name: {business['name']}")
                        break
                except:
                    continue
            
            if not name_element:
                print("No name found with specific selectors, looking for any heading...")
                # If we couldn't find a name with selectors, look for any heading
                try:
                    headings = card_element.find_elements(By.CSS_SELECTOR, "h1, h2, h3, h4, h5, h6")
                    if headings:
                        business["name"] = headings[0].text.strip()
                        print(f"Found business name from heading: {business['name']}")
                        
                        # Try to find a link in or near the heading
                        try:
                            link = headings[0].find_element(By.CSS_SELECTOR, "a")
                            business["website"] = link.get_attribute("href")
                            print(f"Found website: {business['website']}")
                        except:
                            # Look for any link in the card
                            try:
                                link = card_element.find_element(By.CSS_SELECTOR, "a")
                                business["website"] = link.get_attribute("href")
                                print(f"Found website from card: {business['website']}")
                            except:
                                business["website"] = ""
                except Exception as e:
                    print(f"Error finding headings: {e}")
                    # Last resort - use any text in the card
                    print("Using card text as fallback for name")
                    business["name"] = card_element.text.strip()[:50]
                    business["website"] = ""
            
            # Location - try multiple selectors
            location_selectors = [
                ".geodir_post_meta .geodir-field-address", 
                ".address", ".location",
                ".member-address", ".directory-item-address"
            ]
            
            print("Looking for business location...")
            business["location"] = ""
            for selector in location_selectors:
                try:
                    location_element = card_element.find_element(By.CSS_SELECTOR, selector)
                    if location_element:
                        business["location"] = location_element.text.strip()
                        print(f"Found location: {business['location']}")
                        break
                except:
                    continue
            
            # Description - try to find any description
            description_selectors = [
                ".description", ".excerpt", ".summary",
                ".member-description", ".directory-item-description"
            ]
            
            print("Looking for business description...")
            business["description"] = ""
            for selector in description_selectors:
                try:
                    desc_element = card_element.find_element(By.CSS_SELECTOR, selector)
                    if desc_element:
                        business["description"] = desc_element.text.strip()
                        print(f"Found description: {business['description'][:30]}...")
                        break
                except:
                    continue
            
            # Phone - try multiple selectors
            phone_selectors = [
                ".geodir_post_meta .geodir-field-phone", 
                ".phone", ".tel", 
                ".member-phone", ".directory-item-phone"
            ]
            
            print("Looking for business phone...")
            business["phone"] = ""
            for selector in phone_selectors:
                try:
                    phone_element = card_element.find_element(By.CSS_SELECTOR, selector)
                    if phone_element:
                        business["phone"] = phone_element.text.strip()
                        print(f"Found phone: {business['phone']}")
                        break
                except:
                    continue
            
            # Image - try multiple selectors
            image_selectors = [
                ".geodir-image img", ".logo img", 
                "img.business-logo", ".member-logo img",
                ".directory-item-image img", ".featured-image img"
            ]
            
            print("Looking for business image...")
            business["image_url"] = ""
            for selector in image_selectors:
                try:
                    img_element = card_element.find_element(By.CSS_SELECTOR, selector)
                    if img_element:
                        business["image_url"] = img_element.get_attribute("src")
                        print(f"Found image: {business['image_url']}")
                        break
                except:
                    continue
                    
            # If no image found with selectors, try any image in the card
            if not business["image_url"]:
                print("No image found with specific selectors, looking for any image...")
                try:
                    img_elements = card_element.find_elements(By.CSS_SELECTOR, "img")
                    if img_elements:
                        business["image_url"] = img_elements[0].get_attribute("src")
                        print(f"Found image from general search: {business['image_url']}")
                except:
                    pass
            
            # Category - try multiple selectors
            category_selectors = [
                ".geodir-field-categories", ".category", 
                ".business-category", ".member-category",
                ".directory-item-category"
            ]
            
            print("Looking for business category...")
            business["category"] = ""
            for selector in category_selectors:
                try:
                    category_element = card_element.find_element(By.CSS_SELECTOR, selector)
                    if category_element:
                        business["category"] = category_element.text.strip()
                        print(f"Found category: {business['category']}")
                        break
                except:
                    continue
            
            # Add source
            business["source"] = "LowcountryLocalFirstSelenium"
            
            print(f"Successfully extracted business data: {business['name']}")
            return business
        except Exception as e:
            print(f"Error extracting business data: {e}")
            return None 