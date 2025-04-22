import time
from selenium import webdriver
from selenium.webdriver.safari.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from .scrapers import BaseEventScraper

class BaseSeleniumScraper(BaseEventScraper):
    """Base class for all Selenium-based scrapers."""
    
    def __init__(self, db_connection=None):
        """Initialize the Selenium scraper.
        
        Args:
            db_connection: Optional database connection for saving data.
        """
        super().__init__(db_connection)
        self.driver = None
    
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "BaseSeleniumScraper"
        
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30):
        """Required abstract method implementation"""
        return []
        
    def scrape_event(self, event_url):
        """Required abstract method implementation"""
        return None
        
    def _initialize_driver(self):
        """Initialize the Safari WebDriver."""
        options = Options()
        self.driver = webdriver.Safari(options=options)
        # Set default wait time
        self.driver.implicitly_wait(10)
        
    def _close_driver(self):
        """Close the WebDriver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            
    def get_page(self, url, wait_time=10):
        """Load a page in the browser.
        
        Args:
            url: The URL to load.
            wait_time: Time to wait for the page to load in seconds.
            
        Returns:
            The driver instance after loading the page.
        """
        if not self.driver:
            self._initialize_driver()
            
        self.driver.get(url)
        # Give the page some time to load
        time.sleep(wait_time)
        return self.driver
    
    def wait_for_element(self, selector, by=By.CSS_SELECTOR, timeout=10):
        """Wait for an element to be present on the page.
        
        Args:
            selector: The CSS selector or XPath.
            by: The method to locate elements (default: CSS_SELECTOR).
            timeout: Maximum time to wait in seconds.
            
        Returns:
            The element if found, None otherwise.
        """
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except TimeoutException:
            print(f"Timed out waiting for element: {selector}")
            return None
    
    def find_elements(self, selector, by=By.CSS_SELECTOR):
        """Find elements on the page.
        
        Args:
            selector: The CSS selector or XPath.
            by: The method to locate elements (default: CSS_SELECTOR).
            
        Returns:
            List of elements if found, empty list otherwise.
        """
        try:
            return self.driver.find_elements(by, selector)
        except NoSuchElementException:
            return []
    
    def find_element(self, selector, by=By.CSS_SELECTOR):
        """Find a single element on the page.
        
        Args:
            selector: The CSS selector or XPath.
            by: The method to locate elements (default: CSS_SELECTOR).
            
        Returns:
            The element if found, None otherwise.
        """
        try:
            return self.driver.find_element(by, selector)
        except NoSuchElementException:
            return None
    
    def scroll_to_bottom(self, scroll_pause_time=1, max_scrolls=10):
        """Scroll to the bottom of the page to load lazy-loaded content.
        
        Args:
            scroll_pause_time: Time to pause between scrolls in seconds.
            max_scrolls: Maximum number of scrolls to perform.
            
        Returns:
            True if scrolled to bottom, False otherwise.
        """
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        for _ in range(max_scrolls):
            # Scroll down
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for content to load
            time.sleep(scroll_pause_time)
            
            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                # We've reached the bottom
                return True
            
            last_height = new_height
        
        # We've reached max_scrolls but might not be at the bottom
        return False
        
    def __del__(self):
        """Ensure the browser is closed when the object is deleted."""
        self._close_driver() 