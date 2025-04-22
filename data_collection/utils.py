"""
Scraping Utilities

This module contains common utility functions for web scraping and data processing.
"""

import re
import time
import html
import random
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union

import requests
from bs4 import BeautifulSoup


def get_user_agent() -> str:
    """Return a random user agent string"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    ]
    return random.choice(user_agents)


def get_headers() -> Dict[str, str]:
    """Return headers for HTTP requests with random user agent"""
    return {
        'User-Agent': get_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


def fetch_url(url: str, retries: int = 3, delay: float = 1.0) -> Optional[str]:
    """
    Fetch content from a URL with retries and delay
    
    Args:
        url: URL to fetch
        retries: Number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        HTML content as string or None if failed
    """
    headers = get_headers()
    
    for attempt in range(retries):
        try:
            # Add random delay to avoid rate limiting
            if attempt > 0:
                time.sleep(delay + random.uniform(0, 1))
                
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url} (attempt {attempt+1}/{retries}): {e}")
            
    return None


def get_soup(url: str, retries: int = 3) -> Optional[BeautifulSoup]:
    """
    Get BeautifulSoup object for a URL
    
    Args:
        url: URL to fetch
        retries: Number of retry attempts
        
    Returns:
        BeautifulSoup object or None if failed
    """
    html_content = fetch_url(url, retries)
    if html_content:
        return BeautifulSoup(html_content, 'html.parser')
    return None


def clean_text(text: Optional[str]) -> Optional[str]:
    """
    Clean text by removing extra whitespace, decoding HTML entities, etc.
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text or None if input is None
    """
    if text is None:
        return None
        
    # Decode HTML entities
    text = html.unescape(text)
    
    # Normalize unicode
    text = unicodedata.normalize('NFKD', text)
    
    # Replace multiple whitespace with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    return text


def extract_date_range(text: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Extract date range from text
    
    Args:
        text: Text containing date information
        
    Returns:
        Tuple of (start_date, end_date) or None if no date found
    """
    # Various date formats to try
    patterns = [
        # Month day-day, year (e.g., "January 1-5, 2023")
        r'(\w+)\s+(\d{1,2})-(\d{1,2}),\s+(\d{4})',
        
        # Month day, year - Month day, year (e.g., "January 1, 2023 - February 1, 2023")
        r'(\w+)\s+(\d{1,2}),\s+(\d{4})\s*-\s*(\w+)\s+(\d{1,2}),\s+(\d{4})',
        
        # Month day - Month day, year (e.g., "January 1 - February 1, 2023")
        r'(\w+)\s+(\d{1,2})\s*-\s*(\w+)\s+(\d{1,2}),\s+(\d{4})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            # Process based on which pattern matched
            groups = match.groups()
            
            if len(groups) == 4:  # First pattern
                month, day1, day2, year = groups
                month_num = datetime.strptime(month, '%B').month
                start_date = datetime(int(year), month_num, int(day1))
                end_date = datetime(int(year), month_num, int(day2))
                return start_date, end_date
                
            elif len(groups) == 6:  # Second pattern
                month1, day1, year1, month2, day2, year2 = groups
                start_date = datetime.strptime(f"{month1} {day1}, {year1}", '%B %d, %Y')
                end_date = datetime.strptime(f"{month2} {day2}, {year2}", '%B %d, %Y')
                return start_date, end_date
                
            elif len(groups) == 5:  # Third pattern
                month1, day1, month2, day2, year = groups
                month1_num = datetime.strptime(month1, '%B').month
                month2_num = datetime.strptime(month2, '%B').month
                start_date = datetime(int(year), month1_num, int(day1))
                end_date = datetime(int(year), month2_num, int(day2))
                return start_date, end_date
    
    return None


def parse_datetime(date_str: str, time_str: Optional[str] = None) -> Optional[datetime]:
    """
    Parse date and optional time strings into a datetime object
    
    Args:
        date_str: Date string in various formats
        time_str: Optional time string
        
    Returns:
        datetime object or None if parsing fails
    """
    date_str = clean_text(date_str)
    if not date_str:
        return None
        
    # Try various date formats
    date_formats = [
        '%B %d, %Y',  # January 1, 2023
        '%b %d, %Y',  # Jan 1, 2023
        '%m/%d/%Y',   # 01/01/2023
        '%Y-%m-%d',   # 2023-01-01
    ]
    
    dt = None
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue
    
    if not dt:
        return None
        
    # Add time if provided
    if time_str:
        time_str = clean_text(time_str)
        if time_str:
            # Try various time formats
            time_formats = [
                '%I:%M %p',  # 1:00 PM
                '%H:%M',     # 13:00
            ]
            
            for fmt in time_formats:
                try:
                    time_dt = datetime.strptime(time_str, fmt)
                    dt = dt.replace(hour=time_dt.hour, minute=time_dt.minute)
                    break
                except ValueError:
                    continue
    
    return dt


def generate_date_range(start_date=None, end_date=None, days=30) -> List[datetime]:
    """
    Generate a list of dates between start_date and end_date (inclusive)
    
    Args:
        start_date: Start date as string 'YYYY-MM-DD' or datetime object
        end_date: End date as string 'YYYY-MM-DD' or datetime object
        days: Number of days if end_date not provided
        
    Returns:
        List of datetime objects
    """
    # Set default start date to today if not provided
    if start_date is None:
        start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    elif isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        
    # Set default end date based on days if not provided
    if end_date is None:
        end_date = start_date + timedelta(days=days-1)
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
    # Generate date range
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
        
    return date_list


def format_datetime(dt: Optional[datetime], fmt: str = '%Y-%m-%d %H:%M:%S') -> Optional[str]:
    """
    Format datetime object to string
    
    Args:
        dt: datetime object
        fmt: Format string
        
    Returns:
        Formatted string or None if dt is None
    """
    if dt is None:
        return None
    return dt.strftime(fmt)


def extract_price(text: str) -> Optional[float]:
    """
    Extract price from text
    
    Args:
        text: Text containing price information
        
    Returns:
        Price as float or None if no price found
    """
    if not text:
        return None
        
    # Look for patterns like $10, $10.50, 10 dollars, etc.
    pattern = r'\$\s*(\d+(?:\.\d{2})?)'
    match = re.search(pattern, text)
    
    if match:
        return float(match.group(1))
        
    return None 