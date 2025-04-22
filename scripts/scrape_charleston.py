import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

base_url = "https://charleston.com/businesses"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

businesses = []

for page in range(1, 13):  # Scraping 12 pages
    url = f"{base_url}?start={(page - 1) * 20}"  # Each page has 20 listings
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all business listings
    listings = soup.select('div.item-container')
    
    for listing in listings:
        try:
            # Extract the business name
            name_element = listing.select_one('h3.item-title')
            name = name_element.text.strip() if name_element else "N/A"
            
            # Extract location (city, county)
            location_element = listing.select_one('div.item-address')
            location = location_element.text.strip() if location_element else "N/A"
            
            # Extract business description
            description_element = listing.select_one('div.item-description')
            description = description_element.text.strip() if description_element else "N/A"
            
            # Extract contact info (if available)
            contact_element = listing.select_one('div.item-contact')
            phone = ""
            email = ""
            website = ""
            
            if contact_element:
                phone_element = contact_element.select_one('div.item-phone')
                phone = phone_element.text.strip() if phone_element else "N/A"
                
                email_element = contact_element.select_one('div.item-email')
                email = email_element.text.strip() if email_element else "N/A"
                
                website_element = contact_element.select_one('a.item-website')
                website = website_element['href'] if website_element and 'href' in website_element.attrs else "N/A"
            
            businesses.append({
                'Name': name,
                'Location': location,
                'Description': description,
                'Phone': phone,
                'Email': email,
                'Website': website
            })
            
        except Exception as e:
            print(f"Error processing a listing: {e}")
    
    # Add a small delay between requests to be polite
    time.sleep(2)
    print(f"Completed page {page}")

df = pd.DataFrame(businesses)
df.to_csv('charleston_businesses.csv', index=False)
print(f"Scraped {len(businesses)} businesses successfully!")
