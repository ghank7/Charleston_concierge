import os
import pandas as pd
import sqlite3
import re

def clean_text(text):
    """Clean text for database insertion"""
    if pd.isna(text) or text == "N/A":
        return None
    return text

def create_database():
    """Create a SQLite database and import business data"""
    print("Creating SQLite database...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Set up database path
    db_path = os.path.join(project_root, 'data/charleston.db')
    
    # Check for business data
    business_csv = os.path.join(project_root, 'data/charleston_businesses.csv')
    if not os.path.exists(business_csv):
        print(f"Business data not found at {business_csv}")
        return None
    
    # Connect to SQLite database (will create if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create businesses table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS businesses (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        description TEXT,
        url TEXT,
        website TEXT,
        image_url TEXT,
        phone TEXT,
        email TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create events table (for future real events)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        date TEXT,
        time TEXT,
        location TEXT,
        description TEXT,
        url TEXT,
        image_url TEXT,
        source TEXT,
        business_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (business_id) REFERENCES businesses (id)
    )
    ''')
    
    # Create categories table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT
    )
    ''')
    
    # Create business_categories relation table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS business_categories (
        business_id INTEGER,
        category_id INTEGER,
        PRIMARY KEY (business_id, category_id),
        FOREIGN KEY (business_id) REFERENCES businesses (id),
        FOREIGN KEY (category_id) REFERENCES categories (id)
    )
    ''')
    
    # Load business data from CSV
    df = pd.read_csv(business_csv)
    print(f"Loaded {len(df)} businesses from CSV")
    
    # Extract business data
    businesses = []
    
    # Derive categories from business descriptions
    categories = set()
    business_categories = []
    
    # Common Charleston business categories
    common_categories = [
        "Restaurant", "Bar", "Café", "Coffee Shop", "Hotel", "Lodging", 
        "Retail", "Shopping", "Tour", "Museum", "Gallery", "Attraction",
        "Nightlife", "Entertainment", "Spa", "Wellness", "Outdoor Recreation",
        "Historical Site", "Southern Cuisine", "Seafood", "Brewery", "Winery",
        "Event Venue", "Wedding Venue", "Beach", "Park"
    ]
    
    # Add the common categories
    for i, category in enumerate(common_categories):
        cursor.execute("INSERT INTO categories (id, name) VALUES (?, ?)", 
                      (i+1, category))
        categories.add(category.lower())
    
    # Process each business
    for i, row in df.iterrows():
        # Clean data
        name = clean_text(row['Name'])
        if not name:
            continue  # Skip if no name
            
        location = clean_text(row['Location'])
        description = clean_text(row['Description'])
        url = clean_text(row['URL'])
        website = clean_text(row['Website'])
        image_url = clean_text(row['Image_URL'])
        phone = clean_text(row['Phone'])
        email = clean_text(row['Email'])
        
        # Insert business
        cursor.execute('''
        INSERT INTO businesses (id, name, location, description, url, website, image_url, phone, email)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (i, name, location, description, url, website, image_url, phone, email))
        
        # Assign categories based on description or name
        if description:
            # Extract categories from description using common categories
            for category in common_categories:
                if category.lower() in description.lower() or category.lower() in name.lower():
                    category_id = common_categories.index(category) + 1
                    
                    # Add business-category relationship
                    cursor.execute('''
                    INSERT OR IGNORE INTO business_categories (business_id, category_id)
                    VALUES (?, ?)
                    ''', (i, category_id))
    
    # Commit changes and close connection
    conn.commit()
    
    # Print summary
    cursor.execute("SELECT COUNT(*) FROM businesses")
    business_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM business_categories")
    category_count = cursor.fetchone()[0]
    
    print(f"Created database at {db_path}")
    print(f"Imported {business_count} businesses")
    print(f"Created {len(common_categories)} categories")
    print(f"Added {category_count} business-category relationships")
    
    conn.close()
    
    return db_path

if __name__ == "__main__":
    create_database() 