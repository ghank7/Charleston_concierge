import sqlite3
import os
import pandas as pd

class CharlestonDB:
    """
    Database manager for Charleston businesses and events.
    Provides methods to query and update the database.
    """
    
    def __init__(self, db_path=None):
        """Initialize the database connection"""
        if db_path is None:
            # Get the project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_path = os.path.join(project_root, 'data/charleston.db')
        
        self.db_path = db_path
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    def database_exists(self):
        """Check if the database file exists"""
        return os.path.exists(self.db_path)
    
    def connect(self):
        """Create and return a database connection"""
        return sqlite3.connect(self.db_path)
    
    def create_tables(self):
        """Create database tables if they don't exist"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Check if tables exist already
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='businesses'")
        businesses_exists = cursor.fetchone() is not None
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='places'")
        places_exists = cursor.fetchone() is not None
        
        # Create places table (more general than businesses)
        if not places_exists:
            cursor.execute("""
            CREATE TABLE places (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT,
                description TEXT,
                type TEXT,           -- Type of place (park, business, landmark, etc.)
                category TEXT,        -- Categories as comma-separated string
                rating REAL,
                image_url TEXT,
                website TEXT,
                phone TEXT,
                email TEXT,
                source TEXT,          -- Data source
                latitude REAL,
                longitude REAL,
                metadata TEXT         -- JSON metadata for flexible storage
            )
            """)
            
        # Create or alter businesses table
        if not businesses_exists:
            cursor.execute("""
            CREATE TABLE businesses (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT,
                description TEXT,
                category TEXT,
                rating REAL,
                image_url TEXT,
                website TEXT,
                phone TEXT,
                email TEXT,
                source TEXT
            )
            """)
        else:
            # Add columns if they don't exist
            try:
                cursor.execute("SELECT phone FROM businesses LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE businesses ADD COLUMN phone TEXT")
                
            try:
                cursor.execute("SELECT email FROM businesses LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE businesses ADD COLUMN email TEXT")
                
            try:
                cursor.execute("SELECT source FROM businesses LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE businesses ADD COLUMN source TEXT")
        
        # Create place_categories table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS place_categories (
            place_id INTEGER,
            category_id INTEGER,
            PRIMARY KEY (place_id, category_id),
            FOREIGN KEY (place_id) REFERENCES places (id),
            FOREIGN KEY (category_id) REFERENCES categories (id)
        )
        """)
        
        # Create business_info table for additional details
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS business_info (
            business_id INTEGER PRIMARY KEY,
            phone TEXT,
            email TEXT,
            hours TEXT,
            amenities TEXT,
            notes TEXT,
            FOREIGN KEY (business_id) REFERENCES businesses (id)
        )
        """)
        
        # Create categories table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """)
        
        # Create business_categories table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS business_categories (
            business_id INTEGER,
            category_id INTEGER,
            PRIMARY KEY (business_id, category_id),
            FOREIGN KEY (business_id) REFERENCES businesses (id),
            FOREIGN KEY (category_id) REFERENCES categories (id)
        )
        """)
        
        # Create events table
        cursor.execute("""
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
            place_id INTEGER,
            FOREIGN KEY (business_id) REFERENCES businesses (id),
            FOREIGN KEY (place_id) REFERENCES places (id)
        )
        """)
        
        conn.commit()
        conn.close()
        
        return True
    
    def get_all_businesses(self):
        """Get all businesses from the database"""
        conn = self.connect()
        query = "SELECT * FROM businesses"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_business_by_id(self, business_id):
        """Get a business by its ID"""
        conn = self.connect()
        query = "SELECT * FROM businesses WHERE id = ?"
        df = pd.read_sql_query(query, conn, params=(business_id,))
        conn.close()
        return df.iloc[0] if not df.empty else None
    
    def search_businesses(self, query_text, limit=10):
        """
        Search businesses by name, location, or description.
        Returns businesses that match the query text.
        """
        conn = self.connect()
        search_term = f"%{query_text}%"
        query = """
        SELECT b.* FROM businesses b
        WHERE b.name LIKE ? 
        OR b.location LIKE ? 
        OR b.description LIKE ?
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(search_term, search_term, search_term, limit))
        conn.close()
        return df
    
    def get_businesses_by_category(self, category, limit=10):
        """Get businesses in a specific category"""
        conn = self.connect()
        query = """
        SELECT b.* FROM businesses b
        JOIN business_categories bc ON b.id = bc.business_id
        JOIN categories c ON bc.category_id = c.id
        WHERE c.name LIKE ?
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(f"%{category}%", limit))
        conn.close()
        return df
    
    def get_business_categories(self, business_id):
        """Get all categories for a business"""
        conn = self.connect()
        query = """
        SELECT c.* FROM categories c
        JOIN business_categories bc ON c.id = bc.category_id
        WHERE bc.business_id = ?
        """
        df = pd.read_sql_query(query, conn, params=(business_id,))
        conn.close()
        return df
    
    def get_all_events(self):
        """Get all events from the database"""
        conn = self.connect()
        query = "SELECT * FROM events"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_event_by_id(self, event_id):
        """Get an event by its ID"""
        conn = self.connect()
        query = "SELECT * FROM events WHERE id = ?"
        df = pd.read_sql_query(query, conn, params=(event_id,))
        conn.close()
        return df.iloc[0] if not df.empty else None
    
    def search_events(self, query_text, limit=10):
        """
        Search events by name, location, or description.
        Returns events that match the query text.
        """
        conn = self.connect()
        search_term = f"%{query_text}%"
        query = """
        SELECT e.* FROM events e
        WHERE e.name LIKE ? 
        OR e.location LIKE ? 
        OR e.description LIKE ?
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(search_term, search_term, search_term, limit))
        conn.close()
        return df
    
    def get_events_by_business(self, business_id):
        """Get all events at a specific business"""
        conn = self.connect()
        query = "SELECT * FROM events WHERE business_id = ?"
        df = pd.read_sql_query(query, conn, params=(business_id,))
        conn.close()
        return df
    
    def get_events_by_date(self, date_str):
        """Get events on a specific date (YYYY-MM-DD format)"""
        conn = self.connect()
        query = "SELECT * FROM events WHERE date = ?"
        df = pd.read_sql_query(query, conn, params=(date_str,))
        conn.close()
        return df
    
    def add_event(self, name, date=None, time=None, location=None, description=None, 
                  url=None, image_url=None, source=None, business_id=None):
        """Add a new event to the database"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO events (name, date, time, location, description, url, image_url, source, business_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, date, time, location, description, url, image_url, source, business_id))
        
        event_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return event_id
    
    def add_event_from_dict(self, event_dict):
        """Add a new event from a dictionary of event data"""
        # Map dictionary keys to expected parameter names
        key_mapping = {
            'Name': 'name',
            'Date': 'date',
            'Time': 'time',
            'Location': 'location',
            'Description': 'description',
            'URL': 'url',
            'Image_URL': 'image_url',
            'Source': 'source',
            'Business_ID': 'business_id'
        }
        
        # Convert dict keys to lowercase for case-insensitive matching
        event_data = {}
        for key, value in event_dict.items():
            # Try to find a matching parameter name
            param_name = key_mapping.get(key, key.lower())
            event_data[param_name] = value
        
        # Extract parameters for add_event method
        return self.add_event(
            name=event_data.get('name'),
            date=event_data.get('date'),
            time=event_data.get('time'),
            location=event_data.get('location'),
            description=event_data.get('description'),
            url=event_data.get('url'),
            image_url=event_data.get('image_url'),
            source=event_data.get('source'),
            business_id=event_data.get('business_id')
        )
    
    def add_business(self, name, location=None, description=None, website=None, image_url=None, category=None, rating=None, phone=None, email=None, source=None):
        """Add a new business to the database"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Check if business already exists with the same name
        cursor.execute("SELECT id FROM businesses WHERE name = ?", (name,))
        result = cursor.fetchone()
        
        if result:
            # Business already exists, update it
            business_id = result[0]
            cursor.execute("""
            UPDATE businesses 
            SET location = ?, description = ?, website = ?, image_url = ?, category = ?, rating = ?,
                phone = ?, email = ?, source = ?
            WHERE id = ?
            """, (location, description, website, image_url, category, rating, 
                  phone, email, source, business_id))
        else:
            # Insert new business
            cursor.execute("""
            INSERT INTO businesses (name, location, description, website, image_url, category, rating, 
                                  phone, email, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, location, description, website, image_url, category, rating, 
                 phone, email, source))
            
            business_id = cursor.lastrowid
        
        # Add categories if provided
        if category:
            categories = [cat.strip() for cat in category.split(',') if cat.strip()]
            for cat in categories:
                # Ensure category exists
                cursor.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))
                
                # Get category ID
                cursor.execute("SELECT id FROM categories WHERE name = ?", (cat,))
                cat_id = cursor.fetchone()[0]
                
                # Add business-category relationship
                cursor.execute("""
                INSERT OR REPLACE INTO business_categories (business_id, category_id)
                VALUES (?, ?)
                """, (business_id, cat_id))
        
        conn.commit()
        conn.close()
        return business_id
    
    def add_business_from_dict(self, business_dict):
        """Add a business to the database from a dictionary.
        
        Args:
            business_dict: Dictionary containing business information.
            
        Returns:
            ID of the newly added business.
        """
        if not business_dict:
            return None
        
        # Convert all keys to lowercase for case-insensitive matching
        business_dict_lower = {k.lower(): v for k, v in business_dict.items()}
        
        # Map dictionary keys to add_business parameters
        name = business_dict_lower.get('name', '')
        location = business_dict_lower.get('location', '')
        description = business_dict_lower.get('description', '')
        category = business_dict_lower.get('category', '')
        
        # Handle rating - convert to float if possible
        rating = None
        if 'rating' in business_dict_lower:
            try:
                rating = float(business_dict_lower['rating'])
            except (ValueError, TypeError):
                rating = None
                
        image_url = business_dict_lower.get('image_url', '')
        
        # For website, try multiple possible keys
        website = None
        for key in ['website', 'url', 'web', 'link']:
            if key in business_dict_lower and business_dict_lower[key]:
                website = business_dict_lower[key]
                break
        
        # Additional fields
        phone = business_dict_lower.get('phone', None)
        email = business_dict_lower.get('email', None)
        source = business_dict_lower.get('source', None)
        
        # Call the existing add_business method
        return self.add_business(
            name=name,
            location=location,
            description=description,
            website=website,
            image_url=image_url,
            category=category,
            rating=rating,
            phone=phone,
            email=email,
            source=source
        )
    
    def delete_event(self, event_id):
        """Delete an event from the database"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM events WHERE id = ?", (event_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    
    def import_events(self, events_df):
        """Import events from a DataFrame"""
        if events_df.empty:
            print("No events to import")
            return 0
            
        conn = self.connect()
        cursor = conn.cursor()
        
        # Get the highest existing ID
        cursor.execute("SELECT MAX(id) FROM events")
        result = cursor.fetchone()
        start_id = result[0] + 1 if result[0] is not None else 1
        
        imported_count = 0
        
        for i, row in events_df.iterrows():
            # Clean data
            name = row.get('name', None)
            if not name:
                continue  # Skip if no name
                
            # Check if event already exists with the same name, date, and source
            cursor.execute("""
            SELECT id FROM events 
            WHERE name = ? AND date = ? AND source = ?
            """, (
                name, 
                row.get('date', None),
                row.get('source', None)
            ))
            
            if cursor.fetchone():
                # Event already exists, skip
                continue
                
            # Get values, handling missing columns
            date = row.get('date', None)
            time = row.get('time', None)
            location = row.get('location', None)
            description = row.get('description', None)
            url = row.get('url', None)
            image_url = row.get('image_url', None)
            source = row.get('source', None)
            business_id = row.get('business_id', None)
            
            # Insert event
            cursor.execute("""
            INSERT INTO events (id, name, date, time, location, description, url, image_url, source, business_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (start_id + i, name, date, time, location, description, url, image_url, source, business_id))
            
            imported_count += 1
        
        conn.commit()
        conn.close()
        
        return imported_count
    
    def import_csv_events(self, csv_path):
        """Import events from a CSV file"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} events from CSV")
        
        # Convert column names if they don't match
        column_mapping = {
            'Name': 'name',
            'Date': 'date',
            'Time': 'time',
            'Location': 'location',
            'Description': 'description',
            'URL': 'url',
            'Image_URL': 'image_url',
            'Source': 'source'
        }
        
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        return self.import_events(df)
        
    def import_csv_businesses(self, csv_path):
        """Import businesses from a CSV file"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} businesses from CSV")
        
        # Handle different column names in CSV files
        column_mapping = {
            'Category': 'category',
            'Name': 'name',
            'Location': 'location',
            'Description': 'description',
            'Rating': 'rating',
            'Image_URL': 'image_url',
            'Website': 'website'
        }
        
        # Rename columns if they don't match expected names
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Check if we have required columns
        required_columns = ['name']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in CSV")
        
        # Add missing columns with None values
        for col in ['category', 'location', 'description', 'rating', 'image_url', 'website']:
            if col not in df.columns:
                df[col] = None
        
        conn = self.connect()
        cursor = conn.cursor()
        
        # First, add any new categories
        categories = set()
        for cat in df['category'].dropna():
            # Split multiple categories
            for c in str(cat).split(','):
                c = c.strip()
                if c:
                    categories.add(c)
        
        for category in categories:
            cursor.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (category,))
            
        # Get category IDs
        category_ids = {}
        cursor.execute("SELECT id, name FROM categories")
        for cat_id, cat_name in cursor.fetchall():
            category_ids[cat_name] = cat_id
        
        # Import businesses
        imported_count = 0
        
        for i, row in df.iterrows():
            name = row['name'] if not pd.isna(row['name']) else None
            if not name:
                continue  # Skip if no name
                
            # Check if business already exists
            cursor.execute("SELECT id FROM businesses WHERE name = ?", (name,))
            result = cursor.fetchone()
            
            if result:
                # Business already exists, update
                business_id = result[0]
                cursor.execute("""
                UPDATE businesses 
                SET location = ?, description = ?, category = ?, rating = ?, image_url = ?, website = ?
                WHERE id = ?
                """, (
                    row['location'] if not pd.isna(row['location']) else None,
                    row['description'] if not pd.isna(row['description']) else None,
                    row['category'] if not pd.isna(row['category']) else None,
                    row['rating'] if not pd.isna(row['rating']) else None,
                    row['image_url'] if not pd.isna(row['image_url']) else None,
                    row['website'] if not pd.isna(row['website']) else None,
                    business_id
                ))
            else:
                # Insert new business
                cursor.execute("""
                INSERT INTO businesses (name, location, description, category, rating, image_url, website)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    name,
                    row['location'] if not pd.isna(row['location']) else None,
                    row['description'] if not pd.isna(row['description']) else None,
                    row['category'] if not pd.isna(row['category']) else None,
                    row['rating'] if not pd.isna(row['rating']) else None,
                    row['image_url'] if not pd.isna(row['image_url']) else None,
                    row['website'] if not pd.isna(row['website']) else None
                ))
                
                business_id = cursor.lastrowid
                
            # Add business-category relationships
            if not pd.isna(row['category']):
                for cat in row['category'].split(','):
                    cat = cat.strip()
                    if cat in category_ids:
                        cursor.execute("""
                        INSERT OR IGNORE INTO business_categories (business_id, category_id)
                        VALUES (?, ?)
                        """, (business_id, category_ids[cat]))
            
            imported_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"Imported {imported_count} businesses from CSV")
        return imported_count

    def get_all_places(self):
        """Get all places from the database"""
        conn = self.connect()
        query = "SELECT * FROM places"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_place_by_id(self, place_id):
        """Get a place by its ID"""
        conn = self.connect()
        query = "SELECT * FROM places WHERE id = ?"
        df = pd.read_sql_query(query, conn, params=(place_id,))
        conn.close()
        return df.iloc[0] if not df.empty else None
    
    def search_places(self, query_text, limit=10):
        """
        Search places by name, location, or description.
        Returns places that match the query text.
        """
        conn = self.connect()
        search_term = f"%{query_text}%"
        query = """
        SELECT p.* FROM places p
        WHERE p.name LIKE ? 
        OR p.location LIKE ? 
        OR p.description LIKE ?
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(search_term, search_term, search_term, limit))
        conn.close()
        return df
    
    def get_places_by_type(self, place_type, limit=10):
        """Get places of a specific type"""
        conn = self.connect()
        query = """
        SELECT p.* FROM places p
        WHERE p.type LIKE ?
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(f"%{place_type}%", limit))
        conn.close()
        return df
    
    def get_places_by_category(self, category, limit=10):
        """Get places in a specific category"""
        conn = self.connect()
        search_term = f"%{category}%"
        query = """
        SELECT p.* FROM places p
        WHERE p.category LIKE ?
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(search_term, limit))
        conn.close()
        return df
    
    def add_place(self, name, location=None, description=None, place_type=None, category=None,
                 rating=None, image_url=None, website=None, phone=None, email=None,
                 source=None, latitude=None, longitude=None, metadata=None):
        """Add a new place to the database"""
        import json
        
        conn = self.connect()
        cursor = conn.cursor()
        
        # Convert metadata to JSON string if it's not already a string
        if metadata and not isinstance(metadata, str):
            metadata = json.dumps(metadata)
        
        # Check if place already exists with the same name and location
        cursor.execute("SELECT id FROM places WHERE name = ? AND location = ?", (name, location))
        result = cursor.fetchone()
        
        if result:
            # Place already exists, update it
            place_id = result[0]
            cursor.execute("""
            UPDATE places 
            SET description = ?, type = ?, category = ?, rating = ?,
                image_url = ?, website = ?, phone = ?, email = ?, source = ?,
                latitude = ?, longitude = ?, metadata = ?
            WHERE id = ?
            """, (description, place_type, category, rating, 
                 image_url, website, phone, email, source,
                 latitude, longitude, metadata, place_id))
        else:
            # Insert new place
            cursor.execute("""
            INSERT INTO places (name, location, description, type, category, rating, 
                              image_url, website, phone, email, source,
                              latitude, longitude, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, location, description, place_type, category, rating, 
                 image_url, website, phone, email, source,
                 latitude, longitude, metadata))
            
            place_id = cursor.lastrowid
        
        # Add categories if provided
        if category:
            categories = [cat.strip() for cat in category.split(',') if cat.strip()]
            for cat in categories:
                # Ensure category exists
                cursor.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))
                
                # Get category ID
                cursor.execute("SELECT id FROM categories WHERE name = ?", (cat,))
                cat_id = cursor.fetchone()[0]
                
                # Add place-category relationship
                cursor.execute("""
                INSERT OR IGNORE INTO place_categories (place_id, category_id)
                VALUES (?, ?)
                """, (place_id, cat_id))
        
        conn.commit()
        conn.close()
        return place_id
    
    def add_place_from_dict(self, place_dict):
        """Add a new place from a dictionary of place data"""
        # Map dictionary keys to expected parameter names
        key_mapping = {
            'Name': 'name',
            'Location': 'location',
            'Description': 'description',
            'Type': 'place_type',
            'Category': 'category',
            'Categories': 'category',
            'Rating': 'rating',
            'Image_URL': 'image_url',
            'Website': 'website',
            'URL': 'website',
            'Phone': 'phone',
            'Email': 'email',
            'Source': 'source',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Metadata': 'metadata'
        }
        
        # Convert dict keys to lowercase for case-insensitive matching
        place_data = {}
        for key, value in place_dict.items():
            # Try to find a matching parameter name
            param_name = key_mapping.get(key, key.lower())
            place_data[param_name] = value
        
        # Handle categories if they're a list
        if 'categories' in place_data and isinstance(place_data['categories'], list):
            place_data['category'] = ','.join(place_data['categories'])
        elif 'category' in place_data and isinstance(place_data['category'], list):
            place_data['category'] = ','.join(place_data['category'])
        
        # Handle metadata if it exists in details
        if 'details' in place_dict and isinstance(place_dict['details'], dict):
            place_data['metadata'] = place_dict['details']
        
        # Extract parameters for add_place method
        return self.add_place(
            name=place_data.get('name'),
            location=place_data.get('location'),
            description=place_data.get('description'),
            place_type=place_data.get('place_type') or place_data.get('type'),
            category=place_data.get('category'),
            rating=place_data.get('rating'),
            image_url=place_data.get('image_url'),
            website=place_data.get('website'),
            phone=place_data.get('phone'),
            email=place_data.get('email'),
            source=place_data.get('source'),
            latitude=place_data.get('latitude'),
            longitude=place_data.get('longitude'),
            metadata=place_data.get('metadata')
        )
    
    def delete_place(self, place_id):
        """Delete a place from the database"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM places WHERE id = ?", (place_id,))
        deleted = cursor.rowcount > 0
        
        # Also delete related category relationships
        if deleted:
            cursor.execute("DELETE FROM place_categories WHERE place_id = ?", (place_id,))
        
        conn.commit()
        conn.close()
        return deleted
    
    def migrate_businesses_to_places(self):
        """Migrate all businesses to the places table"""
        conn = self.connect()
        
        # Get all businesses
        businesses_df = pd.read_sql_query("SELECT * FROM businesses", conn)
        
        conn.close()
        
        if businesses_df.empty:
            print("No businesses to migrate")
            return 0
        
        migrated_count = 0
        for _, business in businesses_df.iterrows():
            # Convert business to place format
            place_data = {
                'name': business.get('name'),
                'location': business.get('location'),
                'description': business.get('description'),
                'place_type': 'Business',
                'category': business.get('category'),
                'rating': business.get('rating'),
                'image_url': business.get('image_url'),
                'website': business.get('website'),
                'phone': business.get('phone'),
                'email': business.get('email'),
                'source': business.get('source')
            }
            
            # Add as a new place
            place_id = self.add_place_from_dict(place_data)
            if place_id:
                migrated_count += 1
        
        return migrated_count
    
    def clear_businesses(self):
        """Clear all data from the businesses table"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM businesses")
        count = cursor.rowcount
        cursor.execute("DELETE FROM business_categories")
        cursor.execute("DELETE FROM business_info")
        conn.commit()
        conn.close()
        return count

# Test the database manager
if __name__ == "__main__":
    try:
        db = CharlestonDB()
        
        # Create tables if needed
        if not db.database_exists():
            print("Creating database tables...")
            db.create_tables()
            print("Database created successfully")
        
        # Test database connection and a simple query
        businesses = db.get_all_businesses()
        print(f"Found {len(businesses)} businesses in the database")
        
        # Test business search
        if len(businesses) > 0:
            sample_term = "restaurant"
            results = db.search_businesses(sample_term, limit=5)
            print(f"Found {len(results)} businesses matching '{sample_term}'")
            if len(results) > 0:
                print(f"Sample result: {results.iloc[0]['name']}")
        
    except Exception as e:
        print(f"Error testing database: {e}") 