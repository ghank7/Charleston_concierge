#!/usr/bin/env python3
"""
Charleston Concierge CLI

Command-line interface for managing the Charleston Concierge database,
event scraping, and vector store operations.
"""

import os
import sys
import argparse
import datetime
import pandas as pd
from typing import List, Dict, Optional

# Try to import dependencies
try:
    import tkinter as tk
    import numpy as np
    import matplotlib.pyplot as plt
    from utils.database_manager import CharlestonDB
    
    # Import scrapers
    from data_collection.scrapers import BaseEventScraper
    from data_collection.sources.open_data_scraper import OpenDataScraper
    from data_collection.sources.lowcountry_local_first import LowcountryLocalFirstScraper
    # Only import the selenium scraper - comment out the others that don't exist
    # from data_collection.sources.holy_city_sinner import HolyCitySinnerScraper
    # from data_collection.sources.city_paper_scraper import CityScraper
    # from data_collection.sources.charleston_wine_food import CharlestonWineFoodScraper
    from data_collection.sources.lowcountry_local_first_selenium import LowcountryLocalFirstSeleniumScraper
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure you're running this script from the project root directory.")
    sys.exit(1)


def init_db(args):
    """Initialize the database"""
    db = CharlestonDB()
    
    # Check if we need to initialize
    if not args.force and db.database_exists():
        print("Database already exists. Use --force to reinitialize.")
        return False
        
    # Create database tables
    db.create_tables()
    print("Created database tables")
    
    # Import business data from CSV (optional)
    if not args.skip_businesses:
        business_csv = os.path.join('data', 'charleston_businesses.csv')
        if os.path.exists(business_csv):
            try:
                count = db.import_csv_businesses(business_csv)
                print(f"Imported {count} businesses from CSV")
            except Exception as e:
                print(f"Warning: Failed to import businesses: {e}")
                print("Continuing with event data only...")
        else:
            print(f"Warning: Business CSV file {business_csv} not found")
    else:
        print("Skipping business import, focusing on event data...")
    
    # Scrape events if requested
    if args.scrape_events:
        scrape_events(args)
    
    print("Database initialization complete.")
    return True


def scrape_events(args):
    """Handle the scrape-events command"""
    print("Sorry, this function is not implemented in the simplified version.")
    # Original code:
    # # Get parameters
    # source = args.source
    # days = args.days if args.days else 7
    # output = args.output
    # save_to_db = args.save_to_db
    # 
    # # Set up database if needed
    # db_connection = None
    # if save_to_db:
    #     chs_db = CharlestonDB()
    #     db_connection = chs_db.connection
    # 
    # # Get the list of available scrapers if none specified
    # if not source:
    #     scrapers = get_available_scrapers()
    #     print(f"Available scrapers: {', '.join(scrapers)}")
    #     return
    # 
    # # Run the scraper
    # success = run_scraper(source, days, output, db_connection)
    # 
    # if success:
    #     print(f"Successfully scraped events from {source}")
    # else:
    #     print(f"Failed to scrape events from {source}")


def update_vectors(args):
    """Update the vector database"""
    print("Updating vector database...")
    # TODO: Implement vector database update
    print("Vector database update not implemented yet.")


def list_events(args):
    """List events from the database"""
    db = CharlestonDB()
    
    print("Listing events from database...")
    
    # Get events based on options
    if args.date:
        events = db.get_events_by_date(args.date)
        print(f"Events on {args.date}:")
    elif args.query:
        events = db.search_events(args.query, limit=args.limit)
        print(f"Events matching '{args.query}':")
    else:
        events = db.get_all_events().head(args.limit)
        print(f"All events (limited to {args.limit}):")
    
    # Print events
    if events.empty:
        print("No events found.")
    else:
        for _, event in events.iterrows():
            date = event['date'] if not pd.isna(event['date']) else "Date unknown"
            time = event['time'] if not pd.isna(event['time']) else "Time unknown"
            location = event['location'] if not pd.isna(event['location']) else "Location unknown"
            
            print(f"- {event['name']}")
            print(f"  Date: {date}")
            print(f"  Time: {time}")
            print(f"  Location: {location}")
            print()


def create_venue_connections(args):
    """Create connections between events and businesses based on venue information"""
    print("Creating connections between events and businesses...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Check if the necessary files exist
    business_csv = os.path.join(project_root, 'data/charleston_businesses.csv')
    events_csv = os.path.join(project_root, 'data/charleston_events.csv')
    
    if not os.path.exists(business_csv):
        print(f"Business data file not found at {business_csv}")
        return False
        
    if not os.path.exists(events_csv):
        print(f"Events data file not found at {events_csv}")
        return False
    
    # Load the data
    businesses_df = pd.read_csv(business_csv)
    events_df = pd.read_csv(events_csv)
    
    print(f"Loaded {len(businesses_df)} businesses and {len(events_df)} events")
    
    # Import the necessary module for fuzzy matching
    try:
        from thefuzz import fuzz, process
    except ImportError:
        print("thefuzz module not found. Please install it with 'pip install thefuzz'")
        return False
    
    import re
    
    # Create lists of business names and locations for matching
    business_names = businesses_df['Name'].tolist()
    business_locations = businesses_df['Location'].tolist()
    
    # Clean business names for better matching
    clean_business_names = [
        re.sub(r'[^\w\s]', '', name).lower() for name in business_names
    ]
    
    # Add a business_id column to the events DataFrame
    events_df['Business_ID'] = None
    events_df['Business_Match_Type'] = None
    events_df['Business_Match_Score'] = None
    
    # Process each event
    for i, event in events_df.iterrows():
        location = str(event['Location']) if not pd.isna(event['Location']) else ""
        event_name = str(event['Name']) if not pd.isna(event['Name']) else ""
        
        # Skip if no location information
        if not location:
            continue
            
        # Clean the location string for better matching
        clean_location = re.sub(r'[^\w\s]', '', location).lower()
        
        # Check for explicit venue mentions first
        best_match = None
        best_score = 0
        match_type = None
        match_index = -1
        
        # Try to match on business name in the location field
        for j, business_name in enumerate(clean_business_names):
            if business_name and len(business_name) > 3:  # Avoid matching very short names
                score = fuzz.partial_ratio(clean_location, business_name)
                if score > 80 and score > best_score:  # Threshold of 80% match
                    best_score = score
                    best_match = business_names[j]
                    match_type = "location_to_name"
                    match_index = j
        
        # If no match found yet, try matching on business location
        if not best_match:
            for j, business_loc in enumerate(business_locations):
                if business_loc and not pd.isna(business_loc):
                    clean_business_loc = re.sub(r'[^\w\s]', '', str(business_loc)).lower()
                    score = fuzz.partial_ratio(clean_location, clean_business_loc)
                    if score > 80 and score > best_score:
                        best_score = score
                        best_match = business_names[j]
                        match_type = "location_to_location"
                        match_index = j
        
        # If still no match, check if the event name contains a business name
        if not best_match:
            clean_event_name = re.sub(r'[^\w\s]', '', event_name).lower()
            for j, business_name in enumerate(clean_business_names):
                if business_name and len(business_name) > 3:
                    score = fuzz.partial_ratio(clean_event_name, business_name)
                    if score > 85 and score > best_score:  # Higher threshold for name matches
                        best_score = score
                        best_match = business_names[j]
                        match_type = "name_to_name"
                        match_index = j
        
        # Record the match information if found
        if best_match and match_index >= 0:
            events_df.at[i, 'Business_ID'] = businesses_df.iloc[match_index].name
            events_df.at[i, 'Business_Match_Type'] = match_type
            events_df.at[i, 'Business_Match_Score'] = best_score
    
    # Save the enhanced events data
    print(f"Found business connections for {events_df['Business_ID'].notna().sum()} events")
    connections_csv = os.path.join(project_root, 'data/charleston_event_connections.csv')
    events_df.to_csv(connections_csv, index=False)
    print(f"Saved event-business connections to {connections_csv}")
    
    # Update the vector database if requested
    if args.update_vectors:
        print("Updating vector database with business connections...")
        merge_databases()
    
    return True


def build_vectors(args):
    """Build vector databases from various data sources"""
    if args.type not in ['business', 'events', 'places', 'combined', 'all']:
        print(f"Invalid vector database type: {args.type}")
        print("Available types: business, events, places, combined, all")
        return
    
    try:
        from utils.build_vector_db import (
            build_business_vector_db,
            build_events_vector_db,
            build_places_vector_db,
            merge_databases
        )
    except ImportError as e:
        print(f"Error importing vector database utilities: {e}")
        return
    
    if args.type == 'business' or args.type == 'all':
        print("Building business vector database...")
        build_business_vector_db()
    
    if args.type == 'events' or args.type == 'all':
        print("Building events vector database...")
        build_events_vector_db()
    
    if args.type == 'places' or args.type == 'all':
        print("Building places vector database...")
        build_places_vector_db()
    
    if args.type == 'combined' or args.type == 'all':
        print("Building combined vector database...")
        merge_databases()
    
    print("Vector database build complete.")


def scrape_businesses(args):
    """Scrape businesses from Yelp API"""
    # Check if API key is provided or in environment
    api_key = args.api_key or os.environ.get('YELP_API_KEY')
    if not api_key:
        print("Error: Yelp API key is required. Either provide it with --api-key or set the YELP_API_KEY environment variable.")
        return
        
    # Get database connection if saving to database
    db = None
    if args.save_to_db:
        db = CharlestonDB()
        
    try:
        # Initialize scraper
        print(f"Initializing Yelp Business Scraper...")
        scraper = YelpBusinessScraper(db_connection=db, api_key=api_key)
        
        # Determine categories to scrape
        categories = []
        if args.categories:
            categories = args.categories.split(',')
            print(f"Scraping businesses in categories: {', '.join(categories)}")
        else:
            categories = None  # Use default categories
            print(f"Scraping businesses in all default categories")
            
        # Set limit per category if provided
        if args.limit:
            print(f"Limiting to {args.limit} businesses per category")
            
        # Scrape businesses
        businesses = scraper.scrape_businesses(categories=categories)
        
        print(f"Successfully scraped {len(businesses)} businesses from Yelp")
        
        # Save to CSV if requested
        if args.output:
            csv_file = args.output
            print(f"Saving businesses to CSV file: {csv_file}")
            scraper.save_businesses_to_csv(businesses, filename=csv_file)
            
        # Save to database if requested
        if args.save_to_db:
            print(f"Saving businesses to database...")
            saved = scraper.save_to_database(businesses)
            print(f"Saved {saved} businesses to database")
            
        return businesses
        
    except Exception as e:
        print(f"Error scraping businesses: {e}")
        return None


def scrape_open_data(args):
    """Scrape data from Charleston Open Data Portal"""
    db = CharlestonDB() if args.save_to_db else None
    
    print("Scraping data from Charleston Open Data Portal...")
    scraper = OpenDataScraper(db_connection=db)
    
    # Determine which datasets to scrape
    if args.datasets:
        datasets = args.datasets.split(',')
        # Validate datasets
        available_datasets = list(scraper.DATASETS.keys())
        invalid_datasets = [d for d in datasets if d not in available_datasets]
        if invalid_datasets:
            print(f"Warning: Invalid datasets: {', '.join(invalid_datasets)}")
            print(f"Available datasets: {', '.join(available_datasets)}")
            datasets = [d for d in datasets if d in available_datasets]
            if not datasets:
                print("No valid datasets specified. Aborting.")
                return
    else:
        datasets = None  # Use all available datasets
    
    # Scrape the data
    data = scraper.scrape_datasets(datasets=datasets)
    
    # Count total features
    total_features = sum(len(features) for features in data.values())
    print(f"Scraped {total_features} features from {len(data)} datasets")
    
    # Save to CSV if requested
    if args.output_dir:
        print(f"Saving data to CSV files in {args.output_dir}...")
        scraper.save_to_csv(data, directory=args.output_dir)
    
    # Save to database if requested
    if args.save_to_db:
        saved_count = scraper.save_to_database(data)
        print(f"Saved {saved_count} records to database")
    
    return data


def migrate_to_places(args):
    """Migrate business data to the places table"""
    db = CharlestonDB()
    
    # Check if there are businesses to migrate
    businesses = db.get_all_businesses()
    business_count = len(businesses)
    
    if business_count == 0:
        print("No businesses found to migrate.")
        return
    
    print(f"Found {business_count} businesses to migrate to places table.")
    
    # Confirm with user if not forced
    if not args.force:
        confirmation = input(f"Are you sure you want to migrate {business_count} businesses to the places table? (y/n): ")
        if confirmation.lower() != 'y':
            print("Migration cancelled.")
            return
    
    # Start migration
    print("Migrating businesses to places table...")
    migrated_count = db.migrate_businesses_to_places()
    print(f"Successfully migrated {migrated_count} businesses to places table.")
    
    # Clear businesses table if requested
    if args.clear_businesses:
        print("Clearing businesses table...")
        cleared_count = db.clear_businesses()
        print(f"Cleared {cleared_count} records from businesses table.")
    
    # Print success message
    if migrated_count == business_count:
        print("Migration completed successfully! All businesses were migrated.")
    else:
        print(f"Migration completed with issues. {business_count - migrated_count} businesses could not be migrated.")
    
    # List types in places table
    places = db.get_all_places()
    if not places.empty:
        place_types = places['type'].value_counts().to_dict()
        print("\nPlaces by type:")
        for place_type, count in place_types.items():
            print(f"  - {place_type}: {count}")


def scrape_lowcountry(args):
    """Scrape businesses from Lowcountry Local First directory"""
    db = CharlestonDB() if args.save_to_db else None
    
    print("Scraping businesses from Lowcountry Local First directory...")
    scraper = LowcountryLocalFirstScraper(db_connection=db)
    
    # Determine which categories to scrape
    categories = None
    if args.categories:
        categories = args.categories.split(',')
        # Validate categories
        available_categories = scraper.CATEGORIES
        invalid_categories = [c for c in categories if c not in available_categories]
        if invalid_categories:
            print(f"Warning: Invalid categories: {', '.join(invalid_categories)}")
            print(f"Available categories: {', '.join(available_categories)}")
            categories = [c for c in categories if c in available_categories]
            if not categories:
                print("No valid categories specified. Aborting.")
                return
    
    # Set maximum number of businesses to scrape
    max_businesses = args.limit if args.limit and args.limit > 0 else None
    
    # Scrape businesses
    businesses = scraper.scrape_businesses(
        categories=categories, 
        max_businesses=max_businesses,
        save_to_file=args.output is not None
    )
    
    print(f"Scraped {len(businesses)} businesses")
    
    # Save to CSV if requested
    if args.output:
        print(f"Saving businesses to CSV file: {args.output}")
        scraper.save_to_csv(businesses, filename=args.output)
    
    return businesses


def scrape_lowcountry_selenium(args):
    # Check if we should save to database
    db_connection = None
    if args.save_to_db:
        chs_db = CharlestonDB()
        db_connection = chs_db.connection
        
    # Init scraper
    scraper = LowcountryLocalFirstSeleniumScraper(db_connection)
    
    # Validate category
    categories = []
    if args.categories:
        categories = [c.strip() for c in args.categories.split(',')]
    
    # Set limit
    limit = args.limit if args.limit else None
    
    # Get businesses from multiple categories or just one
    all_businesses = []
    
    if categories:
        for category in categories:
            print(f"Scraping businesses for category: {category}")
            businesses = scraper.scrape_businesses(category=category, limit=limit)
            all_businesses.extend(businesses)
    else:
        print("Scraping all businesses")
        all_businesses = scraper.scrape_businesses(limit=limit)
    
    # Save to CSV if specified
    if args.output_file:
        output_file = args.output_file
        if not output_file.endswith('.csv'):
            output_file = f"{output_file}.csv"
        scraper.save_to_csv(all_businesses, output_file)
    
    print(f"Done! Scraped {len(all_businesses)} businesses total.")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Charleston Concierge CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  - Initialize database: 
    python cli.py init-db
    
  - Scrape events from all sources:
    python cli.py scrape-events
    
  - Scrape events from a specific source:
    python cli.py scrape-events --source CharlestonCVBScraper
    
  - List events:
    python cli.py list-events
    
  - Search for events:
    python cli.py list-events --query "music"
    
  - List events on a specific date:
    python cli.py list-events --date 2024-05-01
    
  - Create venue connections:
    python cli.py create-connections
    
  - Build vector databases:
    python cli.py build-vectors --type all
    
  - Scrape open data:
    python cli.py scrape-open-data --datasets Parks,Landmarks
    
  - Migrate businesses to places:
    python cli.py migrate-to-places
    
  - Scrape local businesses:
    python cli.py scrape-lowcountry
    
  - Scrape Lowcountry Local First directory using Selenium:
    python cli.py scrape-lowcountry-selenium --categories "Restaurants,Bars" --limit 10 --output-file businesses_selenium.csv --save-to-db
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Initialize database command
    init_parser = subparsers.add_parser('init-db', help='Initialize the database')
    init_parser.add_argument('--force', action='store_true', help='Force reinitialization even if database exists')
    init_parser.add_argument('--skip-businesses', action='store_true', help='Skip business import and focus on events')
    init_parser.add_argument('--scrape-events', action='store_true', help='Scrape events after initializing database')
    
    # Scrape events command
    scrape_parser = subparsers.add_parser('scrape-events', help='Scrape events from sources')
    scrape_parser.add_argument('--source', help='Source to scrape (default: all)')
    scrape_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    scrape_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    scrape_parser.add_argument('--days', type=int, default=30, help='Number of days to scrape if end-date not provided')
    
    # List events command
    list_parser = subparsers.add_parser('list-events', help='List events from the database')
    list_parser.add_argument('--date', help='Show events for a specific date (YYYY-MM-DD)')
    list_parser.add_argument('--query', help='Search for events matching a keyword')
    list_parser.add_argument('--limit', type=int, default=10, help='Maximum number of events to list')
    
    # Create venue connections command
    connections_parser = subparsers.add_parser('create-connections', help='Create connections between events and businesses')
    connections_parser.add_argument('--update-vectors', action='store_true', help='Update vector database with the connections')
    
    # Build vector database command
    vectors_parser = subparsers.add_parser('build-vectors', help='Build or update vector databases')
    vectors_parser.add_argument('--type', choices=['business', 'events', 'places', 'combined', 'all'], 
                         default='all', help='Type of vector database to build')
    
    # Scrape businesses command
    businesses_parser = subparsers.add_parser('scrape-businesses', help='Scrape businesses from Yelp API')
    businesses_parser.add_argument('--api-key', help='Yelp API key')
    businesses_parser.add_argument('--categories', help='Comma-separated list of categories to scrape')
    businesses_parser.add_argument('--limit', type=int, help='Maximum number of businesses to scrape per category')
    businesses_parser.add_argument('--output', help='Output CSV file for businesses')
    businesses_parser.add_argument('--save-to-db', action='store_true', help='Save businesses to database')
    
    # Scrape open data command
    opendata_parser = subparsers.add_parser('scrape-open-data', help='Scrape data from Charleston Open Data Portal')
    opendata_parser.add_argument('--datasets', help='Comma-separated list of datasets to scrape (default: all)')
    opendata_parser.add_argument('--output-dir', help='Directory to save CSV files')
    opendata_parser.add_argument('--save-to-db', action='store_true', help='Save data to database')
    
    # Migrate to places command
    migrate_parser = subparsers.add_parser('migrate-to-places', help='Migrate business data to places table')
    migrate_parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    migrate_parser.add_argument('--clear-businesses', action='store_true', help='Clear businesses table after migration')
    
    # Scrape Lowcountry Local First command
    lowcountry_parser = subparsers.add_parser('scrape-lowcountry', help='Scrape businesses from Lowcountry Local First directory')
    lowcountry_parser.add_argument('--categories', help='Comma-separated list of categories to scrape (default: all)')
    lowcountry_parser.add_argument('--limit', type=int, help='Maximum number of businesses to scrape')
    lowcountry_parser.add_argument('--output', help='Output CSV file for businesses')
    lowcountry_parser.add_argument('--save-to-db', action='store_true', help='Save businesses to database')
    
    # Lowcountry Local First selenium scraper
    parser_scrape_lowcountry_selenium = subparsers.add_parser(
        'scrape-lowcountry-selenium',
        help='Scrape businesses from Lowcountry Local First directory using Selenium'
    )
    parser_scrape_lowcountry_selenium.add_argument(
        '--categories',
        help='Comma-separated list of categories to scrape'
    )
    parser_scrape_lowcountry_selenium.add_argument(
        '--limit',
        type=int,
        help='Maximum number of businesses to scrape'
    )
    parser_scrape_lowcountry_selenium.add_argument(
        '--output-file',
        help='Output CSV file name'
    )
    parser_scrape_lowcountry_selenium.add_argument(
        '--save-to-db',
        action='store_true',
        help='Save businesses to database'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute the appropriate command
    if args.command == 'init-db':
        init_db(args)
    elif args.command == 'scrape-events':
        scrape_events(args)
    elif args.command == 'list-events':
        list_events(args)
    elif args.command == 'create-connections':
        create_venue_connections(args)
    elif args.command == 'build-vectors':
        build_vectors(args)
    elif args.command == 'scrape-businesses':
        scrape_businesses(args)
    elif args.command == 'scrape-open-data':
        scrape_open_data(args)
    elif args.command == 'migrate-to-places':
        migrate_to_places(args)
    elif args.command == 'scrape-lowcountry':
        scrape_lowcountry(args)
    elif args.command == 'scrape-lowcountry-selenium':
        scrape_lowcountry_selenium(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1) 