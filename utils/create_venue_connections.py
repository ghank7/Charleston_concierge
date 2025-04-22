import pandas as pd
import os
import numpy as np
from thefuzz import fuzz, process
import re

def create_venue_connections():
    """
    Create connections between events and businesses based on venue information.
    This helps connect events to business entities for more integrated recommendations.
    """
    print("Creating connections between events and businesses...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Check if the necessary files exist
    business_csv = os.path.join(project_root, 'data/charleston_businesses.csv')
    events_csv = os.path.join(project_root, 'data/charleston_events.csv')
    
    if not os.path.exists(business_csv):
        print(f"Business data file not found at {business_csv}")
        return None
        
    if not os.path.exists(events_csv):
        print(f"Events data file not found at {events_csv}")
        return None
    
    # Load the data
    businesses_df = pd.read_csv(business_csv)
    events_df = pd.read_csv(events_csv)
    
    print(f"Loaded {len(businesses_df)} businesses and {len(events_df)} events")
    
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
    
    return events_df

def enhance_combined_database():
    """
    Add relationship information to the combined vector database.
    This enriches events with business information and vice versa.
    """
    print("Enhancing the combined vector database with relationship information...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Check if the connections file exists
    connections_csv = os.path.join(project_root, 'data/charleston_event_connections.csv')
    if not os.path.exists(connections_csv):
        print("No connections file found. Run create_venue_connections first.")
        return None
    
    # We'll use the build_events_vector_db script's merge function with enhanced data
    from build_events_vector_db import merge_with_business_db
    
    # First, create the connections
    events_with_connections = pd.read_csv(connections_csv)
    
    # Now we can create the enhanced vector database
    # This will include the connections in the metadata
    merge_with_business_db(events_df=events_with_connections)
    
    print("Enhanced combined vector database created successfully!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create connections between events and businesses")
    parser.add_argument("--enhance", action="store_true", help="Enhance the combined vector database with connections")
    args = parser.parse_args()
    
    # Create connections first
    create_venue_connections()
    
    # Optionally enhance the vector database
    if args.enhance:
        enhance_combined_database() 