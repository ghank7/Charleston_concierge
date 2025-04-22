"""
Charleston Open Data Scraper

This module provides a scraper for collecting data from Charleston's Open Data Portal
using the ArcGIS REST API.
"""

import os
import json
import time
import requests
import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta

from data_collection.scrapers import BaseEventScraper

# Setup logging
logger = logging.getLogger(__name__)

class OpenDataScraper(BaseEventScraper):
    """Scraper for collecting data from Charleston's Open Data Portal"""
    
    # Base URL for the ArcGIS REST API
    BASE_URL = "https://services2.arcgis.com/tQaXW7Zb1Vphzvgd/arcgis/rest/services"
    
    # Available datasets and their endpoints
    DATASETS = {
        "Parks": "Parks/FeatureServer/0",
        "CityLimits": "City_Limits/FeatureServer/0",
        "HistoricDistrict": "Old_and_Historic_District/FeatureServer/0",
        "NeighborhoodCouncils": "Neighborhood_Councils/FeatureServer/0",
        "CityAddresses": "City_Addresses/FeatureServer/0",
        "Landmarks": "Landmark_Overlay/FeatureServer/0"
    }
    
    def __init__(self, db_connection=None):
        """
        Initialize the Charleston Open Data Scraper
        
        Args:
            db_connection: Optional database connection
        """
        super().__init__(db_connection)
    
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "CharlestonOpenData"
    
    def get_events_for_date_range(self, start_date=None, end_date=None, days=30) -> List[str]:
        """
        This method is implemented to satisfy the BaseEventScraper ABC,
        but for open data, we'll just return the dataset list as our "urls" to scrape
        """
        return list(self.DATASETS.keys())
    
    def fetch_dataset(self, dataset_name: str) -> Dict:
        """
        Fetch data from a specific dataset
        
        Args:
            dataset_name: Name of the dataset to fetch (must be in DATASETS)
            
        Returns:
            Dictionary containing the response data
        """
        if dataset_name not in self.DATASETS:
            logger.error(f"Unknown dataset: {dataset_name}")
            return {}
            
        endpoint = f"{self.BASE_URL}/{self.DATASETS[dataset_name]}/query"
        
        params = {
            "where": "1=1",  # Get all records
            "outFields": "*",  # Get all fields
            "outSR": "4326",  # WGS84 coordinate system
            "f": "json"  # Return format is JSON
        }
        
        try:
            logger.info(f"Fetching {dataset_name} dataset from Charleston Open Data")
            response = requests.get(endpoint, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error fetching dataset {dataset_name}: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Exception during API call for {dataset_name}: {e}")
            return {}
    
    def scrape_event(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a specific dataset
        Note: We're overriding the scrape_event method to handle datasets
        instead of individual event URLs.
        
        Args:
            dataset_name: Name of the dataset to scrape
            
        Returns:
            Dictionary with dataset name and features, or None if error
        """
        try:
            logger.info(f"Scraping {dataset_name} dataset")
            
            data = self.fetch_dataset(dataset_name)
            if not data or "features" not in data:
                logger.error(f"No features found in {dataset_name} dataset")
                return None
                
            features = data.get("features", [])
            logger.info(f"Found {len(features)} features in {dataset_name} dataset")
            
            # Format the features for easier processing
            formatted_features = []
            for feature in features:
                # Skip features with no attributes
                if not feature.get("attributes"):
                    continue
                    
                # Get attributes
                attrs = feature.get("attributes", {})
                
                # Add geometry if available
                if "geometry" in feature:
                    geometry = feature.get("geometry", {})
                    attrs["geometry"] = geometry
                
                formatted_features.append(attrs)
            
            return {
                "dataset": dataset_name,
                "features": formatted_features
            }
            
        except Exception as e:
            logger.error(f"Error scraping dataset {dataset_name}: {e}")
            return None
    
    def scrape_datasets(self, datasets=None) -> Dict[str, List[Dict]]:
        """
        Scrape all or specific datasets
        
        Args:
            datasets: Optional list of dataset names to scrape. If None, uses all datasets.
            
        Returns:
            Dictionary mapping dataset names to lists of features
        """
        datasets_to_scrape = datasets or list(self.DATASETS.keys())
        
        results = {}
        for dataset in datasets_to_scrape:
            result = self.scrape_event(dataset)
            if result and "features" in result:
                results[dataset] = result["features"]
                
        return results
    
    def save_to_csv(self, data: Dict[str, List[Dict]], directory: str = "data") -> bool:
        """
        Save scraped data to CSV files
        
        Args:
            data: Dictionary mapping dataset names to lists of features
            directory: Directory to save the CSV files
            
        Returns:
            True if successful, False otherwise
        """
        if not data:
            logger.warning("No data to save")
            return False
            
        try:
            # Get the project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            # Create data directory if it doesn't exist
            data_dir = os.path.join(project_root, directory)
            os.makedirs(data_dir, exist_ok=True)
            
            for dataset_name, features in data.items():
                if not features:
                    continue
                    
                # Set the full path for the CSV file
                csv_path = os.path.join(data_dir, f"charleston_{dataset_name.lower()}.csv")
                
                # Convert to DataFrame and save to CSV
                df = pd.DataFrame(features)
                
                # Handle geometry column if present
                if "geometry" in df.columns:
                    df["geometry"] = df["geometry"].apply(lambda x: json.dumps(x) if x else "")
                    
                df.to_csv(csv_path, index=False)
                logger.info(f"Successfully saved {len(features)} records to {csv_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")
            return False
    
    def save_to_database(self, data: Dict[str, List[Dict]]) -> int:
        """
        Save scraped data to the database
        
        Args:
            data: Dictionary mapping dataset names to lists of features
            
        Returns:
            Number of records saved
        """
        if not self.db_connection:
            logger.warning("No database connection provided, can't save to database")
            return 0
            
        count = 0
        
        # Create POI data for the database from parks and landmarks
        pois = []
        
        # Process parks
        if "Parks" in data:
            for park in data["Parks"]:
                if not park.get("NAME"):
                    continue
                    
                # Format as a POI for the database
                poi = {
                    "name": park.get("NAME", ""),
                    "location": park.get("ADDRESS", ""),
                    "description": park.get("DESC_", "") or f"Park in {park.get('REGION', 'Charleston')}",
                    "website": park.get("URL", ""),
                    "image_url": "",
                    "source": self.source_name,
                    "type": "Park",  # Place type
                    "category": "Park, Recreation, Outdoor",
                    "details": {
                        "region": park.get("REGION", ""),
                        "operated_by": park.get("MAINTBY", ""),
                        "operating_days": park.get("OPERDAYS", ""),
                        "operating_hours": park.get("OPERHOURS", ""),
                        "amenities": self._get_amenities(park)
                    }
                }
                
                # Extract coordinates if available
                if "geometry" in park and park["geometry"].get("rings"):
                    # Take center point of first ring as approximate location
                    rings = park["geometry"]["rings"]
                    if rings and rings[0]:
                        points = rings[0]
                        if len(points) > 0:
                            # Calculate average of all points as center
                            lon_sum = sum(p[0] for p in points)
                            lat_sum = sum(p[1] for p in points)
                            count_points = len(points)
                            
                            poi["longitude"] = lon_sum / count_points
                            poi["latitude"] = lat_sum / count_points
                
                pois.append(poi)
        
        # Process landmarks
        if "Landmarks" in data:
            for landmark in data["Landmarks"]:
                if not landmark.get("NAME"):
                    continue
                    
                # Format as a POI for the database
                poi = {
                    "name": landmark.get("NAME", ""),
                    "location": landmark.get("ADDRESS", "") or "Charleston, SC",
                    "description": landmark.get("DESC_", "") or f"Historic landmark in Charleston",
                    "website": landmark.get("URL", ""),
                    "image_url": "",
                    "source": self.source_name,
                    "type": "Landmark",  # Place type
                    "category": "Landmark, Historic, Tourism",
                    "details": {
                        "historic_significance": landmark.get("SIGNIFICANCE", "")
                    }
                }
                
                # Extract coordinates if available
                if "geometry" in landmark and landmark["geometry"].get("rings"):
                    rings = landmark["geometry"]["rings"]
                    if rings and rings[0]:
                        points = rings[0]
                        if len(points) > 0:
                            # Calculate average of all points as center
                            lon_sum = sum(p[0] for p in points)
                            lat_sum = sum(p[1] for p in points)
                            count_points = len(points)
                            
                            poi["longitude"] = lon_sum / count_points
                            poi["latitude"] = lat_sum / count_points
                
                pois.append(poi)
        
        # Process City Limits as neighborhood information
        if "CityLimits" in data:
            poi = {
                "name": "City of Charleston",
                "location": "Charleston, SC",
                "description": "Official city limits of Charleston, South Carolina",
                "source": self.source_name,
                "type": "Region",  # Place type
                "category": "City, Administrative Area"
            }
            pois.append(poi)
        
        # Process neighborhoods
        if "NeighborhoodCouncils" in data:
            for neighborhood in data["NeighborhoodCouncils"]:
                if not neighborhood.get("NAME"):
                    continue
                
                poi = {
                    "name": neighborhood.get("NAME", "") + " Neighborhood",
                    "location": "Charleston, SC",
                    "description": f"Neighborhood area in Charleston: {neighborhood.get('NAME', '')}",
                    "source": self.source_name,
                    "type": "Neighborhood",  # Place type
                    "category": "Neighborhood, District, Community"
                }
                
                # Extract coordinates if available
                if "geometry" in neighborhood and neighborhood["geometry"].get("rings"):
                    rings = neighborhood["geometry"]["rings"]
                    if rings and rings[0]:
                        points = rings[0]
                        if len(points) > 0:
                            # Calculate average of all points as center
                            lon_sum = sum(p[0] for p in points)
                            lat_sum = sum(p[1] for p in points)
                            count_points = len(points)
                            
                            poi["longitude"] = lon_sum / count_points
                            poi["latitude"] = lat_sum / count_points
                
                pois.append(poi)
        
        # Save POIs to database
        for poi in pois:
            try:
                # Use add_place_from_dict if available
                if hasattr(self.db_connection, "add_place_from_dict"):
                    self.db_connection.add_place_from_dict(poi)
                    count += 1
                # Fallback to business methods if places not available
                elif hasattr(self.db_connection, "add_business_from_dict"):
                    self.db_connection.add_business_from_dict(poi)
                    count += 1
                elif hasattr(self.db_connection, "add_business"):
                    self.db_connection.add_business(
                        name=poi["name"],
                        location=poi["location"],
                        description=poi["description"],
                        url=poi.get("website", ""),
                        website=poi.get("website", ""),
                        image_url=poi.get("image_url", ""),
                        phone="",
                        email="",
                        source=poi.get("source", "")
                    )
                    count += 1
                elif hasattr(self.db_connection, "add_event_from_dict"):
                    # Convert to event format with fake date/time as last resort
                    event = {
                        "name": poi["name"],
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "time": "All day",
                        "location": poi["location"],
                        "description": poi["description"],
                        "url": poi.get("website", ""),
                        "image_url": poi.get("image_url", ""),
                        "source": poi.get("source", "")
                    }
                    self.db_connection.add_event_from_dict(event)
                    count += 1
                else:
                    logger.warning("Database connection doesn't have methods to add places, businesses, or events")
                    break
            except Exception as e:
                logger.error(f"Error saving POI {poi['name']} to database: {e}")
        
        return count
    
    def _get_amenities(self, park: Dict) -> List[str]:
        """Extract amenities from park data"""
        amenities = []
        
        amenity_fields = [
            "RESTROOM", "BASEBALL", "BASKETBALL", "SOCCER", 
            "PICNICAREA", "PLAYGROUND", "MULTIPURPOSEFIELD",
            "BOATING", "DOGPARK", "FISHING", "SKATE", 
            "TENNIS", "ULTIMATEGOLF", "CYCLING"
        ]
        
        for field in amenity_fields:
            if park.get(field) == "YES":
                # Convert field name to readable format
                amenity = field.replace("MULTIPURPOSEFIELD", "Multipurpose Field")
                amenity = amenity.replace("PICNICAREA", "Picnic Area")
                amenity = amenity.replace("DOGPARK", "Dog Park")
                amenity = amenity.capitalize()
                amenities.append(amenity)
                
        return amenities


# Test function to run the scraper directly
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run the scraper
    scraper = OpenDataScraper()
    
    # Scrape parks and landmarks as a test
    test_datasets = ["Parks", "Landmarks"]
    data = scraper.scrape_datasets(datasets=test_datasets)
    
    # Count total features
    total_features = sum(len(features) for features in data.values())
    print(f"Scraped {total_features} features from {len(data)} datasets")
    
    # Save to CSV
    scraper.save_to_csv(data)
    
    # Print first feature as a sample
    for dataset_name, features in data.items():
        if features:
            print(f"\nSample from {dataset_name}:")
            first_feature = features[0]
            for k, v in first_feature.items():
                if k != "geometry":  # Skip geometry for readability
                    print(f"{k}: {v}")
            break 