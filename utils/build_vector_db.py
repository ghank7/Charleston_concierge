import pandas as pd
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.schema.document import Document
import os
import re

def clean_html(text):
    """Clean HTML entities from text"""
    if pd.isna(text):
        return ""
    # Replace HTML entities
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&#39;', "'", text)
    text = re.sub(r'&nbsp;', ' ', text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    return text

def filter_complex_metadata(metadata):
    """Filter out complex metadata that might cause issues with Chroma"""
    filtered_metadata = {}
    for key, value in metadata.items():
        # Only include primitive types
        if isinstance(value, (str, int, float, bool)) and value is not None:
            filtered_metadata[key] = value
    return filtered_metadata

def build_business_vector_db():
    """Build a vector database from business data"""
    print("Building business vector database...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, 'data/charleston_businesses.csv')
    
    # Check if the data file exists
    if not os.path.exists(data_path):
        print(f"Business data file not found at {data_path}")
        return None
    
    # Load and process data
    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} businesses from CSV")
    
    # Setup embedding model
    print("Setting up embedding model...")
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # Prepare documents for vector store
    print("Preparing documents...")
    documents = []
    for i, row in df.iterrows():
        # Skip entries with empty descriptions
        if pd.isna(row['Description']) or row['Description'] == "N/A":
            continue
            
        # Clean text
        description = clean_html(row['Description'])
        name = clean_html(row['Name'])
        location = clean_html(row['Location']) if not pd.isna(row['Location']) else ""
        
        # Create rich text that combines multiple fields
        content = f"Name: {name}\nLocation: {location}\nDescription: {description}"
        
        metadata = {
            "name": name,
            "location": location,
            "url": row["URL"] if not pd.isna(row["URL"]) else "",
            "website": row["Website"] if not pd.isna(row["Website"]) and row["Website"] != "N/A" else "",
            "image_url": row["Image_URL"] if not pd.isna(row["Image_URL"]) else "",
            "phone": row["Phone"] if not pd.isna(row["Phone"]) and row["Phone"] != "N/A" else "",
            "email": row["Email"] if not pd.isna(row["Email"]) and row["Email"] != "N/A" else "",
            "type": "business"  # Mark this as a business for filtering
        }
        
        # Filter out complex metadata
        metadata = filter_complex_metadata(metadata)
        
        documents.append(Document(page_content=content, metadata=metadata))
    
    print(f"Created {len(documents)} document objects")
    
    # Create vector store directory if it doesn't exist
    db_dir = os.path.join(project_root, "data/charleston_db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Create vector store
    print("Building vector database...")
    db = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        persist_directory=db_dir
    )
    db.persist()  # Make sure data is saved
    
    print(f"Vector database successfully built and saved to {db_dir}")
    return db

def build_events_vector_db():
    """Build and save the events vector database"""
    print("Loading events data...")
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    events_path = os.path.join(project_root, 'data/charleston_events.csv')
    
    # Check if events file exists
    if not os.path.exists(events_path):
        print(f"Events file not found at {events_path}")
        print("Please run event scrapers first to generate the events data.")
        return None
    
    df = pd.read_csv(events_path)
    
    print(f"Loaded {len(df)} events")
    
    # Setup embedding model
    print("Setting up embedding model...")
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # Prepare documents for vector store
    print("Preparing documents...")
    documents = []
    for i, row in df.iterrows():
        # Skip entries with empty descriptions
        if pd.isna(row['Description']) or not row['Description']:
            continue
            
        # Create rich text that combines multiple fields
        content = f"Name: {row['Name']}\n"
        content += f"Date: {row['Date']}\n" if not pd.isna(row['Date']) else ""
        content += f"Time: {row['Time']}\n" if not pd.isna(row['Time']) else ""
        content += f"Location: {row['Location']}\n" if not pd.isna(row['Location']) else ""
        content += f"Description: {row['Description']}"
        
        metadata = {
            "name": row["Name"],
            "date": row["Date"] if not pd.isna(row["Date"]) else "",
            "time": row["Time"] if not pd.isna(row["Time"]) else "",
            "location": row["Location"] if not pd.isna(row["Location"]) else "",
            "url": row["URL"] if not pd.isna(row["URL"]) else "",
            "image_url": row["Image_URL"] if not pd.isna(row["Image_URL"]) else "",
            "source": row["Source"] if not pd.isna(row["Source"]) else "",
            "type": "event"  # Mark this as an event for filtering
        }
        
        # Filter out complex metadata
        metadata = filter_complex_metadata(metadata)
        
        documents.append(Document(page_content=content, metadata=metadata))
    
    print(f"Created {len(documents)} event document objects")
    
    # Create vector store directory if it doesn't exist
    db_dir = os.path.join(project_root, "data/charleston_events_db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Create vector store
    print("Building vector database...")
    try:
        db = Chroma.from_documents(
            documents=documents,
            embedding=embeddings,
            persist_directory=db_dir
        )
        db.persist()  # Make sure data is saved
        print(f"Events vector database successfully built and saved to {db_dir}")
        return db
    except Exception as e:
        print(f"Error building vector database: {e}")
        return None

def build_places_vector_db():
    """Build a vector database from places data in the database"""
    print("Building places vector database...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Import database manager
    from utils.database_manager import CharlestonDB
    
    # Get places data from the database
    db = CharlestonDB()
    places_df = db.get_all_places()
    
    if places_df.empty:
        print("No places data found in the database")
        return None
    
    print(f"Loaded {len(places_df)} places from database")
    
    # Setup embedding model
    print("Setting up embedding model...")
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # Prepare documents for vector store
    print("Preparing documents...")
    documents = []
    for i, row in places_df.iterrows():
        # Skip entries with empty descriptions
        if pd.isna(row['description']) or row['description'] == "N/A":
            continue
            
        # Clean text
        description = clean_html(row['description'])
        name = clean_html(row['name'])
        location = clean_html(row['location']) if not pd.isna(row['location']) else ""
        place_type = row['type'] if not pd.isna(row['type']) else "place"
        
        # Create rich text that combines multiple fields
        content = f"Name: {name}\nType: {place_type}\nLocation: {location}\nDescription: {description}"
        
        # Parse metadata JSON if exists
        import json
        metadata_dict = {}
        if not pd.isna(row['metadata']) and row['metadata']:
            try:
                metadata_dict = json.loads(row['metadata'])
            except:
                pass  # If JSON parsing fails, use empty dict
        
        metadata = {
            "name": name,
            "location": location,
            "type": place_type.lower(),  # Type of place (park, landmark, etc.)
            "url": row["website"] if not pd.isna(row["website"]) else "",
            "website": row["website"] if not pd.isna(row["website"]) else "",
            "image_url": row["image_url"] if not pd.isna(row["image_url"]) else "",
            "phone": row["phone"] if not pd.isna(row["phone"]) else "",
            "email": row["email"] if not pd.isna(row["email"]) else "",
            "source": row["source"] if not pd.isna(row["source"]) else "",
            "category": row["category"] if not pd.isna(row["category"]) else "",
            "latitude": float(row["latitude"]) if not pd.isna(row["latitude"]) else None,
            "longitude": float(row["longitude"]) if not pd.isna(row["longitude"]) else None,
        }
        
        # Add additional metadata from JSON
        metadata.update(metadata_dict)
        
        # Filter out complex metadata
        metadata = filter_complex_metadata(metadata)
        
        documents.append(Document(page_content=content, metadata=metadata))
    
    print(f"Created {len(documents)} document objects")
    
    # Create vector store directory if it doesn't exist
    db_dir = os.path.join(project_root, "data/charleston_places_db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Create vector store
    print("Building vector database...")
    db = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        persist_directory=db_dir
    )
    db.persist()  # Make sure data is saved
    
    print(f"Places vector database successfully built and saved to {db_dir}")
    return db

def merge_databases():
    """
    Create a combined vector database with business, event, and places information
    """
    print("Creating combined vector database with businesses, events, and places...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Import database manager for places
    from utils.database_manager import CharlestonDB
    
    # Check if data sources exist
    business_csv = os.path.join(project_root, 'data/charleston_businesses.csv')
    events_csv = os.path.join(project_root, 'data/charleston_events.csv')
    
    # Check if places exist in database
    db = CharlestonDB()
    has_places = len(db.get_all_places()) > 0
    
    # Validate we have at least one data source
    if not os.path.exists(business_csv) and not os.path.exists(events_csv) and not has_places:
        print("Missing data sources. Cannot create combined database.")
        return None
    
    # Setup embedding model
    print("Setting up embedding model...")
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # Prepare documents list
    all_documents = []
    
    # Add business documents if available
    if os.path.exists(business_csv):
        print("Loading business data...")
        businesses_df = pd.read_csv(business_csv)
        print(f"Loaded {len(businesses_df)} businesses")
        
        for i, row in businesses_df.iterrows():
            # Skip entries with empty descriptions
            if pd.isna(row['Description']) or row['Description'] == "N/A":
                continue
                
            # Clean text
            description = clean_html(row['Description'])
            name = clean_html(row['Name'])
            location = clean_html(row['Location']) if not pd.isna(row['Location']) else ""
            
            # Create rich text that combines multiple fields
            content = f"Name: {name}\nType: Business\nLocation: {location}\nDescription: {description}"
            
            metadata = {
                "name": name,
                "location": location,
                "url": row["URL"] if not pd.isna(row["URL"]) else "",
                "website": row["Website"] if not pd.isna(row["Website"]) and row["Website"] != "N/A" else "",
                "image_url": row["Image_URL"] if not pd.isna(row["Image_URL"]) else "",
                "phone": row["Phone"] if not pd.isna(row["Phone"]) and row["Phone"] != "N/A" else "",
                "email": row["Email"] if not pd.isna(row["Email"]) and row["Email"] != "N/A" else "",
                "type": "business"  # Mark this as a business for filtering
            }
            
            # Filter out complex metadata
            metadata = filter_complex_metadata(metadata)
            
            all_documents.append(Document(page_content=content, metadata=metadata))
        
        print(f"Added {len(all_documents)} business documents")
    
    # Add event documents if available
    if os.path.exists(events_csv):
        print("Loading events data...")
        events_df = pd.read_csv(events_csv)
        print(f"Loaded {len(events_df)} events")
        
        events_docs_count = 0
        for i, row in events_df.iterrows():
            # Skip entries with empty descriptions
            if pd.isna(row['Description']) or not row['Description']:
                continue
                
            # Create rich text that combines multiple fields
            content = f"Name: {row['Name']}\nType: Event\n"
            content += f"Date: {row['Date']}\n" if not pd.isna(row['Date']) else ""
            content += f"Time: {row['Time']}\n" if not pd.isna(row['Time']) else ""
            content += f"Location: {row['Location']}\n" if not pd.isna(row['Location']) else ""
            content += f"Description: {row['Description']}"
            
            metadata = {
                "name": row["Name"],
                "date": row["Date"] if not pd.isna(row["Date"]) else "",
                "time": row["Time"] if not pd.isna(row["Time"]) else "",
                "location": row["Location"] if not pd.isna(row["Location"]) else "",
                "url": row["URL"] if not pd.isna(row["URL"]) else "",
                "image_url": row["Image_URL"] if not pd.isna(row["Image_URL"]) else "",
                "source": row["Source"] if not pd.isna(row["Source"]) else "",
                "type": "event"  # Mark this as an event for filtering
            }
            
            # Filter out complex metadata
            metadata = filter_complex_metadata(metadata)
            
            all_documents.append(Document(page_content=content, metadata=metadata))
            events_docs_count += 1
        
        print(f"Added {events_docs_count} event documents")
    
    # Add places documents if available
    if has_places:
        print("Loading places data...")
        places_df = db.get_all_places()
        print(f"Loaded {len(places_df)} places")
        
        places_docs_count = 0
        for i, row in places_df.iterrows():
            # Skip entries with empty descriptions
            if pd.isna(row['description']) or row['description'] == "N/A":
                continue
                
            # Clean text
            description = clean_html(row['description'])
            name = clean_html(row['name'])
            location = clean_html(row['location']) if not pd.isna(row['location']) else ""
            place_type = row['type'] if not pd.isna(row['type']) else "place"
            
            # Create rich text that combines multiple fields
            content = f"Name: {name}\nType: {place_type}\nLocation: {location}\nDescription: {description}"
            
            # Parse metadata JSON if exists
            import json
            metadata_dict = {}
            if not pd.isna(row['metadata']) and row['metadata']:
                try:
                    metadata_dict = json.loads(row['metadata'])
                except:
                    pass  # If JSON parsing fails, use empty dict
            
            metadata = {
                "name": name,
                "location": location,
                "type": place_type.lower(),
                "url": row["website"] if not pd.isna(row["website"]) else "",
                "website": row["website"] if not pd.isna(row["website"]) else "",
                "image_url": row["image_url"] if not pd.isna(row["image_url"]) else "",
                "phone": row["phone"] if not pd.isna(row["phone"]) else "",
                "email": row["email"] if not pd.isna(row["email"]) else "",
                "source": row["source"] if not pd.isna(row["source"]) else "",
                "category": row["category"] if not pd.isna(row["category"]) else "",
            }
            
            # Add coordinates if available
            if not pd.isna(row["latitude"]) and not pd.isna(row["longitude"]):
                metadata["latitude"] = float(row["latitude"])
                metadata["longitude"] = float(row["longitude"])
            
            # Add additional metadata from JSON
            metadata.update(metadata_dict)
            
            # Filter out complex metadata
            metadata = filter_complex_metadata(metadata)
            
            all_documents.append(Document(page_content=content, metadata=metadata))
            places_docs_count += 1
        
        print(f"Added {places_docs_count} place documents")
    
    print(f"Total documents for combined database: {len(all_documents)}")
    
    if not all_documents:
        print("No documents to add to the combined database")
        return None
    
    # Create vector store directory if it doesn't exist
    db_dir = os.path.join(project_root, "data/charleston_combined_db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Create vector store
    print("Building combined vector database...")
    try:
        db = Chroma.from_documents(
            documents=all_documents,
            embedding=embeddings,
            persist_directory=db_dir
        )
        db.persist()  # Make sure data is saved
        print(f"Combined vector database successfully built and saved to {db_dir}")
        return db
    except Exception as e:
        print(f"Error building combined vector database: {e}")
        return None

def test_vector_db(db_type="business"):
    """Test the vector database by performing a sample query"""
    print(f"Testing {db_type} vector database...")
    
    # Initialize the same embedding model used to create the database
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Load the existing database
    if db_type == "business":
        db_dir = os.path.join(project_root, "data/charleston_db")
    elif db_type == "events":
        db_dir = os.path.join(project_root, "data/charleston_events_db")
    else:  # combined
        db_dir = os.path.join(project_root, "data/charleston_combined_db")
    
    if not os.path.exists(db_dir):
        print(f"Error: {db_type} database directory not found at {db_dir}")
        return False
    
    db = Chroma(persist_directory=db_dir, embedding_function=embeddings)
    
    # Check if database has documents
    collection = db.get()
    doc_count = len(collection['ids'])
    print(f"Database contains {doc_count} documents")
    
    if doc_count == 0:
        print("Error: No documents found in the database.")
        return False
        
    # Perform a few test queries
    if db_type == "business":
        test_queries = [
            "restaurants on the waterfront",
            "family activities in Charleston",
            "places to learn about history",
            "outdoor adventures"
        ]
    elif db_type == "events":
        test_queries = [
            "music events this weekend",
            "food festivals",
            "family friendly events",
            "cultural exhibitions"
        ]
    else:  # combined
        test_queries = [
            "restaurants with upcoming events",
            "outdoor music in Charleston",
            "historic tours with good ratings",
            "weekend activities for families"
        ]
    
    print("\nPerforming test queries:")
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        # Retrieve similar documents
        results = db.similarity_search_with_relevance_scores(query, k=3)
        
        if not results:
            print("  No results found")
            continue
            
        # Display results
        for doc, score in results:
            print(f"  Result: {doc.metadata['name']} (Score: {score:.4f})")
            if "location" in doc.metadata:
                print(f"  Location: {doc.metadata['location']}")
            if "date" in doc.metadata:
                print(f"  Date: {doc.metadata['date']}")
            
            # Get description snippet
            if "Description: " in doc.page_content:
                desc = doc.page_content.split('Description: ')[1]
                print(f"  Description snippet: {desc[:100]}...")
            else:
                print(f"  Content snippet: {doc.page_content[:100]}...")
    
    print(f"\n{db_type.capitalize()} vector database test complete!")
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Build and test vector databases")
    parser.add_argument("--type", choices=["business", "events", "combined", "all", "test"], 
                        default="business", help="Type of vector database to build")
    args = parser.parse_args()
    
    if args.type == "business" or args.type == "all":
        build_business_vector_db()
        
    if args.type == "events" or args.type == "all":
        build_events_vector_db()
        
    if args.type == "places" or args.type == "all":
        build_places_vector_db()
        
    if args.type == "combined" or args.type == "all":
        merge_databases()
        
    if args.type == "test":
        db_choice = input("Which database to test? [business/events/combined]: ").strip().lower()
        if db_choice in ["business", "events", "combined"]:
            test_vector_db(db_choice)
        else:
            print("Invalid choice. Please select 'business', 'events', or 'combined'.") 