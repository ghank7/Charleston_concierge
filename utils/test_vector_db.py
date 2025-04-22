from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
import os

def test_vector_db():
    """Test the vector database by performing a sample query"""
    print("Loading vector database...")
    
    # Initialize the same embedding model used to create the database
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Load the existing database
    db_dir = os.path.join(project_root, "data/charleston_db")
    db = Chroma(persist_directory=db_dir, embedding_function=embeddings)
    
    # Check if database has documents
    collection = db.get()
    doc_count = len(collection['ids'])
    print(f"Database contains {doc_count} documents")
    
    if doc_count == 0:
        print("Error: No documents found in the database.")
        return False
        
    # Perform a few test queries
    test_queries = [
        "restaurants on the waterfront",
        "family activities in Charleston",
        "places to learn about history",
        "outdoor adventures"
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
            print(f"  Location: {doc.metadata['location']}")
            print(f"  Description snippet: {doc.page_content.split('Description: ')[1][:100]}...")
    
    print("\nVector database test complete!")
    return True

if __name__ == "__main__":
    test_vector_db() 