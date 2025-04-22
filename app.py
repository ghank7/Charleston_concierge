from flask import Flask, render_template, request, jsonify
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.chains import RetrievalQA
from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import os
import re
import datetime
import pandas as pd
from utils.database_manager import CharlestonDB

# Get the absolute paths to the database directories
db_path = os.path.abspath("data/charleston_db")
events_db_path = os.path.abspath("data/charleston_events_db")
combined_db_path = os.path.abspath("data/charleston_combined_db")

# Initialize Flask app
app = Flask(__name__)

# Initialize global variables
vector_db = None
events_db = None
combined_db = None
sql_db = None
qa_chain = None
llm = None
chat_chain = None  # New LLM chain for chat conversations

def init_rag_system():
    """Initialize the RAG system with vector database and SQL database"""
    global vector_db, events_db, combined_db, sql_db, qa_chain, llm, chat_chain
    
    print("Initializing RAG system...")
    
    # Initialize SQL database connection
    try:
        sql_db = CharlestonDB()
        print("SQL database connected!")
    except Exception as e:
        print(f"Failed to connect to SQL database: {e}")
        sql_db = None
    
    # Initialize embeddings
    embeddings = HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2",
        model_kwargs={'device': 'cpu'}
    )
    
    # First check if combined database exists
    if os.path.exists(combined_db_path):
        print("Loading combined database with businesses and events...")
        combined_db = Chroma(
            persist_directory=combined_db_path,
            embedding_function=embeddings
        )
    else:
        # Load the business vector database
        print("Loading business database...")
        vector_db = Chroma(
            persist_directory=db_path,
            embedding_function=embeddings
        )
        
        # Load events database if it exists
        if os.path.exists(events_db_path):
            print("Loading events database...")
            events_db = Chroma(
                persist_directory=events_db_path,
                embedding_function=embeddings
            )
    
    # Initialize Ollama for generative responses
    print("Initializing Ollama LLM...")
    try:
        # Try to use mistral (or whatever model you have installed)
        llm = Ollama(model="mistral")
        
        # Create a chatbot prompt template
        chat_template = """
        You are CharlestonConcierge, a helpful and friendly local guide for Charleston, South Carolina.
        Always start your response with a friendly greeting like "Awesome!" or "Happy to help!" or similar.

        Based on the user's query and the provided context, suggest relevant businesses or places.
        Present the results in a numbered list format.

        **Formatting Instructions:**
        1.  Provide a friendly opening phrase.
        2.  For the **first** item in the list, provide the name and location. Then, on new lines indented with spaces, provide:
            *   A detailed description.
            *   Any relevant events happening at that location (if mentioned in the context).
        3.  For **all subsequent** items (2nd, 3rd, etc.), provide only the name, location, and a brief description on a single line.
        4.  If no relevant information is found in the context, politely state that.

        **Context from Charleston Database:**
        {context}
        
        **User Query:**
        {question}
        
        **Your Formatted Response:**
        """
        
        chat_prompt = PromptTemplate(
            template=chat_template,
            input_variables=["context", "question"]
        )
        
        # Create a simple chat chain
        chat_chain = LLMChain(
            llm=llm,
            prompt=chat_prompt
        )
        
        # Also create a retrieval chain based on which database is available
        if combined_db:
            qa_chain = RetrievalQA.from_chain_type(
                llm=llm,
                chain_type="stuff",
                retriever=combined_db.as_retriever(search_kwargs={"k": 5})
            )
        elif vector_db:
            qa_chain = RetrievalQA.from_chain_type(
                llm=llm,
                chain_type="stuff",
                retriever=vector_db.as_retriever(search_kwargs={"k": 5})
            )
        
        print("Ollama LLM and chat chain initialized!")
    except Exception as e:
        print(f"Failed to initialize Ollama: {e}")
        print("Running without LLM capabilities")
        llm = None
        qa_chain = None
        chat_chain = None
    
    print("RAG system initialization complete!")

@app.route('/')
def index():
    """Render the main page with chat interface"""
    return render_template('chat.html')

@app.route('/api/chat', methods=['POST'])
def chat():
    """API endpoint for chat interaction"""
    data = request.json
    user_message = data.get('message', '')
    
    if not user_message:
        return jsonify({'error': 'No message provided'}), 400
    
    # Get relevant information from databases
    context = get_context_for_chat(user_message)
    
    # If we have a language model, use it to generate a response
    if chat_chain and llm:
        try:
            # Generate response using the chat chain
            response = chat_chain.run({
                "context": context,
                "question": user_message
            })
            
            return jsonify({
                'response': response,
                'has_results': len(context) > 0
            })
        except Exception as e:
            print(f"Error generating LLM response: {e}")
            # Fall back to simple response
            return jsonify({
                'response': "I'm having trouble generating a response right now. Please try again later.",
                'has_results': False
            })
    else:
        # Simple response if no LLM is available
        return jsonify({
            'response': "I don't have enough information about that yet. Try asking about Charleston businesses or events.",
            'has_results': False
        })

def get_context_for_chat(user_message):
    """Get relevant context from databases for the chat"""
    context = ""
    
    # Check if the query contains time references
    has_time_reference = contains_time_reference(user_message)
    
    # Get businesses from SQL
    if sql_db:
        # Get matching businesses
        businesses = sql_db.search_businesses(user_message, limit=3)
        
        if not businesses.empty:
            context += "BUSINESSES:\n"
            
            for _, business in businesses.iterrows():
                # Get categories
                categories_df = sql_db.get_business_categories(business['id'])
                categories = ", ".join(categories_df['name'].tolist()) if not categories_df.empty else "No categories"
                
                # Get events at this business
                business_events = sql_db.get_events_by_business(business['id'])
                
                context += f"- {business['name']}\n"
                context += f"  Location: {business['location'] if not pd.isna(business['location']) else 'Unknown location'}\n"
                context += f"  Description: {business['description'] if not pd.isna(business['description']) else 'No description available'}\n"
                context += f"  Categories: {categories}\n"
                
                # Add events for this business
                if not business_events.empty:
                    context += f"  Events at this location:\n"
                    for _, event in business_events.iterrows():
                        event_date = event['date'] if not pd.isna(event['date']) else "Date TBD"
                        event_time = event['time'] if not pd.isna(event['time']) else "Time TBD"
                        context += f"  - {event['name']} on {event_date} at {event_time}\n"
                
                context += "\n"
        
        # Get matching events
        events_query = user_message
        date_filter = None
        
        # Handle date-specific queries
        if has_time_reference:
            today = datetime.datetime.now()
            
            if "today" in user_message.lower() or "tonight" in user_message.lower():
                date_filter = today.strftime('%Y-%m-%d')
                events_query = re.sub(r'today|tonight', '', user_message, flags=re.IGNORECASE).strip()
            elif "tomorrow" in user_message.lower():
                tomorrow = today + datetime.timedelta(days=1)
                date_filter = tomorrow.strftime('%Y-%m-%d')
                events_query = re.sub(r'tomorrow', '', user_message, flags=re.IGNORECASE).strip()
            elif "this weekend" in user_message.lower() or "weekend" in user_message.lower():
                # Find next Saturday
                days_until_weekend = (5 - today.weekday()) % 7
                if days_until_weekend == 0:  # If today is Saturday
                    weekend_start = today
                else:
                    weekend_start = today + datetime.timedelta(days=days_until_weekend)
                # Set date filter to weekend start
                date_filter = weekend_start.strftime('%Y-%m-%d')
                events_query = re.sub(r'this weekend|weekend', '', user_message, flags=re.IGNORECASE).strip()
        
        # Get events based on date filter or query
        if date_filter:
            events = sql_db.get_events_by_date(date_filter)
            # If no events on exact date but we have a time reference, get all events
            if events.empty and has_time_reference:
                events = sql_db.search_events(events_query, limit=5)
        else:
            events = sql_db.search_events(events_query, limit=5)
        
        if not events.empty:
            context += "EVENTS:\n"
            
            for _, event in events.iterrows():
                venue_info = "No venue information"
                has_venue = not pd.isna(event['business_id'])
                
                if has_venue:
                    business = sql_db.get_business_by_id(int(event['business_id']))
                    if business is not None:
                        venue_info = f"{business['name']} ({business['location'] if not pd.isna(business['location']) else 'location unknown'})"
                
                context += f"- {event['name']}\n"
                context += f"  Date: {event['date'] if not pd.isna(event['date']) else 'Date TBD'}\n"
                context += f"  Time: {event['time'] if not pd.isna(event['time']) else 'Time TBD'}\n"
                context += f"  Location: {event['location'] if not pd.isna(event['location']) else 'Location TBD'}\n"
                context += f"  Venue: {venue_info}\n"
                context += f"  Description: {event['description'] if not pd.isna(event['description']) else 'No description available'}\n"
                context += f"  URL: {event['url'] if not pd.isna(event['url']) else 'No URL available'}\n"
                context += "\n"
    
    # If we have no SQL results, try the vector database
    if context == "" and (combined_db or vector_db or events_db):
        vector_results = []
        
        if combined_db:
            vector_matches = combined_db.similarity_search_with_relevance_scores(user_message, k=5)
            
            for doc, score in vector_matches:
                entity_type = doc.metadata.get('type', 'business')
                
                if entity_type == 'business':
                    vector_results.append({
                        'type': 'business',
                        'name': doc.metadata.get('name', 'Unknown business'),
                        'location': doc.metadata.get('location', 'Unknown location'),
                        'description': doc.page_content if 'Description:' not in doc.page_content else doc.page_content.split('Description: ')[1].split('\n')[0],
                        'score': score
                    })
                elif entity_type == 'event':
                    vector_results.append({
                        'type': 'event',
                        'name': doc.metadata.get('name', 'Unknown event'),
                        'date': doc.metadata.get('date', 'Date unknown'),
                        'time': doc.metadata.get('time', 'Time unknown'),
                        'location': doc.metadata.get('location', 'Location unknown'),
                        'description': doc.page_content if 'Description:' not in doc.page_content else doc.page_content.split('Description: ')[1].split('\n')[0],
                        'score': score
                    })
        
        # Format vector results as context
        if vector_results:
            businesses = [r for r in vector_results if r['type'] == 'business']
            events = [r for r in vector_results if r['type'] == 'event']
            
            if businesses:
                context += "BUSINESSES (from vector search):\n"
                for business in businesses:
                    context += f"- {business['name']}\n"
                    context += f"  Location: {business['location']}\n"
                    context += f"  Description: {business['description']}\n\n"
            
            if events:
                context += "EVENTS (from vector search):\n"
                for event in events:
                    context += f"- {event['name']}\n"
                    context += f"  Date: {event['date']}\n"
                    context += f"  Time: {event['time']}\n"
                    context += f"  Location: {event['location']}\n"
                    context += f"  Description: {event['description']}\n\n"
    
    return context

def contains_time_reference(query):
    """Check if the query contains time references like today, tonight, etc."""
    time_keywords = [
        'today', 'tonight', 'tomorrow', 'this weekend', 'weekend', 
        'this week', 'next week', 'monday', 'tuesday', 'wednesday', 
        'thursday', 'friday', 'saturday', 'sunday'
    ]
    
    # Convert query to lowercase for case-insensitive matching
    query_lower = query.lower()
    
    # Check for direct keyword matches
    for keyword in time_keywords:
        if keyword in query_lower:
            return True
    
    # Check for date patterns (e.g., MM/DD, Month DD)
    date_patterns = [
        r'\d{1,2}/\d{1,2}',  # MM/DD
        r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]* \d{1,2}'  # Month DD
    ]
    
    for pattern in date_patterns:
        if re.search(pattern, query_lower):
            return True
    
    return False

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """API endpoint to get all categories"""
    if sql_db is None:
        return jsonify({'error': 'SQL database not available'}), 500
    
    conn = sql_db.connect()
    query = "SELECT * FROM categories"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    categories = df.to_dict('records')
    return jsonify(categories)

@app.route('/api/businesses', methods=['GET'])
def get_businesses():
    """API endpoint to get businesses, with optional category filter"""
    if sql_db is None:
        return jsonify({'error': 'SQL database not available'}), 500
    
    category = request.args.get('category', None)
    limit = int(request.args.get('limit', 10))
    
    if category:
        businesses = sql_db.get_businesses_by_category(category, limit=limit)
    else:
        businesses = sql_db.get_all_businesses().head(limit)
    
    return jsonify(businesses.to_dict('records'))

@app.route('/api/business/<int:business_id>', methods=['GET'])
def get_business(business_id):
    """API endpoint to get a specific business"""
    if sql_db is None:
        return jsonify({'error': 'SQL database not available'}), 500
    
    business = sql_db.get_business_by_id(business_id)
    if business is None:
        return jsonify({'error': 'Business not found'}), 404
    
    # Get categories
    categories = sql_db.get_business_categories(business_id).to_dict('records')
    
    # Get events
    events = sql_db.get_events_by_business(business_id).to_dict('records')
    
    result = business.to_dict()
    result['categories'] = categories
    result['events'] = events
    
    return jsonify(result)

@app.route('/api/events', methods=['GET'])
def get_events():
    """API endpoint to get events, with optional date filter"""
    if sql_db is None:
        return jsonify({'error': 'SQL database not available'}), 500
    
    date = request.args.get('date', None)
    limit = int(request.args.get('limit', 10))
    
    if date:
        events = sql_db.get_events_by_date(date)
    else:
        events = sql_db.get_all_events().head(limit)
    
    return jsonify(events.to_dict('records'))

if __name__ == '__main__':
    # Initialize the RAG system before starting the app
    init_rag_system()
    
    # Start the Flask app
    app.run(debug=True, port=5001)
