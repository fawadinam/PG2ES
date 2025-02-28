import psycopg2
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, time
from decimal import Decimal
import json

# Load configuration from JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Function to serialize complex data types like datetime, time, and Decimal
def serialize_data(document):
    for key, value in document.items():
        if isinstance(value, datetime):
            document[key] = value.isoformat()
        elif isinstance(value, time):
            document[key] = str(value)
        elif isinstance(value, Decimal):
            document[key] = float(value)
        elif value is None:
            document[key] = None
    return document

# PostgreSQL connection details
db_config = config['postgres']

# Elasticsearch connection
es_config = config['elasticsearch']
es = Elasticsearch(
    [{'scheme': es_config['scheme'], 'host': es_config['host'], 'port': es_config['port']}],
    verify_certs=es_config['verify_certs'],
    basic_auth=(es_config['basic_auth']['username'], es_config['basic_auth']['password'])
)

# Check Elasticsearch connection
if not es.ping():
    raise ValueError("Connection to Elasticsearch failed")

# Function to check if index exists, and create it if it does not
def ensure_index_exists(es, index_name):
    if not es.indices.exists(index=index_name):
        print(f"Index '{index_name}' does not exist. Creating it...")
        es.indices.create(index=index_name)  # No predefined mappings, Elasticsearch will infer them
        print(f"Index '{index_name}' created successfully.")

# Function to generate a composite document ID
def generate_document_id(document, document_id_fields):
    if isinstance(document_id_fields, list):
        # Concatenate multiple fields to create a composite ID
        return "_".join(str(document[field]) for field in document_id_fields)
    else:
        # Use a single field as the ID
        return str(document[document_id_fields])

# Function to perform bulk indexing
def bulk_index_documents(es, index_name, documents, document_id_fields):
    actions = []
    for doc in documents:
        action = {
            "_op_type": "index",
            "_index": index_name,
            "_id": generate_document_id(doc, document_id_fields),  # Generate the document ID
            "_source": doc
        }
        actions.append(action)
    try:
        helpers.bulk(es, actions)  # Perform bulk indexing
    except Exception as e:
        print(f"Error during bulk indexing: {str(e)}")

# Function to transfer data for a single index
def transfer_data_for_index(es, db_config, index_config):
    index_name = index_config['index_name']
    query = index_config['query']
    document_id_fields = index_config['document_id']
    print(f"Processing index: {index_name} with document ID fields: {document_id_fields}")

    try:
        # Ensure the index exists before inserting documents
        ensure_index_exists(es, index_name)

        # Establish connection to PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all rows from the query result
        rows = cursor.fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]

        # Create a list of documents to index in bulk
        documents = []
        
        for row in rows:
            document = {column_names[i]: row[i] for i in range(len(column_names))}
            document = serialize_data(document)
            documents.append(document)

            if len(documents) >= 1000:
                bulk_index_documents(es, index_name, documents, document_id_fields)
                documents = []  # Clear the list

        if documents:
            bulk_index_documents(es, index_name, documents, document_id_fields)

        print(f"Documents successfully indexed into '{index_name}'.")

    except Exception as e:
        print(f"Error transferring data for index '{index_name}': {str(e)}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Iterate over all indexes and transfer data
for index_config in config['indexes']:
    transfer_data_for_index(es, db_config, index_config)
