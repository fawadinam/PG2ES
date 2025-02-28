# PostgreSQL to Elasticsearch Data Transfer Tool

A Python utility for transferring data from PostgreSQL databases to Elasticsearch indices with support for custom queries, bulk indexing, and automatic handling of complex data types.

## Overview

This tool provides a configurable way to extract data from PostgreSQL databases using custom SQL queries and index it into Elasticsearch. It supports:

- Configurable database connections via a JSON configuration file
- Custom SQL queries to extract specific data
- Automatic creation of Elasticsearch indices if they don't exist
- Bulk indexing for improved performance
- Automatic serialization of complex data types (datetime, Decimal, etc.)
- Composite document IDs based on one or multiple fields

## Requirements

- Python 3.6+
- `psycopg2` library for PostgreSQL connections
- `elasticsearch` library for Elasticsearch interactions

## Installation

```bash
pip install psycopg2-binary elasticsearch
```

## Configuration

Create a `config.json` file with the following structure:

```json
{
  "postgres": {
    "host": "localhost",
    "port": 5432,
    "dbname": "your_database",
    "user": "your_username",
    "password": "your_password"
  },
  "elasticsearch": {
    "scheme": "https",
    "host": "localhost",
    "port": 9200,
    "verify_certs": true,
    "basic_auth": {
      "username": "elastic",
      "password": "your_password"
    }
  },
  "indexes": [
    {
      "index_name": "products",
      "query": "SELECT * FROM products WHERE active = true",
      "document_id": "product_id"
    },
    {
      "index_name": "customers",
      "query": "SELECT id, name, email, created_at FROM customers",
      "document_id": ["id", "email"]
    }
  ]
}
```

### Configuration Options

#### PostgreSQL Connection

Standard psycopg2 connection parameters:
- `host`: Database server hostname
- `port`: Database server port
- `dbname`: Database name
- `user`: Username
- `password`: Password

#### Elasticsearch Connection

- `scheme`: HTTP or HTTPS
- `host`: Elasticsearch server hostname
- `port`: Elasticsearch server port
- `verify_certs`: Whether to verify SSL certificates
- `basic_auth`: Authentication credentials
  - `username`: Elasticsearch username
  - `password`: Elasticsearch password

#### Indexes Configuration

For each index, define:
- `index_name`: Name of the target Elasticsearch index
- `query`: SQL query to fetch data from PostgreSQL
- `document_id`: Field or list of fields to use as the document ID
  - Single field: `"document_id": "id"`
  - Composite ID: `"document_id": ["id", "email"]`

## Usage

Run the script:

```bash
python postgres_to_es.py
```

## Features

### Automatic Data Type Handling

The tool automatically handles the conversion of complex PostgreSQL data types to JSON-compatible formats:
- `datetime` → ISO format string
- `time` → string
- `Decimal` → float
- `NULL` → `None`

### Bulk Indexing

Documents are indexed in batches of 1000 to optimize performance.

### Composite Document IDs

Supports creating document IDs from multiple fields by concatenating their values with underscores.

### Index Creation

Automatically creates Elasticsearch indices if they don't exist.

## Error Handling

The script includes error handling for:
- Elasticsearch connection failures
- Bulk indexing errors
- Database connection issues
- Query execution problems

## Limitations

- No schema mapping definitions (relies on Elasticsearch's dynamic mapping)
- No incremental updates (full data transfer each time)
- No parallelization of transfers across indices

## License

[Your license here]
