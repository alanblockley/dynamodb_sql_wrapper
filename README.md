# DynamoDBSQLWrapper

The DynamoDBSQLWrapper is a Python class that provides a SQL-like interface for interacting with Amazon DynamoDB. It allows users to execute SQL-like queries on DynamoDB tables, abstracting away the complexities of DynamoDB's native query language.

## Features

- Execute SELECT, INSERT, UPDATE, and DELETE queries on DynamoDB tables
- Support for basic WHERE clauses
- Basic JOIN functionality (both implicit and explicit joins)
- Parsing of SQL-like queries into DynamoDB operations
- Logging of operations for debugging and monitoring

## Installation

To install the DynamoDBSQLWrapper, use pip:

```bash
pip install dynamodb-sql-wrapper
```

## Usage

To use the DynamoDBSQLWrapper, first import and initialize the class:

```python
from dynamodb_sql_wrapper.mysql_to_ddb_class import DynamoDBSQLWrapper

wrapper = DynamoDBSQLWrapper()
```

Then, you can execute SQL-like queries:

# SELECT query
`result = wrapper.execute_query("SELECT column1, column2 FROM mytable WHERE column3 = 'value'")`

# INSERT query
`wrapper.execute_query("INSERT INTO mytable (column1, column2) VALUES ('value1', 'value2')")`

# UPDATE query
`wrapper.execute_query("UPDATE mytable SET column1 = 'new_value' WHERE column2 = 'condition'")`

# DELETE query
`wrapper.execute_query("DELETE FROM mytable WHERE column1 = 'value'")`

## Supported Query Types

1. SELECT: Retrieve data from one or more tables
2. INSERT: Add new data to a table
3. UPDATE: Modify existing data in a table
4. DELETE: Remove data from a table

## Limitations

- Complex SQL operations and functions are not supported
- JOIN operations are basic and may not cover all use cases
- Performance may vary for large datasets or complex queries

## Requirements

- boto3
- Python 3.6+

## Note

This wrapper is designed for simplifying DynamoDB operations and may not be suitable for all use cases. It's important to consider the underlying DynamoDB architecture and potential performance implications when using this wrapper in production environments.
