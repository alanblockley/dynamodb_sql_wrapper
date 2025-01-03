# Platypus DynamoDB SQL Wrapper

The DynamoDBSQLWrapper is a Python class that provides a SQL-like interface for interacting with Amazon DynamoDB. It allows users to execute SQL-like queries on DynamoDB tables, abstracting away the complexities of DynamoDB's native query language.

![logo](docs/logo.png "Platypus")

*Note: This is a work in progress and is not yet ready for production use.*

## Features

- Execute SELECT, INSERT, UPDATE, and DELETE queries on DynamoDB tables
- Support for basic WHERE clauses
- Basic JOIN functionality (both implicit and explicit joins)
- Parsing of SQL-like queries into DynamoDB operations
- Logging of operations for debugging and monitoring

## When would I use this?

This wrapper is useful when you want to use SQL-like syntax to interact with DynamoDB, but you don't want to learn DynamoDB's native query language.

This can be useful when you are migrating from a relational database to DynamoDB, or if you are using DynamoDB as a data store for reporting and business intelligence workloads.

Some legacy applications are built using SQL, and you want to gradually transition to DynamoDB without having to rewrite the queries.

In other cases, businesses may not have the resources to hire a team of DynamoDB experts, and may instead use this wrapper to allow their existing SQL-focused teams to interact with DynamoDB.

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

## Why platypus?

The name "Platypus" was chosen because it is a unique and mythical creature, representing the uniqueness of the DynamoDBSQLWrapper. 

When asked for feedback for this project, the first thing that came to mind was "Platypus". 
> "... it shouldn’t exist, but now that it does it kind of beautiful… even though it’s an abomination ..."