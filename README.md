
# DynamoDB SQL Wrapper

This is a Python class that allows you to perform SQL-style operations such as `SELECT`, `WHERE`, `JOIN`, `INSERT`, and `UPDATE` on an Amazon DynamoDB table using `boto3`.

## Features

- **SELECT**: Retrieve specific columns from the DynamoDB table.
- **WHERE**: Apply conditions to filter the items in the table.
- **JOIN**: Simulate joins between two DynamoDB tables.
- **INSERT**: Add a new item to the DynamoDB table.
- **UPDATE**: Update an existing item in the DynamoDB table.

## Example Usage

### 1. Initialize the Wrapper

```python
# Initialize the DynamoDBSQLWrapper for a specific table
db_wrapper = DynamoDBSQLWrapper('YourTableName')
```

### 2. SELECT Operation

```python
# Select specific columns from the table
columns = ['id', 'name']
items = db_wrapper.select(columns)
print(items)
```

### 3. SELECT with WHERE Clause

```python
# Select items with a condition
columns = ['id', 'name']
conditions = "age > 30 AND city = 'Seattle'"
items = db_wrapper.select(columns, conditions)
print(items)
```

### 4. INSERT Operation

```python
# Insert a new item into the table
item = {
    'id': '123',
    'name': 'John Doe',
    'age': 30,
    'city': 'Seattle'
}
response = db_wrapper.insert(item)
print(response)
```

### 5. UPDATE Operation

```python
# Update an existing item
key = {'id': '123'}
updates = {'age': 31, 'city': 'San Francisco'}
response = db_wrapper.update(key, updates)
print(response)
```

### 6. JOIN Operation

```python
# Perform a join between two tables
join_condition = "id = related_id"
joined_data = db_wrapper.join('AnotherTable', join_condition)
print(joined_data)
```

## Requirements

- Python 3.11+
- `boto3` library for AWS DynamoDB interactions

## Installation

Install the required library:

```bash
pip install boto3
```

## License

This code is open-source and available under the MIT license.
# dynamodb_sql_wrapper
