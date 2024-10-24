import boto3
import re
import logging
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)

# query can be SELECT col1, col2, col3 FROM table WHERE col1 = 'avalue'
# or INSERT INTO table (col1, col2, col3) VALUES ('avalue', 'bvalue', 'cvalue')
# or UPDATE table SET col1 = 'avalue' WHERE col1 = 'avalue'
# or DELETE FROM table WHERE col1 = 'avalue'

class DynamoDBSQLWrapper:
    def __init__(self):
        self.ddb = boto3.client('dynamodb')
    
    def execute_query(self, query):
        query_type, parsed_query = self.parse_sql_query(query)
        ddb_params = self.sql_to_ddb_params(query_type, parsed_query)
        logger.info(f"DynamoDB parameters: {ddb_params}")
        if query_type == 'SELECT':
            return self.execute_select(ddb_params)
        elif query_type == 'INSERT':
            return self.execute_insert(ddb_params)
        elif query_type == 'UPDATE':
            return self.execute_update(ddb_params)
        elif query_type == 'DELETE':
            return self.execute_delete(ddb_params)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

    def execute_select(self, ddb_params):
        logger.info(f"Executing SELECT with params: {ddb_params}")
        response = self.ddb.scan(**ddb_params)
        return self.process_select_response(response['Items'], ddb_params.get('ProjectionExpression'))

    def process_select_response(self, items, projection_expression):
        if not items:
            return []

        processed_items = []
        for item in items:
            processed_item = {}
            for key, value in item.items():
                processed_item[key] = next(iter(value.values()))
            
            if projection_expression:
                # Filter the processed item based on the projection expression
                selected_columns = [col.strip() for col in projection_expression.split(',')]
                processed_item = {col: processed_item.get(col) for col in selected_columns if col in processed_item}
            
            processed_items.append(processed_item)
        
        return processed_items

    def execute_insert(self, ddb_params):
        response = self.ddb.put_item(**ddb_params)
        return response

    def execute_update(self, ddb_params):
        response = self.ddb.update_item(**ddb_params)
        return response

    def execute_delete(self, ddb_params):
        response = self.ddb.delete_item(**ddb_params)
        return response

    def parse_sql_query(self, sql_query: str) -> Tuple[str, Dict[str, Any]]:
        # Remove any leading/trailing whitespace and ensure the query ends with a semicolon
        sql_query = sql_query.strip()
        if not sql_query.endswith(';'):
            sql_query += ';'

        # Determine query type
        query_type = self.get_query_type(sql_query)
        
        if query_type == 'SELECT':
            return query_type, self.parse_select_query(sql_query)
        elif query_type == 'INSERT':
            return query_type, self.parse_insert_query(sql_query)
        elif query_type == 'UPDATE':
            return query_type, self.parse_update_query(sql_query)
        elif query_type == 'DELETE':
            return query_type, self.parse_delete_query(sql_query)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

    def get_query_type(self, sql_query: str) -> str:
        first_word = sql_query.split()[0].upper()
        if first_word in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']:
            return first_word
        else:
            raise ValueError(f"Unsupported query type: {first_word}")

    def parse_select_query(self, sql_query: str) -> Dict[str, Any]:
        # Remove any leading/trailing whitespace and ensure the query ends with a semicolon
        sql_query = sql_query.strip()
        if not sql_query.endswith(';'):
            sql_query += ';'

        # Regular expressions for parsing different parts of the SQL query
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        from_pattern = r'FROM\s+(.*?)(?:\s+WHERE|\s+ORDER BY|\s+LIMIT|;)'
        where_pattern = r'WHERE\s+(.*?)(?:\s+ORDER BY|\s+LIMIT|;)'
        order_by_pattern = r'ORDER BY\s+(.*?)(?:\s+LIMIT|;)'
        limit_pattern = r'LIMIT\s+(\d+)'

        # Parse the query
        parsed_query = {}
        
        # Extract SELECT clause
        select_match = re.search(select_pattern, sql_query, re.IGNORECASE)
        if select_match:
            select_columns = select_match.group(1).strip()
            if select_columns == '*':
                parsed_query['select'] = '*'
            else:
                parsed_query['select'] = [col.strip() for col in select_columns.split(',')]
            logger.info("Found SELECT clause")

        # Extract FROM clause
        from_match = re.search(from_pattern, sql_query, re.IGNORECASE)
        if from_match:  
            parsed_query['from'] = from_match.group(1).strip()
            logger.info("Found FROM clause")

        # Extract WHERE clause
        where_match = re.search(where_pattern, sql_query, re.IGNORECASE)
        if where_match:
            parsed_query['where'] = where_match.group(1).strip()
            logger.info("Found WHERE clause")

        # Extract ORDER BY clause
        order_by_match = re.search(order_by_pattern, sql_query, re.IGNORECASE)
        if order_by_match:
            parsed_query['order_by'] = order_by_match.group(1).strip()
            logger.info("Found ORDER BY clause")
            
        # Extract LIMIT clause
        limit_match = re.search(limit_pattern, sql_query, re.IGNORECASE)
        if limit_match:
            parsed_query['limit'] = int(limit_match.group(1))
            logger.info("Found LIMIT clause")
            
        return parsed_query

    def parse_insert_query(self, sql_query: str) -> Dict[str, Any]:
        insert_pattern = r'INSERT INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)'
        match = re.search(insert_pattern, sql_query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT query format")
        
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values = [val.strip() for val in match.group(3).split(',')]
        
        return {
            'table': table_name,
            'columns': columns,
            'values': values
        }

    def parse_update_query(self, sql_query: str) -> Dict[str, Any]:
        update_pattern = r'UPDATE\s+(\w+)\s+SET\s+(.*?)\s+WHERE\s+(.*)'
        match = re.search(update_pattern, sql_query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE query format")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3).rstrip(';')
        
        set_items = [item.strip() for item in set_clause.split(',')]
        
        return {
            'table': table_name,
            'set': set_items,
            'where': where_clause
        }

    def parse_delete_query(self, sql_query: str) -> Dict[str, Any]:
        delete_pattern = r'DELETE FROM\s+(\w+)\s+WHERE\s+(.*)'
        match = re.search(delete_pattern, sql_query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DELETE query format")
        
        table_name = match.group(1)
        where_clause = match.group(2).rstrip(';')
        
        return {
            'table': table_name,
            'where': where_clause
        }

    def sql_to_ddb_params(self, query_type: str, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        if query_type == 'SELECT':
            return self.select_to_ddb_params(parsed_query)
        elif query_type == 'INSERT':
            return self.insert_to_ddb_params(parsed_query)
        elif query_type == 'UPDATE':
            return self.update_to_ddb_params(parsed_query)
        elif query_type == 'DELETE':
            return self.delete_to_ddb_params(parsed_query)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

    def select_to_ddb_params(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        ddb_params = {}
        expression_attribute_values = {}

        if 'select' in parsed_query:
            if parsed_query['select'] != '*':
                ddb_params['ProjectionExpression'] = ', '.join(parsed_query['select'])

        if 'from' in parsed_query:
            ddb_params['TableName'] = parsed_query['from']

        if 'where' in parsed_query:
            filter_expression, attr_values = self.parse_where_clause(parsed_query['where'])
            ddb_params['FilterExpression'] = filter_expression
            expression_attribute_values.update(attr_values)

        if 'order_by' in parsed_query:
            ddb_params['ScanIndexForward'] = 'DESC' not in parsed_query['order_by'].upper()

        if 'limit' in parsed_query:
            ddb_params['Limit'] = parsed_query['limit']

        if expression_attribute_values:
            ddb_params['ExpressionAttributeValues'] = expression_attribute_values

        return ddb_params

    def parse_where_clause(self, where_clause: str) -> Tuple[str, Dict[str, Any]]:
        # Simple parsing for basic conditions
        conditions = where_clause.split(' AND ')
        filter_parts = []
        attr_values = {}

        for i, condition in enumerate(conditions):
            parts = condition.split()
            if len(parts) == 3:
                column, operator, value = parts
                placeholder = f":val{i}"
                filter_parts.append(f"{column} {self.translate_operator(operator)} {placeholder}")
                attr_values[placeholder] = self.parse_value(value)

        return ' AND '.join(filter_parts), attr_values

    def translate_operator(self, operator: str) -> str:
        operator_map = {
            '=': '=',
            '<>': '<>',
            '<': '<',
            '<=': '<=',
            '>': '>',
            '>=': '>=',
            'LIKE': 'contains',
            'IN': 'in'
        }
        return operator_map.get(operator.upper(), operator)

    def parse_value(self, value: str) -> Any:
        if value.startswith("'") and value.endswith("'"):
            return {'S': value[1:-1]}  # String
        elif value.isdigit():
            return {'N': value}  # Number
        elif value.lower() in ('true', 'false'):
            return {'BOOL': value.lower() == 'true'}  # Boolean
        else:
            return {'S': value}  # Default to string if unsure

    def insert_to_ddb_params(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'TableName': parsed_query['table'],
            'Item': dict(zip(parsed_query['columns'], parsed_query['values']))
        }

    def update_to_ddb_params(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        update_expression = "SET " + ", ".join(parsed_query['set'])
        return {
            'TableName': parsed_query['table'],
            'UpdateExpression': update_expression,
            'ConditionExpression': parsed_query['where']
            # Note: You'll need to add ExpressionAttributeValues and ExpressionAttributeNames
            # based on the specific requirements of your update operation
        }

    def delete_to_ddb_params(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'TableName': parsed_query['table'],
            'ConditionExpression': parsed_query['where']
            # Note: You'll need to add Key and potentially ExpressionAttributeValues
            # based on the specific requirements of your delete operation
        }
