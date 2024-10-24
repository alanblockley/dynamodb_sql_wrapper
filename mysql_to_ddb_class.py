import boto3
import re
import logging
from typing import Dict, Any, Tuple, List
import itertools

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# query can be SELECT col1, col2, col3 FROM table WHERE col1 = 'avalue'
# or INSERT INTO table (col1, col2, col3) VALUES ('avalue', 'bvalue', 'cvalue')
# or UPDATE table SET col1 = 'avalue' WHERE col1 = 'avalue'
# or DELETE FROM table WHERE col1 = 'avalue'

class DynamoDBSQLWrapper:
    def __init__(self):
        self.ddb = boto3.client('dynamodb')
    
    def execute_query(self, query):
        logger.info(f"Executing query: {query}")
        query_type, parsed_query = self.parse_sql_query(query)
        logger.info(f"Query type: {query_type}")
        ddb_params = self.sql_to_ddb_params(query_type, parsed_query)
        logger.info("DynamoDB parameters prepared")
        
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
        if 'join' in ddb_params:
            return self.execute_join_select(ddb_params)
        else:
            scan_params = {
                'TableName': ddb_params['TableName']
            }
            if 'ProjectionExpression' in ddb_params:
                scan_params['ProjectionExpression'] = ddb_params['ProjectionExpression']
            if 'FilterExpression' in ddb_params and ddb_params.get('ExpressionAttributeValues'):
                scan_params['FilterExpression'] = ddb_params['FilterExpression']
                scan_params['ExpressionAttributeValues'] = ddb_params['ExpressionAttributeValues']
            
            logger.info(f"Executing scan on table: {scan_params['TableName']}")
            response = self.ddb.scan(**scan_params)
            logger.info(f"Scan complete. Items retrieved: {len(response['Items'])}")
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
        sql_query = sql_query.strip()
        if not sql_query.endswith(';'):
            sql_query += ';'

        query_type = self.get_query_type(sql_query)
        
        if query_type == 'SELECT':
            parsed_query = self.parse_select_query(sql_query)
            logger.info("SELECT query parsed successfully")
            return query_type, parsed_query
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
        parsed_query = {}

        # Remove any trailing semicolon
        sql_query = sql_query.strip().rstrip(';')

        # Extract SELECT part
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_columns = [col.strip() for col in select_match.group(1).split(',')]
            parsed_query['select'] = select_columns
        else:
            logger.error("Failed to parse SELECT clause")
            raise ValueError("Invalid SQL: SELECT clause not found")

        # Extract FROM part
        from_match = re.search(r'FROM\s+(.*?)(?:\s+WHERE|$)', sql_query, re.IGNORECASE)
        if from_match:
            from_tables = [table.strip() for table in from_match.group(1).split(',')]
            parsed_query['from'] = from_tables
        else:
            logger.error("Failed to parse FROM clause")
            raise ValueError("Invalid SQL: FROM clause not found")

        # Extract WHERE part
        where_match = re.search(r'WHERE\s+(.*)', sql_query, re.IGNORECASE)
        if where_match:
            parsed_query['where'] = where_match.group(1)
        else:
            logger.info("No WHERE clause found")

        logger.info(f"Parsed SELECT query: {parsed_query}")
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
        
        if 'select' in parsed_query:
            projection_expression = ', '.join([col.split('.')[-1] for col in parsed_query['select']])
            ddb_params['ProjectionExpression'] = projection_expression

        if 'from' in parsed_query and parsed_query['from']:
            ddb_params['TableName'] = parsed_query['from'][0]  # Use the first table as main table
            if len(parsed_query['from']) > 1:
                ddb_params['join'] = {'tables': parsed_query['from'][1:]}

        if 'where' in parsed_query and parsed_query['where']:
            filter_expression, attr_values = self.parse_where_clause(parsed_query['where'], parsed_query.get('from', []))
            if filter_expression and attr_values:
                ddb_params['FilterExpression'] = filter_expression
                ddb_params['ExpressionAttributeValues'] = attr_values

        logger.info("DynamoDB parameters generated")
        return ddb_params

    def parse_where_clause(self, where_clause: str, tables: List[str]) -> Tuple[str, Dict[str, Any]]:
        if not where_clause:
            logger.info("No WHERE clause to parse")
            return '', {}

        conditions = where_clause.split(' AND ')
        filter_parts = []
        attr_values = {}
        join_conditions = []

        for i, condition in enumerate(conditions):
            parts = condition.split('=')
            if len(parts) == 2:
                left, right = parts
                left = left.strip()
                right = right.strip()

                left_table, left_col = self.split_table_column(left, tables)
                right_table, right_col = self.split_table_column(right, tables)

                if left_table and right_table and left_table != right_table:
                    # This is a join condition
                    join_conditions.append(f"{left} = {right}")
                else:
                    placeholder = f":val{i}"
                    filter_parts.append(f"{left_col} = {placeholder}")
                    attr_values[placeholder] = {'S': right_col.strip("'")}

        logger.info(f"WHERE clause parsed. Conditions count: {len(conditions)}")
        return ' AND '.join(filter_parts + join_conditions), attr_values

    def split_table_column(self, identifier: str, tables: List[str]) -> Tuple[str, str]:
        parts = identifier.split('.')
        if len(parts) == 2 and parts[0] in tables:
            return parts[0], parts[1]
        elif len(parts) == 1:
            return '', parts[0]
        else:
            return '', identifier

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

    def execute_join_select(self, ddb_params):
        if ddb_params['join'].get('type') == 'implicit':
            return self.execute_implicit_join_select(ddb_params)
        else:
            return self.execute_explicit_join_select(ddb_params)

    def execute_implicit_join_select(self, ddb_params):
        table_responses = {}
        for table in ddb_params['from']:
            scan_params = {'TableName': table}
            
            if 'FilterExpression' in ddb_params and ddb_params['ExpressionAttributeValues']:
                table_filter = self.get_table_specific_filter(ddb_params['FilterExpression'], table)
                if table_filter:
                    scan_params['FilterExpression'] = table_filter
                    scan_params['ExpressionAttributeValues'] = ddb_params['ExpressionAttributeValues']
            
            logger.info(f"Scanning table: {table}")
            table_responses[table] = self.ddb.scan(**scan_params)

        for table, response in table_responses.items():
            logger.info(f"Retrieved {len(response['Items'])} items from table {table}")

        processed_items = {
            table: self.process_select_response(response['Items'], ddb_params.get('ProjectionExpression'))
            for table, response in table_responses.items()
        }

        joined_items = self.perform_implicit_join(processed_items, ddb_params.get('FilterExpression'), ddb_params.get('ExpressionAttributeValues', {}))

        logger.info(f"Join operation complete. Result items: {len(joined_items)}")
        return joined_items

    def get_table_specific_filter(self, filter_expression, table):
        conditions = filter_expression.split(' AND ')
        table_conditions = [cond for cond in conditions if table in cond or '.' not in cond]
        return ' AND '.join(table_conditions) if table_conditions else None

    def perform_implicit_join(self, processed_items, filter_expression, expression_attribute_values):
        tables = list(processed_items.keys())
        if len(tables) < 2:
            logger.info("Not enough tables for join operation")
            return processed_items[tables[0]]

        joined_items = processed_items[tables[0]]
        for table in tables[1:]:
            new_joined_items = []
            for item1 in joined_items:
                for item2 in processed_items[table]:
                    if self.items_match(item1, item2, filter_expression, expression_attribute_values, tables):
                        new_joined_items.append({**item1, **item2})
            joined_items = new_joined_items

        logger.info(f"Join operation complete. Result items: {len(joined_items)}")
        return joined_items

    def items_match(self, item1, item2, filter_expression, expression_attribute_values, tables):
        if not filter_expression:
            return True
        conditions = filter_expression.split(' AND ')
        for condition in conditions:
            parts = condition.split('=')
            if len(parts) == 2:
                left, right = parts
                left = left.strip()
                right = right.strip()
                
                left_table, left_col = self.split_table_column(left, tables)
                right_table, right_col = self.split_table_column(right, tables)
                
                if left_table and right_table and left_table != right_table:
                    # This is a join condition
                    if item1.get(left_col) != item2.get(right_col):
                        return False
                elif left_col in item1 or left_col in item2:
                    value = expression_attribute_values.get(right, {}).get('S', right_col)
                    if item1.get(left_col) != value and item2.get(left_col) != value:
                        return False

        return True

    def execute_explicit_join_select(self, ddb_params):
        # Scan both tables
        main_table_response = self.ddb.scan(TableName=ddb_params['TableName'])
        join_table_response = self.ddb.scan(TableName=ddb_params['join']['table'])

        # Process the responses
        main_items = self.process_select_response(main_table_response['Items'], ddb_params.get('ProjectionExpression'))
        join_items = self.process_select_response(join_table_response['Items'], None)

        # Perform the join operation
        joined_items = self.perform_join(main_items, join_items, ddb_params['join']['condition'])

        # Apply any remaining filters
        if 'FilterExpression' in ddb_params:
            joined_items = self.apply_filter(joined_items, ddb_params['FilterExpression'], ddb_params.get('ExpressionAttributeValues', {}))

        return joined_items

    def perform_join(self, main_items, join_items, join_condition):
        # Parse the join condition
        condition_parts = join_condition.split('=')
        main_key = condition_parts[0].strip().split('.')[-1]
        join_key = condition_parts[1].strip().split('.')[-1]

        # Create a dictionary for faster lookups
        join_dict = {item[join_key]: item for item in join_items}

        # Perform the join
        joined_items = []
        for main_item in main_items:
            if main_item[main_key] in join_dict:
                joined_item = {**main_item, **join_dict[main_item[main_key]]}
                joined_items.append(joined_item)

        return joined_items

    def apply_filter(self, items, filter_expression, expression_attribute_values):
        # Implement filtering logic here
        # This is a simplified version and may need to be expanded for complex filters
        filtered_items = []
        for item in items:
            if self.evaluate_filter(item, filter_expression, expression_attribute_values):
                filtered_items.append(item)
        return filtered_items








