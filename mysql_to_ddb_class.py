import boto3
import re
import logging
from typing import Dict, Any, Tuple, List

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DynamoDBSQLWrapper:
    def __init__(self):
        self.ddb = boto3.client('dynamodb')
    
    def execute_query(self, sql: str) -> Any:
        # Parse the SQL query
        parsed_query = self.parse_sql_query(sql)
        
        # Convert to DynamoDB parameters
        ddb_params = self.sql_to_ddb_params(parsed_query)
        logger.info("DynamoDB parameters prepared")
        
        # Route to appropriate execution method
        if len(parsed_query['from']) > 1:
            return self.execute_join_select(ddb_params)
        else:
            return self.execute_simple_select(ddb_params)

    def execute_simple_select(self, ddb_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handle simple single-table SELECT queries"""
        scan_params = {
            'TableName': ddb_params['TableName']
        }
        
        # Add filter expression if present
        if 'FilterExpression' in ddb_params and ddb_params['FilterExpression']:
            scan_params['FilterExpression'] = ddb_params['FilterExpression']
            scan_params['ExpressionAttributeValues'] = ddb_params['ExpressionAttributeValues']
        
        # Add projection expression if present
        if 'ProjectionExpression' in ddb_params:
            scan_params['ProjectionExpression'] = ddb_params['ProjectionExpression']
        
        logger.info(f"Executing simple select with params: {scan_params}")
        response = self.ddb.scan(**scan_params)
        
        # Process and filter empty results
        items = response.get('Items', [])
        processed_items = self.process_select_response(items, ddb_params.get('ProjectionExpression', ''))
        
        # Remove empty results
        return [item for item in processed_items if item]

    def execute_join_select(self, ddb_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handle JOIN queries generically"""
        logger.info("Starting generic join execution")
        
        # Parse WHERE conditions into table filters and join conditions
        table_filters, join_conditions = self.parse_join_conditions(
            ddb_params.get('where', ''),
            ddb_params.get('from', [])
        )
        
        logger.info(f"Table filters: {table_filters}")
        logger.info(f"Join conditions: {join_conditions}")
        
        # Get results for each table
        table_results = {}
        for table in ddb_params.get('from', []):
            scan_params = self.build_table_scan_params(
                table,
                table_filters.get(table, []),
                ddb_params.get('select', []),
                join_conditions
            )
            logger.info(f"Scanning {table} with params: {scan_params}")
            table_results[table] = self.ddb.scan(**scan_params)['Items']
            logger.info(f"Found {len(table_results[table])} items for {table}")
        
        # Perform the join
        return self.perform_join(table_results, join_conditions, ddb_params.get('select', []))

    def parse_join_conditions(self, where_clause: str, tables: List[str]) -> Tuple[Dict[str, List], List[Dict]]:
        """Parse WHERE conditions into table filters and join conditions"""
        table_filters = {table: [] for table in tables}
        join_conditions = []
        
        # Split conditions
        conditions = [cond.strip() for cond in where_clause.split('AND')]
        logger.info(f"Processing WHERE conditions: {conditions}")
        
        for condition in conditions:
            if '=' not in condition:
                continue
            
            left, right = [x.strip() for x in condition.split('=')]
            
            # Check if right side is a quoted value
            is_quoted_value = right.startswith("'") or right.startswith('"')
            
            if is_quoted_value:
                # This is a filter condition (column = 'value')
                for table in tables:
                    if table in left:
                        column = left.split('.')[-1].strip()
                        value = right.strip("' ")
                        table_filters[table].append({
                            'column': column,
                            'value': value
                        })
                        break
            else:
                # This is a join condition
                left_parts = left.split('.')
                right_parts = right.split('.')
                
                if len(right_parts) > 1:  # Explicit table reference
                    join_conditions.append({
                        'left_table': left_parts[0],
                        'left_column': left_parts[1],
                        'right_table': right_parts[0],
                        'right_column': right_parts[1]
                    })
                else:  # Implicit reference
                    join_conditions.append({
                        'left_table': left_parts[0],
                        'left_column': left_parts[1],
                        'right_table': [t for t in tables if t != left_parts[0]][0],
                        'right_column': right
                    })
        
        logger.info(f"Parsed table filters: {table_filters}")
        logger.info(f"Parsed join conditions: {join_conditions}")
        return table_filters, join_conditions

    def build_table_scan_params(self, table: str, filters: List[Dict], projections: List[str], join_conditions: List[Dict]) -> Dict[str, Any]:
        """Build DynamoDB scan parameters for a table"""
        scan_params = {'TableName': table}
        expr_names = {}
        
        # Handle filters
        if filters:
            filter_parts = []
            expr_values = {}
            
            for i, f in enumerate(filters):
                placeholder = f":val{i}"
                name_placeholder = f"#n{i}"
                
                filter_parts.append(f"{name_placeholder} = {placeholder}")
                expr_values[placeholder] = {'S': f['value']}
                expr_names[name_placeholder] = f['column']
                
            scan_params['FilterExpression'] = ' AND '.join(filter_parts)
            scan_params['ExpressionAttributeValues'] = expr_values
        
        # Handle projections
        proj_parts = []
        for proj in projections:
            if table in proj:  # Only include projections for this table
                col = proj.split('.')[-1]
                name_placeholder = f"#p_{col}"
                expr_names[name_placeholder] = col
                proj_parts.append(name_placeholder)
        
        # Add join columns to projections if not already included
        for condition in join_conditions:
            if condition['left_table'] == table:
                col = condition['left_column']
                if f"#p_{col}" not in expr_names:
                    expr_names[f"#p_{col}"] = col
                    proj_parts.append(f"#p_{col}")
            if condition['right_table'] == table:
                col = condition['right_column']
                if f"#p_{col}" not in expr_names:
                    expr_names[f"#p_{col}"] = col
                    proj_parts.append(f"#p_{col}")
        
        if expr_names:
            scan_params['ExpressionAttributeNames'] = expr_names
        if proj_parts:
            scan_params['ProjectionExpression'] = ', '.join(proj_parts)
        
        return scan_params

    def perform_join(self, table_results: Dict[str, List], join_conditions: List[Dict], projections: List[str]) -> List[Dict]:
        """Perform the actual join operation"""
        if not table_results or len(table_results) < 2:
            return []
        
        tables = list(table_results.keys())
        results = table_results[tables[0]]
        
        # Join with each subsequent table
        for right_table in tables[1:]:
            new_results = []
            for left in results:
                for right in table_results[right_table]:
                    if self.matches_join_conditions(left, right, join_conditions):
                        # Create merged result with proper structure
                        merged = {}
                        for proj in projections:
                            table, col = proj.split('.')
                            source = left if table == tables[0] else right
                            value = source.get(col, {})
                            
                            # Convert DynamoDB types
                            if 'S' in value:
                                merged[col] = value['S']
                            elif 'N' in value:
                                merged[col] = int(value['N'])
                            elif 'BOOL' in value:
                                merged[col] = value['BOOL']
                        
                        new_results.append(merged)
            results = new_results
        
        logger.info(f"Join complete. Found {len(results)} matching records")
        return results

    def matches_join_conditions(self, left: Dict, right: Dict, join_conditions: List[Dict]) -> bool:
        """Check if two items match the join conditions"""
        if not join_conditions:
            return True
        
        for condition in join_conditions:
            # Get values from both sides
            left_val = left.get(condition['right_column'], {}).get('S')
            right_val = right.get(condition['left_column'], {}).get('S')
            
            # If either value is missing or they don't match, return False
            if not left_val or not right_val or left_val != right_val:
                return False
        
        return True

    def process_select_response(self, items: List[Dict[str, Any]], projection: str = '') -> List[Dict[str, Any]]:
        """Process DynamoDB response into clean format"""
        result = []
        for item in items:
            clean_item = {}
            for key, value in item.items():
                # Convert DynamoDB types to Python types
                if 'S' in value:
                    clean_item[key] = value['S']
                elif 'N' in value:
                    clean_item[key] = int(value['N'])
                elif 'BOOL' in value:
                    clean_item[key] = value['BOOL']
            # Only add non-empty items
            if clean_item:
                result.append(clean_item)
        return result

    def execute_insert(self, ddb_params):
        response = self.ddb.put_item(**ddb_params)
        return response

    def execute_update(self, ddb_params):
        response = self.ddb.update_item(**ddb_params)
        return response

    def execute_delete(self, ddb_params):
        response = self.ddb.delete_item(**ddb_params)
        return response

    def parse_sql_query(self, sql: str) -> Dict[str, Any]:
        """Parse full SQL query including all WHERE conditions"""
        parsed = {
            'select': [],
            'from': [],
            'where': ''  # Keep as string for simple queries
        }
        
        # Extract SELECT, FROM, and WHERE parts
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
        from_match = re.search(r"FROM\s+(.*?)(?:\s+WHERE|$)", sql, re.IGNORECASE)
        where_match = re.search(r"WHERE\s+(.*?)$", sql, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            parsed['select'] = [col.strip() for col in select_match.group(1).split(',')]
        if from_match:
            parsed['from'] = [table.strip() for table in from_match.group(1).split(',')]
        if where_match:
            parsed['where'] = where_match.group(1).strip()
            # Add parsed where conditions for joins
            if len(parsed['from']) > 1:
                parsed['where_conditions'] = [cond.strip() for cond in where_match.group(1).split('AND')]
        
        logger.info(f"Parsed query: {parsed}")
        return parsed

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

    def sql_to_ddb_params(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Convert parsed SQL to DynamoDB parameters"""
        ddb_params = {
            'TableName': parsed_query['from'][0],  # First table name
            'select': parsed_query.get('select', []),
            'from': parsed_query.get('from', []),
            'where': parsed_query.get('where', '')
        }
        
        # For simple queries, handle WHERE clause
        if len(parsed_query['from']) == 1 and parsed_query.get('where'):
            filter_expression, attr_values, _ = self.parse_where_clause(
                parsed_query['where'], 
                parsed_query['from']
            )
            if filter_expression:
                ddb_params['FilterExpression'] = filter_expression
                ddb_params['ExpressionAttributeValues'] = attr_values
        
        # Handle projections
        if parsed_query.get('select'):
            projection_parts = []
            expr_names = {}
            
            # Reserved words that need special handling
            reserved_words = {'method', 'name', 'key', 'timestamp', 'count', 'type', 'fail', 'win'}
            
            for col in parsed_query['select']:
                clean_col = col.split('.')[-1]  # Remove table prefix if present
                if clean_col.lower() in reserved_words:
                    name_placeholder = f"#n_{clean_col}"
                    expr_names[name_placeholder] = clean_col
                    projection_parts.append(name_placeholder)
                else:
                    projection_parts.append(clean_col)
            
            if projection_parts:
                ddb_params['ProjectionExpression'] = ', '.join(projection_parts)
                if expr_names:
                    ddb_params['ExpressionAttributeNames'] = expr_names
        
        logger.info(f"Converted DynamoDB parameters: {ddb_params}")
        return ddb_params

    def parse_where_clause(self, where_clause: str, tables: List[str]) -> Tuple[str, Dict[str, Any], List[str]]:
        """Parse WHERE clause into DynamoDB filter expressions"""
        filter_parts = []
        join_conditions = []
        attr_values = {}
        value_counter = 0
        
        # Split on AND if multiple conditions
        conditions = [where_clause] if 'AND' not in where_clause else where_clause.split('AND')
        
        for condition in conditions:
            condition = condition.strip()
            if '=' not in condition:
                continue
            
            left, right = [x.strip() for x in condition.split('=')]
            
            # Remove table prefix if present
            if '.' in left:
                _, left = left.split('.')
            
            # Clean the value (remove quotes)
            right = right.strip("'\"")
            
            # Add to filter parts
            placeholder = f":val{value_counter}"
            filter_parts.append(f"{left} = {placeholder}")
            
            # Check if value is numeric
            try:
                float(right)  # Test if value can be converted to number
                attr_values[placeholder] = {'N': str(right)}  # Store as number type
            except ValueError:
                attr_values[placeholder] = {'S': right}  # Store as string type
            
            value_counter += 1
        
        logger.info(f"Filter parts: {filter_parts}")
        logger.info(f"Join conditions: {join_conditions}")
        logger.info(f"Attribute values: {attr_values}")
        
        return (
            ' AND '.join(filter_parts) if filter_parts else '',
            attr_values,
            join_conditions
        )

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

    def split_where_conditions(self, ddb_params):
        """Split WHERE conditions by table"""
        table_conditions = {}
        where_clause = ddb_params.get('where', '')
        if not where_clause:
            return table_conditions
            
        conditions = [c.strip() for c in where_clause.split('AND')]
        for condition in conditions:
            for table in ddb_params['from']:
                if table in condition:
                    if table not in table_conditions:
                        table_conditions[table] = []
                    # Remove table prefix for DynamoDB
                    clean_condition = condition.replace(f"{table}.", '')
                    table_conditions[table].append(clean_condition)
                    break
        
        return table_conditions

    def extract_join_conditions(self, ddb_params):
        """Extract join conditions between tables"""
        join_conditions = []
        where_clause = ddb_params.get('where', '')
        if not where_clause:
            return join_conditions
            
        conditions = [c.strip() for c in where_clause.split('AND')]
        tables = ddb_params['from']
        
        for condition in conditions:
            # If condition references multiple tables, it's a join condition
            if all(any(table in condition for table in tables)):
                join_conditions.append(condition)
                
        return join_conditions

    def join_results(self, left_results, right_results, join_conditions):
        """Join two result sets based on conditions"""
        joined_results = []
        
        for left in left_results:
            for right in right_results:
                if self.matches_join_conditions(left, right, join_conditions):
                    # Merge the items
                    joined_item = {}
                    joined_item.update(self.flatten_item(left))
                    joined_item.update(self.flatten_item(right))
                    joined_results.append(joined_item)
                    
        return joined_results

    def get_table_type(self, item: Dict) -> str:
        """Determine which table an item is from based on its structure"""
        if 'card_shortname' in item:
            return 'cards'
        elif 'foe' in item:
            return 'questmaster'
        else:
            return 'unknown'

    def flatten_item(self, item):
        """Convert DynamoDB format to plain values"""
        flattened = {}
        for key, value in item.items():
            if 'S' in value:
                flattened[key] = value['S']
            elif 'N' in value:
                flattened[key] = int(value['N'])
            # Add other types as needed
        return flattened

