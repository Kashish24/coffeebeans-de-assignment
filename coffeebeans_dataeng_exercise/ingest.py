import json
from collections import defaultdict, OrderedDict
from datetime import datetime
import logging
import time
from .db import DuckDBConnection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VoteDataIngestor:
    def __init__(self, db_connection, file_path='uncommitted/votes.jsonl'):
        # Initialize the VoteDataIngestor with a database connection and an optional file path
        self.db_connection = db_connection
        self.file_path = file_path

    def create_schema_and_table(self, columns):
        start_time = time.time()
        try:
            # Create schema 'blog_analysis' if it doesn't exist
            self.db_connection.execute_query('CREATE SCHEMA IF NOT EXISTS blog_analysis')
            print("columns.items: ", columns.items())
            # Construct the CREATE TABLE query with provided columns and their types
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            '''
            create_table_query += ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
            create_table_query += ')' 

            print("create_table_query: ", create_table_query)

            # Execute the CREATE TABLE query
            self.db_connection.execute_query(create_table_query)
            logger.info("Schema and table created successfully.")
        except Exception as e:
            logger.error(f"Error creating schema and table: {e}")

        elapsed_time = time.time() - start_time
        logger.info(f"create_schema_and_table() took {elapsed_time:.4f} seconds.")

    def ingest_votes(self, columns):
        start_time = time.time()
        # Open the JSONL file and read each line
        skipped_row_count = 0
        with open(self.file_path, 'r') as f:
            for line in f:
                vote = json.loads(line.strip())  # Parse the JSON data
                try:
                    # Ensure each row has all necessary columns (Handling missing columns in Data as compared to schema of table)
                    for col_name, col_type in columns.items():
                        if col_name not in vote:
                            # Set default value based on the column type
                            if col_type == 'INTEGER':
                                vote[col_name] = 0
                            elif col_type == 'TIMESTAMP':
                                vote[col_name] = None #(Setting None as default value for CreationDate column)
                    
                    # Log if extra columns are present
                    extra_columns = set(vote.keys()) - set(columns.keys())
                    #if extra_columns:
                        #print(f"Extra columns found and ignored: {extra_columns}")

                    # Ensure data types are correct and prepare insert values
                    insert_values = []
                    for col_name, col_type in columns.items():
                        if col_type == 'INTEGER':
                            vote[col_name] = int(vote[col_name])
                        elif col_type == 'TIMESTAMP':
                            vote[col_name] = datetime.strptime(vote[col_name], '%Y-%m-%dT%H:%M:%S.%f')
                        insert_values.append(vote[col_name])
                    
                    # Insert or replace based on the primary key to avoid duplicates
                    self.db_connection.execute_query(f'''
                        INSERT OR REPLACE INTO blog_analysis.votes ({', '.join(columns.keys())}) VALUES ({', '.join(['?' for _ in columns])})
                    ''', insert_values)
                except (ValueError, KeyError) as e:
                    # Log and skip invalid records
                    skipped_row_count+=1
                    print(f"Skipping invalid record: {vote} due to error: {e}")
        print("skipped_row_count: ", skipped_row_count)
        elapsed_time = time.time() - start_time
        logger.info(f"ingest_votes() took {elapsed_time:.4f} seconds. Skipped {skipped_row_count} rows.")



def perform_eda(file_path):
    start_time = time.time()
    # Perform Exploratory Data Analysis (EDA) on the data file to determine column types and potential primary key
    column_types = defaultdict(list)
    column_counts = defaultdict(int)
    total_rows = 0

    def is_int(value):
        try:
            int(value)
            return True
        except ValueError:
            return False

    with open(file_path, 'r') as f:
        sample_data = [json.loads(line.strip()) for line in f]
        total_rows = len(sample_data)

    ordered_columns = list(sample_data[0].keys())  # Maintain the order of columns as in the first row

    for vote in sample_data:
        for key, value in vote.items():
            column_counts[key] += 1
            if is_int(value):
                column_types[key].append("INTEGER")
            else:
                # Check if the value matches the datetime format 'YYYY-MM-DDTHH:MM:SS'
                try:
                    datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f')
                    column_types[key].append("TIMESTAMP")
                except ValueError:
                    column_types[key].append("STRING")

    # Filter columns to include only those appearing in more than 100% of rows
    threshold = 1 * total_rows
    filtered_columns = {col for col, count in column_counts.items() if count >= threshold}

    # Determine the most common type for each column
    inferred_schema = OrderedDict()
    for column in ordered_columns:
        if column in filtered_columns:
            types = column_types[column]
            if types.count("INTEGER") > len(types) / 2:
                inferred_schema[column] = "INTEGER"
            elif "TIMESTAMP" in types:
                inferred_schema[column] = "TIMESTAMP"
            else:
                inferred_schema[column] = "STRING"

    print("column_counts.items: ", column_counts.items())
    # Identify the primary key
    primary_key_candidates = [col for col, count in column_counts.items() if count == total_rows]

    if not primary_key_candidates:
        raise ValueError("No unique column found to be used as a primary key.")

    # Assuming the first unique column found is the primary key for simplicity
    primary_key = primary_key_candidates[0]

    elapsed_time = time.time() - start_time
    logger.info(f"perform_eda() took {elapsed_time:.4f} seconds.")
    
    return inferred_schema, primary_key


def main(file_path='uncommitted/votes.jsonl'):
    # Perform EDA to determine column types and primary key
    columns, primary_key = perform_eda(file_path)
    
    print("columns: ", columns)
    print("Primary Keys: ", primary_key)

    # Add PRIMARY KEY constraint to the primary key column
    columns[primary_key] += ' PRIMARY KEY'
    
    # Create a DuckDB connection
    db_conn = DuckDBConnection()
    db_conn.connect()

    # Create an ingestor instance
    ingestor = VoteDataIngestor(db_conn, file_path)

    # Create the schema and table
    ingestor.create_schema_and_table(columns)

    # Ingest the vote data file
    ingestor.ingest_votes(columns)

    # Close the database connection
    db_conn.close()

if __name__ == '__main__':
    # Allow specifying a different data file via command-line argument
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()