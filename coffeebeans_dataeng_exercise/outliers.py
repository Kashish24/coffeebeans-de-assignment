from datetime import datetime
from .db import DuckDBConnection
import sys

class OutlierDetector:
    def __init__(self, db_connection, table_name='votes'):
        self.db_connection = db_connection
        self.table_name = table_name
    
    
    def detect_outliers_and_create_view(self):
        # Create schema if not exists
        self.db_connection.execute_query('CREATE SCHEMA IF NOT EXISTS blog_analysis')

        # Define the query to create the view
        query = f'''
        CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
        WITH weekly_vote_count AS (
            SELECT 
                EXTRACT(YEAR FROM CreationDate) AS Year,
                EXTRACT(WEEK FROM CreationDate) AS WeekNumber,
                COUNT(*) AS VoteCount
            FROM blog_analysis.{self.table_name}
            GROUP BY Year, WeekNumber
        ),
        avg_vote_count AS (
            SELECT 
                AVG(VoteCount) AS mean_votes
            FROM weekly_vote_count
        )
        SELECT 
            Year,
            WeekNumber,
            VoteCount
        FROM weekly_vote_count, avg_vote_count
        WHERE ABS(1.0 * VoteCount / mean_votes - 1) > 0.2

        '''

        # Execute the query to create the view
        self.db_connection.execute_query(query)

        

def main():
    # Check if a table name is provided as a command-line argument
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
    else:
        table_name = 'votes'  # Default table name

    # Initialize the DuckDB connection
    db_conn = DuckDBConnection()
    db_conn.connect()

    # Create the OutlierDetector instance
    detector = OutlierDetector(db_conn, table_name)

    # Detect outliers
    detector.detect_outliers_and_create_view()

    # Close the database connection
    db_conn.close()

if __name__ == "__main__":
    main()