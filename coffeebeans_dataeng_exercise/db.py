import duckdb

class DuckDBConnection:
    def __init__(self, database='warehouse.db'):
        self.database = database
        self.connection = None

    def connect(self):
        if self.connection is None:
            self.connection = duckdb.connect(self.database)
        return self.connection
    
    def execute_query(self, query, params=None):
        if self.connection is None:
            raise Exception("Connection is not established. Call connect() first.")
        if params:
            return self.connection.execute(query, params)
        else:
            return self.connection.execute(query)

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    
def test_duckdb_connection():
    conn = DuckDBConnection()
    conn.connect()
    cursor = conn.connection
    assert list(cursor.execute("SELECT 1").fetchall()) == [(1,)]
    conn.close()

if __name__ == "__main__":
    test_duckdb_connection()
