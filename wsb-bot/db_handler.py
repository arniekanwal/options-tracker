import sqlite3

class Database:
    """
    Database class to handle SQLite operations. Contains methods to update/query/insert stock data 
    collected via the Reddit Bot.

    Attributes:
        db_name (str): sqlite database filename
        conn (sqlite3.Connection): sqlite connection object
    """

    def __init__(self):
        """
        Initialize Database class

        Args:
            db_name (str): sqlite database filename
            conn (sqlite3.Connection): sqlite connection object
        """
        self.db_name = "stocks.db"
        self.conn = None

    def backfillDB(self):
        pass

    def queryPopular(self):
        pass

    def insertData(self):
        pass

    def deleteOldData(self):
        pass

    def createConnection(self):
        """
        Connect to SQLite database. Create tables if they don't exist
        """
        self.conn = sqlite3.connect(self.db_name)
        print(f"Connection to {self.db_name} was successful...")
        cursor = self.conn.cursor()

        # Create a table only if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_mentions (
                id INTEGER PRIMARY KEY,
                stock_ticker TEXT,
                mentions INTEGER,
                bullish_count INTEGER,
                bearish_count INTEGER,
                last_seen DATE
            );
        """)
        self.conn.commit()
        cursor.close()
        
        
    def stopConnection(self):
        """
        Close SQLite connection.
        """
        if self.conn:
            self.conn.close()