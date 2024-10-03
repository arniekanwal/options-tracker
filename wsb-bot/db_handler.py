import sqlite3
from datetime import datetime, timedelta

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

    def queryPopular(self, count: int = 6, days: int = 14) -> list:
        """
        Query the most popular (by mentions) stock tickers.

        Args:
            count (int): Limit number of stocks returned (default is 6)
            days (int): Aggregate stock mentions dating back however many days (default is 14, i.e. 2 weeks)
        """
        days = min(days, 60) # Cannot query data older than 60 days
        cursor = self.conn.cursor()

        query = f"""
            SELECT stock_ticker, SUM(mentions) AS total_mentions
            FROM stock_mentions
            WHERE date >= date('now', '-{days} days')  
            GROUP BY stock_ticker
            ORDER BY total_mentions DESC
            LIMIT {count};
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def insertData(self, data: dict, date: datetime):
        """
        Insert data scraped via reddit bot into the database.

        Args:
            data (dict): reddit data { stock_ticker (str) : daily_mentions (int) }
            date: Datetime object to track the day this data was collected
        """
        
        cursor = self.conn.cursor()
        query = """
            INSERT INTO stock_mentions (date, stock_ticker, mentions)
            VALUES (?, ?, ?)
        """
        # Iterate over the data dictionary and insert each stock ticker and its mentions
        for ticker, mentions in data.items():
            cursor.execute(query, (date.strftime('%Y-%m-%d'), ticker, mentions))
        
        # Commit the transaction 
        self.conn.commit()
        cursor.close()

    def deleteOldData(self):
        """
        Delete data strictly older than 60 days.
        """
        # Calculate the cutoff date (60 days ago from today) and create query
        cursor = self.conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        query = f"DELETE FROM stock_mentions WHERE date < {cutoff_date}"

        # Execute the query and commit transaction
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def createConnection(self):
        """
        Connect to SQLite database. Create tables if they don't exist
        """
        self.conn = sqlite3.connect(self.db_name)
        print(f"Connection to {self.db_name} was successful...")
        cursor = self.conn.cursor()

        # Create stock_mentions table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_mentions (
                date DATE,
                stock_ticker TEXT,
                mentions INTEGER,
                PRIMARY KEY (date, stock_ticker)
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