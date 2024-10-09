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

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        print(f"Connection to {self.db_name} was successful...")
        self._create_table()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            print(f"Connection to {self.db_name} closed.")

    def _create_table(self):
        cursor = self.conn.cursor()
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

    def query_popular(self, count: int = 6, days: int = 14) -> list:
        """
        Query for the most popular stocks/ETFs by mentions.

        Parameters:
            count (int): Limit number of stocks returned (default is 6)
            days (int): Aggregate stock mentions dating back however many days (default is 14, i.e. 2 weeks)

        Returns:
            list: List of tuples containing stock ticker and total mentions
        """
        # TODO: set limit for max days in search range
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

    def insert_data(self, data: dict, date: datetime):
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

    def delete_old_data(self):
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

    def _create_connection(self):
        """
        Manually connect to SQLite database. Create tables if they don't exist
        """
        self.__enter__()

    def _stop_connection(self):
        """
        Manually close SQLite connection.
        """
        self.__exit__(None, None, None)

    def get_oldest_date(self):
        """
        Get the oldest date in the stock_mentions table.

        Returns:
            str: The oldest date in 'YYYY-MM-DD' format, or None if the table is empty.
        """
        cursor = self.conn.cursor()
        query = "SELECT MIN(date) FROM stock_mentions"
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        # Return oldest date if it exists, otherwise return None
        return datetime.strptime(result[0], '%Y-%m-%d').date() if result else None

