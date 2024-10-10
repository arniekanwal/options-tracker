import praw
import os
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict

# nlp imports
import spacy
from textblob import TextBlob

# custom imports
from db_handler import Database

class RedditBot:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Initialize Reddit API client
        self.rclient = praw.Reddit(
            client_id=os.getenv('REDDIT_ID'),
            client_secret=os.getenv('REDDIT_SECRET'),
            user_agent=os.getenv('APP_NAME')
        )

        # Initialize NLP model
        self.nlp = spacy.load("en_core_web_sm")

        # Initialize database connection
        self.db = Database()

    def run(self):
        pass

    def search_subreddit(self, date: datetime, subreddit: str, search_query: str):
        pass  

    def get_wsb_top_posts(self, date: datetime = self._days_ago(1)):
        '''
        Get the most popular posts from r/wallstreetbets on a given day. 

        Parameters:
            date (datetime): Must be a past date (yesterday by default).

        Returns:
            List[praw.models.Submission]: A list of praw.models.Submission objects.
        '''
        # Connect to r/wallstreetbets and retrieve the top posts for the given date
        subreddit = self.rclient.subreddit('wallstreetbets')
        top_posts = subreddit.top(time_filter='day', limit=None, date=date)
        return top_posts


    def __backfill_database(self, days: int):
        pass

    def __stock_disambiguation(self, ticker: str) -> str:
        pass

    def _days_ago(self, days: int):
        '''
        Returns the date of the day `days` ago
        '''
        if days < 0:
            raise ValueError("Days must be a non-negative integer")
        return datetime.now().date() - timedelta(days=days)

if __name__ == "__main__":
    wsb_bot = RedditBot()
    wsb_bot.run()