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

    def get_wsb_top_posts(self, date: datetime):
        # Connect to r/wallstreetbets
        subreddit = self.rclient.subreddit('wallstreetbets')

        # Initialize dictionaries to store ticker mentions and sentiment
        ticker_mentions = defaultdict(int)
        ticker_sentiment = defaultdict(list)

        # Get top posts for the given date
        for submission in subreddit.top(time_filter='day', limit=None):
            if submission.created_utc.date() == date:
                # Analyze submission title and body
                self.__analyze_text(submission.title, ticker_mentions, ticker_sentiment)
                self.__analyze_text(submission.selftext, ticker_mentions, ticker_sentiment)

                # Analyze comments
                submission.comments.replace_more(limit=None)
                for comment in submission.comments.list():
                    self.__analyze_text(comment.body, ticker_mentions, ticker_sentiment)

        # Calculate average sentiment for each ticker
        avg_sentiment = {ticker: sum(scores)/len(scores) if scores else 0 
                         for ticker, scores in ticker_sentiment.items()}

        return ticker_mentions, avg_sentiment

    def __analyze_text(self, text, ticker_mentions, ticker_sentiment):
        # Use the class variable self.nlp instead of reinitializing
        doc = self.nlp(text)
        
        for ent in doc.ents:
            if ent.label_ == 'ORG':  # Assuming tickers are recognized as organizations
                ticker = self.__stock_disambiguation(ent.text)
                if ticker:
                    ticker_mentions[ticker] += 1
                    
                    # Sentiment analysis using TextBlob
                    sentiment = TextBlob(text).sentiment.polarity
                    ticker_sentiment[ticker].append(sentiment)  

    def __backfill_database(self, days: int):
        pass

    def __stock_disambiguation(self, ticker: str) -> str:
        pass

if __name__ == "__main__":
    wsb_bot = RedditBot()
    wsb_bot.run()