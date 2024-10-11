import praw
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict
import json

# nlp imports
# import spacy
# from textblob import TextBlob

# custom imports
from db_handler import Database

class RedditBot:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Initialize Reddit API client
        self.rclient = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT'),
            # username=os.getenv('REDDIT_USERNAME'),
            # password=os.getenv('REDDIT_PASSWORD')
        )

        # Initialize NLP model
        # self.nlp = spacy.load("en_core_web_sm")

        # Initialize database connection
        self.db = Database()

    def run(self):
        pass

    def search_subreddit(self, date: datetime, subreddit: str, search_query: str):
        pass  

    def get_wsb_top_posts(self, date: datetime = None, limit: int = 20):
        if date is None:
            date = self._days_ago(1)
        
        subreddit = self.rclient.subreddit('wallstreetbets')
        top_posts = subreddit.top(time_filter='day', limit=limit, date=date)
        
        posts_data = []
        
        for post in top_posts:
            post_data = {
                "post_id": post.id,
                "title": post.title,
                "author": str(post.author),
                "score": post.score,
                "url": post.url,
                "body": post.selftext,
                "created_utc": post.created_utc,
                "num_comments": post.num_comments,
                "comments": []
            }
            
            post.comments.replace_more(limit=0)  # Remove MoreComments objects
            for comment in post.comments.list():
                comment_data = {
                    "comment_id": comment.id,
                    "author": str(comment.author),
                    "score": comment.score,
                    "body": comment.body,
                    "created_utc": comment.created_utc
                }
                post_data["comments"].append(comment_data)
            
            posts_data.append(post_data)
        
        with open('top_posts.json', 'w', encoding='utf-8') as f:
            json.dump(posts_data, f, ensure_ascii=False, indent=2)
        
        return posts_data


    def __backfill_database(self, days: int):
        pass

    def __stock_disambiguation(self, ticker: str) -> str:
        pass

    @staticmethod
    def _days_ago(days: int):
        '''
        Returns the date `days` ago from today.
        '''
        if days < 0:
            raise ValueError("Days must be a non-negative integer")
        return datetime.now().date() - timedelta(days=days)

if __name__ == "__main__":
    wsb_bot = RedditBot()
    wsb_bot.get_wsb_top_posts()