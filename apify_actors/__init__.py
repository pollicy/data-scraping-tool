from apify_client import ApifyClient
from .twitter_scraper import ScrapePostsAndComments as ScrapeTwitterPosts
from .instagram_scraper import ScrapeUserComentsAndPosts as ScrapeInstagramPosts
from .facebook_scraper import ScrapePostsAndComments as ScrapeFacebookPosts
import os
from pathlib import Path
import datetime
from components.auth import get_api_key

client = ApifyClient(get_api_key())

DEFAULT_PATH = Path("scraped_data")
FACEBOOK_PATH = DEFAULT_PATH / "facebook"
INSTAGRAM_PATH = DEFAULT_PATH / "instagram"
TWITTER_PATH = DEFAULT_PATH / "twitter"

FACEBOOK_PATH.mkdir(parents=True, exist_ok=True)
INSTAGRAM_PATH.mkdir(parents=True, exist_ok=True)
TWITTER_PATH.mkdir(parents=True, exist_ok=True)

def scrape_facebook_posts(username, end_time:datetime.datetime, max_posts=100, max_comments=100):
    ScrapeFacebookPosts(client, username, end_time=end_time, path=FACEBOOK_PATH, max_posts=max_posts, max_comments=max_comments)
    
def scrape_instagram_posts(username, end_time:datetime.datetime, max_posts=100, max_comments=100):
    ScrapeInstagramPosts(client, username, end_time=end_time, path=INSTAGRAM_PATH, max_posts=max_posts, max_comments=max_comments)
    
def scrape_twitter_posts(username, end_time:datetime.datetime, max_posts=100, max_comments=100):
    ScrapeTwitterPosts(client, username, end_time=end_time, path=TWITTER_PATH, max_posts=max_posts, max_comments=max_comments)
