from apify_client import ApifyClient
from dotenv import load_dotenv
from .twitter_scraper import ScrapePosts as ScrapeTwitterPosts
from .instagram_scraper import ScrapeUserComentsAndPosts as ScrapeInstagramPosts
from .facebook_scraper import ScrapePostsAndComments as ScrapeFacebookPosts
import os
from pathlib import Path
import datetime

# Load environment variables from .env file
load_dotenv()

client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

DEFAULT_PATH = Path("scraped_data")
FACEBOOK_PATH = DEFAULT_PATH / "facebook"
INSTAGRAM_PATH = DEFAULT_PATH / "instagram"
TWITTER_PATH = DEFAULT_PATH / "twitter"

FACEBOOK_PATH.mkdir(parents=True, exist_ok=True)
INSTAGRAM_PATH.mkdir(parents=True, exist_ok=True)
TWITTER_PATH.mkdir(parents=True, exist_ok=True)

def main():
    # username = "elonmusk"
    # end_time = datetime.datetime.now() - datetime.timedelta(days=30)

    # # Call the ScrapePosts function to scrape posts
    # ScrapePosts(client, username, end_time=end_time)
    
    # username = "irenenamatovuofficialug"
    # end_time = datetime.datetime.now() - datetime.timedelta(days=30)
    
    # # Call the ScrapePosts function to scrape posts
    # ScrapeInstagramPosts(client, username, end_time=end_time, max_posts=10)
    
    username = "phitchayathan.thanvarat"
    end_time = datetime.datetime.now() - datetime.timedelta(days=30)
    max_posts = 10
    max_comments = 10
    
    ScrapeFacebookPosts(client, username, end_time=end_time, path=FACEBOOK_PATH, max_posts=max_posts, max_comments=max_comments)