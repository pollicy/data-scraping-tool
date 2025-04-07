from apify_client import ApifyClient
from dotenv import load_dotenv
from .twitter_scraper import ScrapePosts as ScrapeTwitterPosts
from .instagram_scraper import ScrapeUserComentsAndPosts as ScrapeInstagramPosts
import os
import datetime

# Load environment variables from .env file
load_dotenv()

client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

def main():
    # username = "elonmusk"
    # end_time = datetime.datetime.now() - datetime.timedelta(days=30)

    # # Call the ScrapePosts function to scrape posts
    # ScrapePosts(client, username, end_time=end_time)
    
    username = "irenenamatovuofficialug"
    end_time = datetime.datetime.now() - datetime.timedelta(days=30)
    
    # Call the ScrapePosts function to scrape posts
    ScrapeInstagramPosts(client, username, end_time=end_time, max_posts=10)