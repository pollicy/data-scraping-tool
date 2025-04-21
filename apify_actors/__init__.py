from apify_client import ApifyClient
from .twitter_scraper import ScrapePostsAndComments as ScrapeTwitterPosts
from .instagram_scraper import ScrapeUserComentsAndPosts as ScrapeInstagramPosts
from .facebook_scraper import ScrapePostsAndComments as ScrapeFacebookPosts
from typing import Dict
from pathlib import Path
import datetime
from components.auth import get_api_key
import pandas as pd

DEFAULT_PATH = Path("scraped_data")
FACEBOOK_PATH = DEFAULT_PATH / "facebook"
INSTAGRAM_PATH = DEFAULT_PATH / "instagram"
TWITTER_PATH = DEFAULT_PATH / "twitter"

FACEBOOK_PATH.mkdir(parents=True, exist_ok=True)
INSTAGRAM_PATH.mkdir(parents=True, exist_ok=True)
TWITTER_PATH.mkdir(parents=True, exist_ok=True)

for directory in [FACEBOOK_PATH, INSTAGRAM_PATH, TWITTER_PATH]:
    posts_directory = directory / "posts"
    comments_directory = directory / "comments"
    posts_directory.mkdir(parents=True, exist_ok=True)
    comments_directory.mkdir(parents=True, exist_ok=True)


def scrape_data(start:datetime.datetime, end:datetime.datetime, max_posts, max_comments, user_handles : Dict):
    """Scrape data from social media platforms based on user handles."""
    if not user_handles:
        return
    
    scraped_twitter_df = pd.DataFrame()
    scraped_instagram_df = pd.DataFrame()
    scraped_facebook_df = pd.DataFrame()
    
    client = ApifyClient(get_api_key())

    try:
        print(user_handles)

        # for platform, handles in user_handles.items():
        #     for handle in handles:
        #         if platform == "Facebook":
        #             facebook_df = ScrapeFacebookPosts(client=client, facebook_handle=handle, end_time=end, max_posts=max_posts, max_comments=max_comments, path=FACEBOOK_PATH)
        #             scraped_facebook_df = pd.concat([scraped_facebook_df, facebook_df], ignore_index=True)
                    
        #         elif platform == "Instagram":
        #             instagram_df = ScrapeInstagramPosts(
        #                 client=client, username=handle, end_time=end, max_posts=max_posts, max_comments=max_comments, path=INSTAGRAM_PATH
        #             )
        #             scraped_instagram_df = pd.concat([scraped_instagram_df, instagram_df], ignore_index=True)
        #         elif platform == "Twitter":
        #             twitter_df = ScrapeTwitterPosts(
        #                 client=client, username=handle, end_time=end, max_posts=max_posts, max_comments=max_comments, path=TWITTER_PATH
        #             )
        #             scraped_twitter_df = pd.concat([scraped_twitter_df, twitter_df], ignore_index=True)
                    
        # return {
        #     "facebook": scraped_facebook_df,
        #     "instagram": scraped_instagram_df,
        #     "twitter": scraped_twitter_df
        # }
        
        return {
            "Facebook" : pd.read_excel("scraped_data/facebook/comments/phitchayathan.thanvarat_facebook_2025-03-10.xlsx"),
            "Instagram" : pd.read_excel("scraped_data/instagram/comments/mizzflav_instagram_2025-04-15.xlsx"),
            "Twitter" : pd.read_excel("scraped_data/facebook/comments/phitchayathan.thanvarat_facebook_2025-03-10.xlsx")
        }
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"An error occurred: {e}")
        return {}