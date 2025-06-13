import datetime
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Callable, Tuple, TypedDict, Optional

from apify_client import ApifyClient
# Assuming these imports remain correct
from components.auth import get_api_key
from .twitter_scraper import ScrapePostsAndComments as ScrapeTwitterPostsAndComments, ScrapePosts as ScrapeTwitterPosts
from .instagram_scraper import ScrapeUserComentsAndPosts as ScrapeInstagramPostsAndComments, ScrapePosts as ScrapeInstagramPosts
from .facebook_scraper import ScrapePostsAndComments as ScrapeFacebookPostsAndComments, ScrapePosts as ScrapeFacebookPosts
from .linkedin_scraper import ScrapePostsAndComments as ScrapeLinkedinPostsAndComments, ScrapePosts as ScrapeLinkedinPosts

# --- Type Hinting for Configuration (Unchanged) ---
class PlatformConfig(TypedDict):
    posts_scraper: Callable
    posts_and_comments_scraper: Callable
    path: Path
    handle_arg_name: str
    threads_arg_name: str
    post_id_col: str
    comment_id_col: str

# --- Central Configuration Registry (Unchanged) ---
DEFAULT_PATH = Path("scraped_data")
PLATFORM_REGISTRY: Dict[str, PlatformConfig] = {
    "Facebook": {
        "posts_scraper": ScrapeFacebookPosts,
        "posts_and_comments_scraper": ScrapeFacebookPostsAndComments,
        "path": DEFAULT_PATH / "facebook",
        "handle_arg_name": "facebook_handle",
        "threads_arg_name": "max_threads",
        "post_id_col": "url",
        "comment_id_col": "id",
    },
    "Instagram": {
        "posts_scraper": ScrapeInstagramPosts,
        "posts_and_comments_scraper": ScrapeInstagramPostsAndComments,
        "path": DEFAULT_PATH / "instagram",
        "handle_arg_name": "username",
        "threads_arg_name": "max_threads",
        "post_id_col": "shortcode",
        "comment_id_col": "id",
    },
    "Twitter": {
        "posts_scraper": ScrapeTwitterPosts,
        "posts_and_comments_scraper": ScrapeTwitterPostsAndComments,
        "path": DEFAULT_PATH / "twitter",
        "handle_arg_name": "username",
        "threads_arg_name": "max_threads",
        "post_id_col": "tweetId",
        "comment_id_col": "id",
    },
    "LinkedIn": {
        "posts_scraper": ScrapeLinkedinPosts,
        "posts_and_comments_scraper": ScrapeLinkedinPostsAndComments,
        "path": DEFAULT_PATH / "linkedin",
        "handle_arg_name": "username",
        "threads_arg_name": "max_threads",
        "post_id_col": "url",
        "comment_id_col": "comment_id",
    },
}

class PlatformScraper:
    """
    A reusable scraper for social media platforms, configured at initialization
    and capable of scraping one or all platforms on demand.
    """

    def __init__(self, api_key: str, **default_thread_counts: int):
        """
        Initializes the scraper with the Apify client and default thread counts.

        Args:
            api_key: Your Apify API key.
            **default_thread_counts: Set default threads, e.g.,
                                     facebook_max_threads=10, twitter_max_threads=15
        """
        self.client = ApifyClient(api_key)
        print("api key set:", api_key)
        
        # Store default thread counts in a structured way
        self.thread_counts = {
            "Facebook": default_thread_counts.get("facebook_max_threads", 10),
            "Instagram": default_thread_counts.get("instagram_max_threads", 10),
            "Twitter": default_thread_counts.get("twitter_max_threads", 15),
            "LinkedIn": default_thread_counts.get("linkedin_max_threads", 15),
        }
        print("--- Scraper Initialized ---")
        print(f"Default Thread Counts: {self.thread_counts}")

    def _setup_directories(self, platform_name: str):
        """Creates necessary directories for a single, specified platform."""
        config = PLATFORM_REGISTRY.get(platform_name)
        if config:
            base_path = config["path"]
            base_path.mkdir(parents=True, exist_ok=True)
            (base_path / "posts").mkdir(exist_ok=True)
            (base_path / "comments").mkdir(exist_ok=True)

    def _deduplicate_df(self, df: pd.DataFrame, id_col: str, item_type: str, platform: str) -> pd.DataFrame:
        """Helper to remove duplicates from a DataFrame and print a summary."""
        if not id_col or id_col not in df.columns or df.empty:
            return df
        
        initial_count = len(df)
        df.drop_duplicates(subset=[id_col], inplace=True, ignore_index=True)
        removed_count = initial_count - len(df)
        if removed_count > 0:
            print(f"Removed {removed_count} duplicate {item_type} for {platform} based on '{id_col}'.")
        return df

    def scrape(
        self,
        platform: str,
        handles: List[str],
        start: datetime.datetime,
        end: datetime.datetime,
        max_posts: int,
        max_comments: int,
        scrape_comments: bool,
        max_threads: Optional[int] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Scrapes data for a single specified platform.

        Args:
            platform: The name of the platform to scrape (e.g., "Twitter").
            handles: A list of user handles for that platform.
            start: The start date for scraping.
            end: The end date for scraping.
            max_posts: Max posts to scrape per handle.
            max_comments: Max comments to scrape per post.
            scrape_comments: Whether to scrape comments.
            max_threads: Optionally override the default thread count for this run.

        Returns:
            A dictionary containing 'posts' and 'comments' DataFrames for the platform.
        """
        config = PLATFORM_REGISTRY.get(platform)
        if not config:
            print(f"Error: Platform '{platform}' is not supported. Skipping.")
            return {"posts": pd.DataFrame(), "comments": pd.DataFrame()}

        if not handles:
            print(f"No handles provided for {platform}. Skipping.")
            return {"posts": pd.DataFrame(), "comments": pd.DataFrame()}

        self._setup_directories(platform)
        
        print(f"\n---== Processing Platform: {platform.upper()} ==---")
        print(f"Handles: {handles}")

        cumulative_posts_df = pd.DataFrame()
        cumulative_comments_df = pd.DataFrame()

        for handle in handles:
            print(f"\n--- Processing Handle: {handle} ---")
            
            scraper_func = config['posts_and_comments_scraper'] if scrape_comments else config['posts_scraper']
            
            scraper_args = {
                "client": self.client,
                config['handle_arg_name']: handle,
                "start_time": start, "end_time": end, "max_posts": max_posts, "path": config['path'],
            }

            if scrape_comments:
                # Use override `max_threads` if provided, otherwise use class default
                thread_count = max_threads if max_threads is not None else self.thread_counts.get(platform, 10)
                scraper_args["max_comments"] = max_comments
                scraper_args[config['threads_arg_name']] = thread_count
                print(f"Using {thread_count} concurrent tasks for comments.")

            # --- Execute Scraper ---
            try:
                scraped_data = scraper_func(**scraper_args)
                
                posts_df, comments_df = (None, None)
                if scrape_comments:
                    if isinstance(scraped_data, tuple) and len(scraped_data) == 2:
                        posts_df, comments_df = scraped_data
                    else:
                        print(f"Error: Scraper for {platform} (with comments) did not return a valid result tuple.")
                        continue
                else:
                    posts_df = scraped_data

                if posts_df is None:
                    print(f"Critical error during scrape for {handle}. Skipping this handle.")
                    continue

                if not posts_df.empty:
                    print(f"Received {len(posts_df)} new posts from {handle}.")
                    cumulative_posts_df = pd.concat([cumulative_posts_df, posts_df], ignore_index=True)

                if comments_df is not None and not comments_df.empty:
                    print(f"Received {len(comments_df)} comments from {handle}.")
                    cumulative_comments_df = pd.concat([cumulative_comments_df, comments_df], ignore_index=True)

            except Exception as e:
                import traceback
                print(f"An error occurred while scraping handle '{handle}' on '{platform}': {e}")
                traceback.print_exc()
                continue
        
        # --- Final Deduplication and Summary for the Platform ---
        final_posts = self._deduplicate_df(cumulative_posts_df, config['post_id_col'], 'posts', platform)
        final_comments = self._deduplicate_df(cumulative_comments_df, config['comment_id_col'], 'comments', platform)
        
        print(f"\n--- {platform.upper()} Scrape Complete ---")
        print(f"Total unique posts collected: {len(final_posts)}")
        print(f"Total unique comments collected: {len(final_comments)}")

        return {"posts": final_posts, "comments": final_comments}

    def scrape_all(
        self,
        user_handles: Dict[str, List[str]],
        start: datetime.datetime,
        end: datetime.datetime,
        max_posts: int,
        max_comments: int,
        scrape_comments: bool
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        A convenience method to scrape all platforms defined in the user_handles dictionary.

        Args:
            user_handles: A dictionary mapping platform names to lists of user handles.
            (Other arguments are the same as the scrape method)

        Returns:
            A nested dictionary containing the results for all scraped platforms.
            { "PlatformName": {"posts": pd.DataFrame, "comments": pd.DataFrame} }
        """
        print("\n---### Starting Full Scrape for All Provided Platforms ###---")
        all_results = {}
        for platform, handles in user_handles.items():
            # This method will call the main 'scrape' method for each platform
            platform_result = self.scrape(
                platform=platform,
                handles=handles,
                start=start,
                end=end,
                max_posts=max_posts,
                max_comments=max_comments,
                scrape_comments=scrape_comments
            )
            all_results[platform] = platform_result
        print("\n---### Full Scrape Finished ###---")
        return all_results
