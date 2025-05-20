from apify_client import ApifyClient
from .twitter_scraper import ScrapePostsAndComments as ScrapeTwitterPostsAndComments, ScrapePosts as ScrapeTwitterPosts
from .instagram_scraper import ScrapeUserComentsAndPosts as ScrapeInstagramPostsAndComments, ScrapePosts as ScrapeInstagramPosts
from .facebook_scraper import ScrapePostsAndComments as ScrapeFacebookPostsAndComments, ScrapePosts as ScrapeFacebookPosts
from typing import Dict
from pathlib import Path
import datetime
from components.auth import get_api_key # Assuming this correctly gets your API key
import pandas as pd

# --- Path Setup (Keep as is) ---
DEFAULT_PATH = Path("scraped_data")
FACEBOOK_PATH = DEFAULT_PATH / "facebook"
INSTAGRAM_PATH = DEFAULT_PATH / "instagram"
TWITTER_PATH = DEFAULT_PATH / "twitter"

# Ensure directories exist
FACEBOOK_PATH.mkdir(parents=True, exist_ok=True)
INSTAGRAM_PATH.mkdir(parents=True, exist_ok=True)
TWITTER_PATH.mkdir(parents=True, exist_ok=True)

for directory in [FACEBOOK_PATH, INSTAGRAM_PATH, TWITTER_PATH]:
    posts_directory = directory / "posts"
    comments_directory = directory / "comments"
    posts_directory.mkdir(parents=True, exist_ok=True)
    comments_directory.mkdir(parents=True, exist_ok=True)

# --- Scraping Function ---
def scrape_data(
    start: datetime.datetime,
    end: datetime.datetime,
    max_posts: int,
    max_comments: int,
    user_handles: Dict,
    scrape_comments: bool,
    facebook_max_threads: int = 10,
    twitter_max_threads: int = 15,
    instagram_max_threads: int = 10 # Add parameter for Instagram threading
):
    """Scrape data from social media platforms based on user handles."""

    if not user_handles:
        print("No user handles provided.")
        # Return empty DataFrames with the new nested structure for all platforms
        return {
            "Facebook": {"posts": pd.DataFrame(), "comments": pd.DataFrame()},
            "Instagram": {"posts": pd.DataFrame(), "comments": pd.DataFrame()}, # Updated Instagram return structure
            "Twitter": {"posts": pd.DataFrame(), "comments": pd.DataFrame()}
        }

    client = ApifyClient(get_api_key())

    # --- Initialize cumulative DataFrames *before* the loop ---
    # Separate DataFrames for posts and comments for Facebook, Instagram, and Twitter
    cumulative_facebook_posts_df = pd.DataFrame()
    cumulative_facebook_comments_df = pd.DataFrame()

    cumulative_twitter_posts_df = pd.DataFrame()
    cumulative_twitter_comments_df = pd.DataFrame()

    cumulative_instagram_posts_df = pd.DataFrame() 
    cumulative_instagram_comments_df = pd.DataFrame() 


    print("\n--- Starting Multi-Platform Scrape ---")
    print(f"Handles to process: {user_handles}")
    print(f"Date range for posts: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    print(f"Scraping comments: {scrape_comments}")
    print(f"Facebook concurrent comment tasks: {facebook_max_threads}")
    print(f"Twitter concurrent comment tasks: {twitter_max_threads}")
    print(f"Instagram concurrent comment tasks: {instagram_max_threads}") # Print Instagram threads


    try:
        for platform, handles in user_handles.items():
            if not handles:
                print(f"No handles provided for {platform}. Skipping.")
                continue

            print(f"\n--- Processing Platform: {platform} with handles {handles} ---")

            for handle in handles:
                print(f"\n--- Processing Handle: {handle} on {platform} ---")

                # --- Facebook Logic (Keep as is) ---
                if platform == "Facebook":
                    if scrape_comments:
                        # Call the combined function which returns (posts_df, comments_df)
                        posts_df_this_handle, comments_df_this_handle = ScrapeFacebookPostsAndComments(
                            client=client,
                            facebook_handle=handle,
                            start_time=start,
                            end_time=end,
                            max_posts=max_posts,
                            max_comments=max_comments,
                            path=FACEBOOK_PATH,
                            max_threads=facebook_max_threads
                        )

                        # Check if the process for this handle succeeded
                        if posts_df_this_handle is None or comments_df_this_handle is None:
                             print(f"API key issue or critical error during Facebook scrape for {handle}. Skipping this handle.")
                             continue

                        # Append the posts scraped in *this run* for *this handle*
                        if not posts_df_this_handle.empty:
                             print(f"Adding {len(posts_df_this_handle)} posts from {handle} (this run) to cumulative Facebook posts.")
                             cumulative_facebook_posts_df = pd.concat(
                                 [cumulative_facebook_posts_df, posts_df_this_handle],
                                 ignore_index=True
                             )

                        # Append the *combined* comments (new + existing) for *this handle*
                        if not comments_df_this_handle.empty:
                             print(f"Adding {len(comments_df_this_handle)} comments from {handle} (total for handle including existing) to cumulative Facebook comments.")
                             cumulative_facebook_comments_df = pd.concat(
                                 [cumulative_facebook_comments_df, comments_df_this_handle],
                                 ignore_index=True
                             )

                             if 'id' in cumulative_facebook_comments_df.columns:
                                  initial_count = len(cumulative_facebook_comments_df)
                                  cumulative_facebook_comments_df.drop_duplicates(subset=['id'], inplace=True)
                                  if len(cumulative_facebook_comments_df) < initial_count:
                                       print(f"Removed {initial_count - len(cumulative_facebook_comments_df)} duplicate comments across handles in cumulative Facebook comments.")
                             else:
                                  print("Warning: 'id' column not found in Facebook comments for robust cross-handle duplicate checking.")


                    else: # scrape_comments is False, only scrape posts for Facebook
                        posts_df_this_handle = ScrapeFacebookPosts(
                            client=client,
                            facebook_handle=handle,
                            start_time=start,
                            end_time=end,
                            max_posts=max_posts,
                            path=FACEBOOK_PATH
                        )

                        # Check if the process for this handle succeeded
                        if posts_df_this_handle is None:
                             print(f"API key issue or critical error during Facebook post scrape for {handle}. Skipping this handle.")
                             continue

                        # Append the posts from *this run* for *this handle*
                        if not posts_df_this_handle.empty:
                            print(f"Adding {len(posts_df_this_handle)} posts from {handle} (this run) to cumulative Facebook posts.")
                            cumulative_facebook_posts_df = pd.concat(
                                [cumulative_facebook_posts_df, posts_df_this_handle],
                                ignore_index=True
                            )

                        else:
                             print(f"No posts returned from scraper for handle {handle} (comments=False) in this run.")


                # --- Instagram Logic (UPDATED to handle tuple return and separate DFs) ---
                elif platform == "Instagram":
                    if scrape_comments:
                        # Call the combined function which returns (posts_df_this_run, combined_comments_df_for_this_user)
                        posts_df_this_handle, comments_df_this_handle = ScrapeInstagramPostsAndComments(
                            client=client,
                            username=handle,
                            start_time=start, # Pass start_time (used by Instagram scraper)
                            end_time=end,     # Pass end_time (kept for consistency)
                            max_posts=max_posts,
                            max_comments=max_comments,
                            path=INSTAGRAM_PATH,
                            max_threads=instagram_max_threads # Pass the threading parameter
                        )

                        # Check if the process for this handle succeeded
                        # Note: ScrapeInstagramPostsAndComments returns (None, None) if post scrape fails critically
                        if posts_df_this_handle is None or comments_df_this_handle is None:
                             print(f"API key issue or critical error during Instagram scrape for {handle}. Skipping this handle.")
                             continue # Move to the next handle

                        # Append the posts scraped in *this run* for *this handle*
                        if not posts_df_this_handle.empty:
                             print(f"Adding {len(posts_df_this_handle)} posts from {handle} (this run) to cumulative Instagram posts.")
                             cumulative_instagram_posts_df = pd.concat(
                                 [cumulative_instagram_posts_df, posts_df_this_handle],
                                 ignore_index=True
                             )
                            
                        # Append the *combined* comments (new + existing) for *this handle*
                        # ScrapeInstagramPostsAndComments returns the combined historical comments for this user
                        if not comments_df_this_handle.empty:
                             print(f"Adding {len(comments_df_this_handle)} comments from {handle} (total for handle including existing) to cumulative Instagram comments.")
                             cumulative_instagram_comments_df = pd.concat(
                                 [cumulative_instagram_comments_df, comments_df_this_handle],
                                 ignore_index=True
                             )
                             # Drop duplicates in the cumulative comments DataFrame across *all* handles processed so far
                             # This is important if multiple handles could comment on the same post and their comments get pulled multiple times
                             if 'id' in cumulative_instagram_comments_df.columns: # Assuming 'id' is a unique comment identifier from Instagram actor
                                  initial_count = len(cumulative_instagram_comments_df)
                                  cumulative_instagram_comments_df.drop_duplicates(subset=['id'], inplace=True)
                                  if len(cumulative_instagram_comments_df) < initial_count:
                                       print(f"Removed {initial_count - len(cumulative_instagram_comments_df)} duplicate comments across handles in cumulative Instagram comments.")
                             else:
                                  print("Warning: 'id' column not found in Instagram comments for robust cross-handle duplicate checking.")


                    else: # scrape_comments is False, only scrape posts for Instagram
                        posts_df_this_handle = ScrapeInstagramPosts(
                            client=client,
                            username=handle,
                            start_time=start, # Pass start_time
                            end_time=end,     # Pass end_time
                            max_posts=max_posts,
                            path=INSTAGRAM_PATH
                        )

                        # Check if the process for this handle succeeded
                        if posts_df_this_handle is None:
                            print(f"API key issue or critical error during Instagram post scrape for {handle}. Skipping this handle.")
                            continue # Move to the next handle

                        # Append the posts from *this run* for *this handle*
                        if not posts_df_this_handle.empty:
                            print(f"Adding {len(posts_df_this_handle)} posts from {handle} (this run) to cumulative Instagram posts.")
                            cumulative_instagram_posts_df = pd.concat(
                                [cumulative_instagram_posts_df, posts_df_this_handle],
                                ignore_index=True
                            )
                            # Optional: Drop duplicates based on shortcode/url across handles if needed
                            if 'shortcode' in cumulative_instagram_posts_df.columns:
                                initial_count = len(cumulative_instagram_posts_df)
                                cumulative_instagram_posts_df.drop_duplicates(subset=['shortcode'], inplace=True)
                                if len(cumulative_instagram_posts_df) < initial_count:
                                     print(f"Removed {initial_count - len(cumulative_instagram_posts_df)} duplicate Instagram posts across handles based on 'shortcode'.")
                        else:
                             print(f"No posts returned from scraper for handle {handle} (comments=False) in this run.")


                # --- Twitter Logic (Keep as is) ---
                elif platform == "Twitter":
                    if scrape_comments:
                        # Call the combined function which returns (posts_df_this_run, combined_comments_df_for_this_user)
                        posts_df_this_handle, comments_df_this_handle = ScrapeTwitterPostsAndComments(
                            client=client,
                            username=handle,
                            start_time=start, # Twitter scraper uses start/end date range for posts
                            end_time=end,
                            max_posts=max_posts,
                            max_comments=max_comments,
                            path=TWITTER_PATH,
                            max_threads=twitter_max_threads # Pass the threading parameter
                        )

                        # Check if the process for this handle succeeded
                        # Note: ScrapeTwitterPostsAndComments returns (None, None) if post scrape fails critically
                        if posts_df_this_handle is None or comments_df_this_handle is None:
                             print(f"API key issue or critical error during Twitter scrape for {handle}. Skipping this handle.")
                             continue # Move to the next handle

                        # Append the posts scraped in *this run* for *this handle*
                        # ScrapeTwitterPostsAndComments returns posts from the date range in this run
                        if not posts_df_this_handle.empty:
                             print(f"Adding {len(posts_df_this_handle)} posts from {handle} (this run) to cumulative Twitter posts.")
                             cumulative_twitter_posts_df = pd.concat(
                                 [cumulative_twitter_posts_df, posts_df_this_handle],
                                 ignore_index=True
                             )

                        # Append the *combined* comments (new + existing) for *this handle*
                        # ScrapeTwitterPostsAndComments returns the combined historical comments for this user
                        if not comments_df_this_handle.empty:
                             print(f"Adding {len(comments_df_this_handle)} comments from {handle} (total for handle including existing) to cumulative Twitter comments.")
                             cumulative_twitter_comments_df = pd.concat(
                                 [cumulative_twitter_comments_df, comments_df_this_handle],
                                 ignore_index=True
                             )
                             # Drop duplicates in the cumulative comments DataFrame across *all* handles processed so far
                             # This is important if multiple handles could comment on the same post and their comments get pulled multiple times
                             if 'id' in cumulative_twitter_comments_df.columns: # Assuming 'id' is a unique comment/reply identifier from Twitter actor
                                  initial_count = len(cumulative_twitter_comments_df)
                                  cumulative_twitter_comments_df.drop_duplicates(subset=['id'], inplace=True)
                                  if len(cumulative_twitter_comments_df) < initial_count:
                                       print(f"Removed {initial_count - len(cumulative_twitter_comments_df)} duplicate comments across handles in cumulative Twitter comments.")
                             else:
                                  print("Warning: 'id' column not found in Twitter comments for robust cross-handle duplicate checking.")


                    else: # scrape_comments is False, only scrape posts for Twitter
                        # Call the posts-only function which returns a single DataFrame
                        posts_df_this_handle = ScrapeTwitterPosts(
                            client=client,
                            username=handle,
                            start_time=start,
                            end_time=end,
                            max_posts=max_posts,
                            path=TWITTER_PATH
                        )

                        # Check if the process for this handle succeeded
                        if posts_df_this_handle is None:
                            print(f"API key issue or critical error during Twitter post scrape for {handle}. Skipping this handle.")
                            continue # Move to the next handle

                        # Append the posts from *this run* for *this handle*
                        if not posts_df_this_handle.empty:
                            print(f"Adding {len(posts_df_this_handle)} posts from {handle} (this run) to cumulative Twitter posts.")
                            cumulative_twitter_posts_df = pd.concat(
                                [cumulative_twitter_posts_df, posts_df_this_handle],
                                ignore_index=True
                            )
                             # Optional: Drop duplicates based on url/tweetId across handles if needed
                            if 'tweetId' in cumulative_twitter_posts_df.columns:
                                 initial_count = len(cumulative_twitter_posts_df)
                                 cumulative_twitter_posts_df.drop_duplicates(subset=['tweetId'], inplace=True)
                                 if len(cumulative_twitter_posts_df) < initial_count:
                                      print(f"Removed {initial_count - len(cumulative_twitter_posts_df)} duplicate Twitter posts across handles based on 'tweetId'.")
                        else:
                             print(f"No posts returned from scraper for handle {handle} (comments=False) in this run.")

                else:
                    print(f"Warning: Unsupported platform '{platform}' for handle '{handle}'. Skipping.")
                    continue # Skip this platform/handle combination

        # --- Final Cumulative DataFrames & Return ---
        print("\n--- Multi-Platform Scrape Complete ---")
        print(f"Total Facebook Posts collected: {len(cumulative_facebook_posts_df)}")
        print(f"Total Facebook Comments collected: {len(cumulative_facebook_comments_df)}")
        print(f"Total Instagram Posts collected: {len(cumulative_instagram_posts_df)}") # Updated print
        print(f"Total Instagram Comments collected: {len(cumulative_instagram_comments_df)}") # Updated print
        print(f"Total Twitter Posts collected: {len(cumulative_twitter_posts_df)}")
        print(f"Total Twitter Comments collected: {len(cumulative_twitter_comments_df)}")

        # Return the separate cumulative DataFrames for each platform in the new nested structure
        return {
            "Facebook": {"posts": cumulative_facebook_posts_df, "comments": cumulative_facebook_comments_df},
            "Instagram": {"posts": cumulative_instagram_posts_df, "comments": cumulative_instagram_comments_df}, # Updated Instagram return structure
            "Twitter": {"posts": cumulative_twitter_posts_df, "comments": cumulative_twitter_comments_df}
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\nAn error occurred during the scraping process: {e}")
        # Return empty DataFrames with the new nested structure on failure
        return {
            "Facebook": {"posts": None, "comments": None},
            "Instagram": {"posts": None, "comments": None},
            "Twitter": {"posts": None, "comments": None}
        }