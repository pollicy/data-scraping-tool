import pandas as pd
import datetime
import concurrent.futures
import os
import re
from tqdm import tqdm
from pathlib import Path
import time

# --- Apify Actor IDs (Keep as is) ---
# Assuming these are correct for the Twitter actors you are using
POSTS_ACTOR_ID = "nfp1fpt5gUlBwPcor"  # Twitter Profile Scraper (or similar for user posts)
COMMENTS_ACTOR_ID = "qhybbvlFivx7AP0Oh" # Twitter Conversation Scraper (or similar for replies)


# --- Helper Functions ---

def parse_twitter_date(date_str):
    """Parse Twitter date format to datetime object"""
    if not isinstance(date_str, str):
         # Handle non-string inputs gracefully
         return None
    try:
        return datetime.datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
    except ValueError:
        # Fallback regex for slight variations or if %z parsing fails
        pattern = r'(\w{3}) (\w{3}) (\d{1,2}) (\d{2}):(\d{2}):(\d{2}) \+?\d{4} (\d{4})' # Added +?
        match = re.match(pattern, date_str)

        if match:
             month_map = {
                 'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
             }
             try:
                 _, month_name, day, hour, minute, second, year = match.groups()
                 month = month_map.get(month_name)
                 if month is not None:
                     return datetime.datetime(
                         int(year), month, int(day),
                         int(hour), int(minute), int(second)
                     )
             except (ValueError, TypeError):
                 pass # Regex match didn't yield valid parts


        print(f"Unable to parse date: {date_str}")
        return None

# Helper function to load existing posts data
def load_existing_twitter_posts(path: Path, username: str) -> pd.DataFrame:
    """Loads existing Twitter posts data from saved Excel files for a user."""
    posts_dir = path / "posts"
    if not posts_dir.exists():
        print("Twitter posts directory not found.")
        return pd.DataFrame()

    # Find all Excel files for this username in the posts directory
    # The naming convention is {username}_{start_time}_to_{end_time}_{timestamp}_posts.xlsx
    pattern = f"*{username}*posts.xlsx"
    existing_files = list(posts_dir.glob(pattern))

    if not existing_files:
        print(f"No existing Twitter posts files found for user: {username}.")
        return pd.DataFrame()

    all_existing_posts = []
    print(f"Found {len(existing_files)} potential existing Twitter posts files for {username}. Loading...")

    for f in existing_files:
        try:
            df = pd.read_excel(f)
            # Need to check for a column that identifies the post uniquely
            # 'url' or 'tweetId' are good candidates
            if 'url' in df.columns or 'tweetId' in df.columns:
                all_existing_posts.append(df)
            else:
                 print(f"Warning: Existing Twitter posts file {f} does not contain 'url' or 'tweetId' column. Skipping.")
        except Exception as e:
            print(f"Warning: Could not load existing Twitter posts file {f}: {e}")

    if all_existing_posts:
        combined_df = pd.concat(all_existing_posts, ignore_index=True)
        # Drop duplicates based on a unique post identifier, e.g., 'tweetId' or 'url'
        id_col = 'tweetId' if 'tweetId' in combined_df.columns else ('url' if 'url' in combined_df.columns else None)
        if id_col:
             initial_count = len(combined_df)
             combined_df.drop_duplicates(subset=[id_col], inplace=True)
             if len(combined_df) < initial_count:
                 print(f"Removed {initial_count - len(combined_df)} duplicate Twitter post entries based on '{id_col}' across files.")
        else:
             print("Warning: Neither 'tweetId' nor 'url' found in existing Twitter posts for duplicate checking.")

        print(f"Loaded {len(combined_df)} unique existing Twitter posts from {len(all_existing_posts)} file(s).")
        return combined_df
    else:
        print("No valid data loaded from existing Twitter posts files.")
        return pd.DataFrame()

# Helper function to load existing comments data
def load_existing_twitter_comments(path: Path, username: str) -> pd.DataFrame:
    """Loads existing Twitter comments (replies) data from the combined Excel file for a user."""
    comments_dir = path / "comments"
    if not comments_dir.exists():
        print("Twitter comments directory not found.")
        return pd.DataFrame()

    # We expect a single combined file for comments for this user
    combined_file_path = comments_dir / f"{username}_twitter_comments_combined.xlsx"

    if not combined_file_path.exists():
        print(f"No combined Twitter comments file found at {combined_file_path}.")
        return pd.DataFrame()

    try:
        df = pd.read_excel(combined_file_path)
        # Ensure the necessary column exists for tracking which posts comments belong to
        # 'post_url' (added by scraper) or 'conversationId' (from actor) are candidates
        if 'post_url' in df.columns or 'conversationId' in df.columns:
             print(f"Loaded {len(df)} existing Twitter comments from {combined_file_path}.")
             return df
        else:
             print(f"Warning: Existing Twitter comments file {combined_file_path} does not contain 'post_url' or 'conversationId' column. Cannot use effectively for skipping.")
             return pd.DataFrame()
    except Exception as e:
        print(f"Warning: Could not load existing Twitter comments file {combined_file_path}: {e}")
        return pd.DataFrame()


# --- Modified ScrapePosts Function ---
def ScrapePosts(client, username, start_time: datetime.datetime, end_time: datetime.datetime, path: Path, max_posts: int = 100) -> pd.DataFrame | None:
    """Scrape posts for a specific user within a date range."""
    start_time_str = start_time.strftime("%Y-%m-%d")
    end_time_str = end_time.strftime("%Y-%m-%d")

    print(f"\n--- Starting Twitter post scrape for user: {username} ---")
    print(f"Fetching posts between {start_time_str} and {end_time_str}")

    payload = {
        "start": start_time_str,
        "end": end_time_str,
        "maxItems": max_posts,
        "sort": "Latest", # Or "Popular" depending on the actor's capability/your need
        "twitterHandles": [f"{username}"]
    }

    try:
        print(f"Calling Apify Actor {POSTS_ACTOR_ID} for Twitter posts...")
        run = client.actor(POSTS_ACTOR_ID).call(run_input=payload)
        print(f"Actor run started with ID: {run['id']}")

    except Exception as e:
        print(f"Error calling Apify Actor {POSTS_ACTOR_ID} for {username}. Please check API key/Actor ID/Permissions. Error: {e}")
        return None

    # Fetch Actor results from the run's dataset
    data = []
    dataset_id = run["defaultDatasetId"]
    print(f"Collecting post data from dataset: {dataset_id}...")

    try:
        # Attempt to get item count for tqdm total
        dataset_info = client.dataset(dataset_id).get()
        total_items = dataset_info.get('itemCount')
        if total_items is None: total_items = 0

        for item in tqdm(client.dataset(dataset_id).iterate_items(), total=total_items, desc=f"Processing tweets for {username}", unit="tweet"):
            data.append(item)

    except Exception as e:
        print(f"Error fetching data from dataset {dataset_id}: {e}")
        # Even if fetching fails partially, return what we got
        pass

    df = pd.DataFrame(data)
    print(f"Collected {len(df)} tweets from the dataset.")

    if df.empty:
        print("No tweets found within the specified date range by the actor.")
        return df # Return empty DataFrame instead of None

    # Parse createdAt dates and add 'parsed_date' column
    if 'createdAt' in df.columns:
        df['parsed_date'] = df['createdAt'].apply(parse_twitter_date)
        # Filter out rows where date parsing failed if necessary, or handle None later
        df = df.dropna(subset=['parsed_date']) # Drop rows where date couldn't be parsed
        print(f"After date parsing: {len(df)} valid tweets.")
    else:
        print("Warning: 'createdAt' column not found in tweet data.")
        df['parsed_date'] = None # Add the column even if no data

    # Save results to a unique file based on username, date range, and timestamp
    save_dir = path / "posts"
    save_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists

    # Clean username for filename
    username_cleaned = username.replace("/", "_").replace("?", "_").replace("&", "_")
    current_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = save_dir / f"{username_cleaned}_twitter_posts_{start_time_str}_to_{end_time_str}_{current_timestamp}.xlsx"

    print(f"Saving newly scraped tweet data ({len(df)} tweets) to {output_filename}...")
    try:
        df.to_excel(output_filename, index=False)
        print("Tweet data saved successfully.")
    except Exception as e:
        print(f"Error saving tweet data to {output_filename}: {e}")

    return df

# --- Modified ScrapeComments Function (minor changes) ---
def ScrapeComments(client, post_url, post_text, max_comments: int = 100) -> pd.DataFrame:
    """Scrape comments (replies) for a specific post URL."""
     # Show which post is being processed (truncated)
    post_url_display = post_url.split("/")[-1] if "/" in post_url else post_url
    if len(post_url_display) > 20: # Shorter display for Twitter tweet IDs
        post_url_display = post_url_display[:10] + "..." + post_url_display[-10:]

    # print(f"Scraping comments for post ID: {post_url_display}") # Too noisy in threaded execution

    payload = {
        "postUrls": [f"{post_url}"],
        "resultsLimit": max_comments,
    }

    try:
        # print(f"Calling Apify Actor {COMMENTS_ACTOR_ID} for comments on {post_url_display}...") # Too noisy
        run = client.actor(COMMENTS_ACTOR_ID).call(run_input=payload)
        # print(f"Comment actor run started for {post_url_display} with ID: {run['id']}") # Too noisy

    except Exception as e:
        print(f"\nError calling Apify Actor {COMMENTS_ACTOR_ID} for post {post_url_display}. Error: {e}")
        # Return empty DataFrame if actor call fails for this post
        return pd.DataFrame()

    # Fetch Actor results from the run's dataset
    data = []
    dataset_id = run["defaultDatasetId"]
    # print(f"Collecting comment data from dataset: {dataset_id} for post {post_url_display}...") # Too noisy

    try:
        # Note: TQDM not used here as it's per thread
        for item in client.dataset(dataset_id).iterate_items():
            data.append(item)
        # print(f"Collected {len(data)} comments for post {post_url_display}") # Too noisy
    except Exception as e:
         print(f"\nError fetching comment data from dataset {dataset_id} for post {post_url_display}: {e}")
         # Return partial data or empty df if fetching fails
         pass

    df = pd.DataFrame(data)

    # Add context columns IF data was collected
    if not df.empty:
        df['tweet_text'] = post_text # The text of the tweet replies are associated with
        df['post_url'] = post_url   # The URL of the tweet replies are associated with
    return df

# --- Helper function for ThreadPoolExecutor ---
# Adjusted args to match what ScrapeComments needs and what we have
def process_twitter_post_comments(args):
    """Helper function to process comments for a single Twitter post in a thread."""
    client, post_url, post_text, max_comments = args # reply_count removed
    return ScrapeComments(client, post_url, post_text, max_comments)


# --- Modified ScrapePostsAndComments Function ---
def ScrapePostsAndComments(
    client,
    username: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    path: Path,
    max_posts: int = 100,
    max_comments: int = 100,
    max_threads: int = 10 # New parameter for controlling concurrency
) -> tuple[pd.DataFrame, pd.DataFrame] | tuple[None, None]:
    """
    Scrape Twitter posts and their comments (replies) for a specific user.
    Uses threading for comment scraping and avoids re-scraping comments for posts already processed.

    Returns a tuple of (posts_df_this_run, combined_comments_df_for_this_user)
    or (None, None) if post scraping fails.
    """
    print("-" * 60)
    print(f"--- Starting combined Twitter scrape process for {username} ---")
    print(f"Posts between {start_time.strftime('%Y-%m-%d')} and {end_time.strftime('%Y-%m-%d')}")
    print(f"Using {max_threads} threads for comment scraping.")
    print("-" * 60)

    # --- 1. Load existing data to identify already scraped items ---
    existing_comments_df = load_existing_twitter_comments(path, username)
    # Identify unique identifiers for posts from the existing comments data
    existing_comment_post_urls = set()
    if not existing_comments_df.empty:
        if 'post_url' in existing_comments_df.columns:
             existing_comment_post_urls.update(existing_comments_df['post_url'].dropna().unique())
             

    print(f"Identified {len(existing_comment_post_urls)} posts with existing comments data.")

    # --- 2. Scrape Posts for the specified date range ---
    # ScrapePosts saves its results independently. It returns posts within the date range.
    scraped_posts_df = ScrapePosts(
        client=client,
        username=username,
        start_time=start_time,
        end_time=end_time,
        path=path,
        max_posts=max_posts
    )

    # Check if post scraping failed or returned no posts
    if scraped_posts_df is None:
        print("Twitter post scraping failed. Aborting comment scraping.")
        return None, None # Indicate failure for both

    if scraped_posts_df.empty or "url" not in scraped_posts_df.columns:
        print("No posts scraped or 'url' column missing. No comments to scrape.")
        # Load existing comments just in case, though the main scraper already did
        final_comments_df = load_existing_twitter_comments(path, username)
        # Return the scraped posts_df (even if empty) and the loaded existing comments_df
        return scraped_posts_df, final_comments_df

    # --- 3. Determine which posts need comments scraped ---
    # Filter posts that have replies reported by the post scraper AND don't have existing comments data
    # Also ensure the post has a 'url' which is needed for the comments actor
    posts_to_scrape_comments_df = scraped_posts_df[
        (scraped_posts_df['replyCount'] > 0) & # Actor reported replies
        (scraped_posts_df['url'].notna())    & # Ensure URL is available
        (~scraped_posts_df['url'].isin(existing_comment_post_urls)) # URL is NOT in our set of already scraped posts
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    all_post_urls_from_scrape = scraped_posts_df['url'].dropna().tolist()
    total_posts_with_urls = len(all_post_urls_from_scrape)
    posts_with_replies_count = len(scraped_posts_df[scraped_posts_df['replyCount'] > 0])
    posts_to_scrape_comments_count = len(posts_to_scrape_comments_df)

    skipped_post_count = posts_with_replies_count - posts_to_scrape_comments_count
    already_scraped_count = len(existing_comment_post_urls.intersection(set(scraped_posts_df['url'].dropna().tolist())))


    print(f"Total posts found in this scrape run: {len(scraped_posts_df)}")
    print(f"Posts with 'url' column: {total_posts_with_urls}")
    print(f"Posts with replyCount > 0: {posts_with_replies_count}")
    print(f"Posts already processed (comments exist): {already_scraped_count}")

    if skipped_post_count > 0:
        print(f"Skipping comment scraping for {skipped_post_count} posts from this run based on existing data.")
    print(f"Proceeding to scrape comments for {posts_to_scrape_comments_count} posts.")


    newly_scraped_comments_list = []

    # --- 4. Scrape Comments using Threading ---
    if not posts_to_scrape_comments_df.empty:
        print(f"Starting Twitter comment scraping using {max_threads} threads...")

        # Prepare arguments list for the thread pool
        # Columns needed: 'url' (for scraper), 'text' (to add context)
        process_args = [
            (client, row['url'], row['text'], max_comments) # Pass what process_twitter_post_comments expects
            for _, row in posts_to_scrape_comments_df.iterrows()
        ]

        # Use ThreadPoolExecutor for concurrent comment scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Create a dictionary to map future objects to post URLs for easier tracking
            future_to_url = {
                executor.submit(process_twitter_post_comments, args): args[1] # args[1] is post_url
                for args in process_args
            }

            # Use tqdm with as_completed to show progress
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_url),
                                total=len(future_to_url),
                                desc=f"Scraping replies for {username}",
                                unit="post",
                                leave=True)

            for future in progress_bar:
                post_url = future_to_url[future]
                try:
                    comments_df = future.result() # Retrieves the DataFrame or raises exception
                    if not comments_df.empty:
                        # Metadata like post_url and tweet_text are added inside ScrapeComments
                        newly_scraped_comments_list.append(comments_df)

                except Exception as exc:
                    # Handle exceptions raised by ScrapeComments for individual posts
                    post_url_display = post_url.split("/")[-1] if "/" in post_url else post_url
                    if len(post_url_display) > 20:
                         post_url_display = post_url_display[:10] + "..." + post_url_display[-10:]
                    print(f"\nPost {post_url_display} ({post_url}) generated an exception during comment scraping: {exc}")
                    # Continue processing other posts

            progress_bar.close()

        # --- 5. Combine newly scraped comments ---
        newly_scraped_comments_df = pd.DataFrame() # Initialize as empty
        if newly_scraped_comments_list:
             newly_scraped_comments_df = pd.concat(newly_scraped_comments_list, ignore_index=True)
             print(f"Successfully scraped comments for {len(newly_scraped_comments_list)} posts in this run.")
             print(f"Collected {len(newly_scraped_comments_df)} new comments in this run.")
             # Add the twitter username to the newly scraped comments
             newly_scraped_comments_df['twitter username'] = username
        else:
            print("No new comments were successfully scraped for the selected posts in this run.")

        # --- 6. Combine with existing comments and save ---
        # Use the initially loaded existing_comments_df
        if not existing_comments_df.empty:
             # Combine existing and new comments
             combined_comments_df = pd.concat([existing_comments_df, newly_scraped_comments_df], ignore_index=True)
             print(f"Combined new comments with {len(existing_comments_df)} existing comments.")

             # Drop duplicates in the final combined set
             # Use a unique identifier for replies/comments. 'id' from the actor is common.
             id_col = 'id' # Assuming 'id' is the unique comment/reply ID returned by the actor
             if id_col in combined_comments_df.columns:
                  initial_count = len(combined_comments_df)
                  combined_comments_df.drop_duplicates(subset=[id_col], inplace=True)
                  if len(combined_comments_df) < initial_count:
                       print(f"Removed {initial_count - len(combined_comments_df)} duplicate comments based on '{id_col}' in the combined dataset.")
             else:
                  print(f"Warning: '{id_col}' column not found in comments for robust duplicate checking.")

        else:
             # No existing comments, the combined dataframe is just the new ones
             combined_comments_df = newly_scraped_comments_df
             print("No existing comments found. Saving only newly scraped comments.")


        # Save the combined comments data to the single cumulative file
        comments_dir = path / "comments"
        comments_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists
        output_filename = comments_dir / f"{username}_twitter_comments_combined.xlsx"

        print(f"Saving combined comments data ({len(combined_comments_df)} total unique comments) to {output_filename}...")
        try:
            combined_comments_df.to_excel(output_filename, index=False)
            print("Combined comments data saved successfully.")
            final_comments_df = combined_comments_df
        except Exception as e:
            print(f"Error saving combined comments data to {output_filename}: {e}")
            # If saving fails, return the dataframe we attempted to save
            final_comments_df = combined_comments_df

    else:
        print("No posts required comment scraping or no comments were found in this run.")
        # If no new comments were scraped, the final comments dataframe is just the existing one
        final_comments_df = existing_comments_df
        print(f"Loaded existing comments dataframe has {len(final_comments_df)} rows.")


    # Return the posts scraped in this run and the total combined comments for this user
    return scraped_posts_df, final_comments_df
