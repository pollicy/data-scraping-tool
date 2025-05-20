import pandas as pd
import datetime
from tqdm import tqdm
from pathlib import Path
import concurrent.futures
import os
import time # Added for potential delays

# --- Apify Actor ID (Keep as is) ---
APIFY_ACTOR_ID = "shu8hvrXbJbY3Eb9W"

# --- Helper Functions ---

# Helper function to load existing posts data (for consistency, though not strictly needed for skipping comments)
def load_existing_instagram_posts(path: Path, username: str) -> pd.DataFrame:
    """Loads existing Instagram posts data from saved Excel files for a user."""
    posts_dir = path / "posts"
    if not posts_dir.exists():
        print("Instagram posts directory not found.")
        return pd.DataFrame()

    # Find all Excel files for this username in the posts directory
    pattern = f"*{username}*.xlsx" # Use a broader pattern and then filter/check later
    existing_files = list(posts_dir.glob(pattern))

    if not existing_files:
        print(f"No existing Instagram posts files found for user: {username}.")
        return pd.DataFrame()

    all_existing_posts = []
    print(f"Found {len(existing_files)} potential existing Instagram posts files for {username}. Loading...")

    for f in existing_files:
        try:
            df = pd.read_excel(f)
            # Need to check for a column that identifies the post uniquely
            # 'shortcode' or 'id' or 'url' are good candidates
            if 'shortcode' in df.columns or 'id' in df.columns or 'url' in df.columns:
                all_existing_posts.append(df)
            else:
                 print(f"Warning: Existing Instagram posts file {f} does not contain key columns (shortcode/id/url). Skipping.")
        except Exception as e:
            print(f"Warning: Could not load existing Instagram posts file {f}: {e}")

    if all_existing_posts:
        combined_df = pd.concat(all_existing_posts, ignore_index=True)
        # Drop duplicates based on a unique post identifier, e.g., 'shortcode'
        id_col = 'shortcode' if 'shortcode' in combined_df.columns else ('id' if 'id' in combined_df.columns else ('url' if 'url' in combined_df.columns else None))
        if id_col:
             initial_count = len(combined_df)
             combined_df.drop_duplicates(subset=[id_col], inplace=True)
             if len(combined_df) < initial_count:
                 print(f"Removed {initial_count - len(combined_df)} duplicate Instagram post entries based on '{id_col}' across files.")
        else:
             print("Warning: Neither 'shortcode', 'id', nor 'url' found in existing Instagram posts for duplicate checking.")

        print(f"Loaded {len(combined_df)} unique existing Instagram posts from {len(all_existing_posts)} file(s).")
        return combined_df
    else:
        print("No valid data loaded from existing Instagram posts files.")
        return pd.DataFrame()

# Helper function to load existing comments data
def load_existing_instagram_comments(path: Path, username: str) -> pd.DataFrame:
    """Loads existing Instagram comments data from the combined Excel file for a user."""
    comments_dir = path / "comments"
    if not comments_dir.exists():
        print("Instagram comments directory not found.")
        return pd.DataFrame()

    # We expect a single combined file for comments for this user
    combined_file_path = comments_dir / f"{username}_instagram_comments_combined.xlsx"

    if not combined_file_path.exists():
        print(f"No combined Instagram comments file found at {combined_file_path}.")
        return pd.DataFrame()

    try:
        df = pd.read_excel(combined_file_path)
        # Ensure the necessary column exists for tracking which posts comments belong to
        # 'post_url' (added by scraper) or 'parentPostShortcode' (from actor) are candidates
        if 'post_url' in df.columns or 'parentPostShortcode' in df.columns:
             print(f"Loaded {len(df)} existing Instagram comments from {combined_file_path}.")
             return df
        else:
             print(f"Warning: Existing Instagram comments file {combined_file_path} does not contain 'post_url' or 'parentPostShortcode' column. Cannot use effectively for skipping.")
             return pd.DataFrame()
    except Exception as e:
        print(f"Warning: Could not load existing Instagram comments file {combined_file_path}: {e}")
        return pd.DataFrame()

# --- Modified ScrapePosts Function ---
def ScrapePosts(client, url: str, start_time: datetime.datetime, path: Path, max_posts: int = 100) -> pd.DataFrame | None:
    """Scrape posts for a specific Instagram URL (user profile, hashtag, etc.) newer than a start time."""
    start_time_str = start_time.strftime("%Y-%m-%d")

    print(f"\n--- Starting Instagram post scrape for URL: {url} ---")
    print(f"Fetching posts newer than {start_time_str}")

    payload = {
        "addParentData": False, # Typically False for main posts
        "directUrls": [ url ],
        "enhanceUserSearchWithFacebookPage": False,
        "isUserReelFeedURL": False,
        "isUserTaggedFeedURL": False,
        "onlyPostsNewerThan": start_time_str, # Note: Actor filtering might vary
        "resultsLimit": max_posts,
        "resultsType": "posts",
        "searchLimit": 1, # Assuming directUrl is used, searchLimit might be ignored or used differently
    }

    try:
        print(f"Calling Apify Actor {APIFY_ACTOR_ID} for Instagram posts...")
        run = client.actor(APIFY_ACTOR_ID).call(run_input=payload)
        print(f"Actor run started with ID: {run['id']}")

    except Exception as e:
        print(f"Error calling Apify Actor {APIFY_ACTOR_ID} for {url}. Please check API key/Actor ID/Permissions. Error: {e}")
        return None # Critical failure

    # Fetch Actor results from the run's dataset
    data = []
    dataset_id = run["defaultDatasetId"]
    print(f"Collecting post data from dataset: {dataset_id}...")

    try:
        # Attempt to get item count for tqdm total
        dataset_info = client.dataset(dataset_id).get()
        total_items = dataset_info.get('itemCount')
        if total_items is None: total_items = 0

        for item in tqdm(client.dataset(dataset_id).iterate_items(), total=total_items, desc=f"Processing Instagram posts for {url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]}", unit="post"):
            data.append(item)

    except Exception as e:
        print(f"Error fetching data from dataset {dataset_id}: {e}")
        # Return what we got even if fetching fails partially
        pass

    df = pd.DataFrame(data)
    print(f"Collected {len(df)} Instagram posts from the dataset.")

    if df.empty:
        print(f"No Instagram posts found for {url} newer than {start_time_str}.")
        return df # Return empty DataFrame

    # Save results to a unique file based on username/url part, start_time, and timestamp
    save_dir = path / "posts"
    save_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists

    # Clean url part for filename
    url_cleaned = url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]
    if not url_cleaned: url_cleaned = "instagram_scrape" # Fallback name
    url_cleaned = url_cleaned.replace("?", "_").replace("&", "_").replace("=", "_")

    current_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = save_dir / f"{url_cleaned}_instagram_posts_newerthan_{start_time_str}_{current_timestamp}.xlsx"

    print(f"Saving newly scraped Instagram post data ({len(df)} posts) to {output_filename}...")
    try:
        df.to_excel(output_filename, index=False)
        print("Instagram post data saved successfully.")
    except Exception as e:
        print(f"Error saving Instagram post data to {output_filename}: {e}")

    return df

# --- Modified ScrapePostComments Function ---
def ScrapePostComments(client, post_url: str, max_comments: int = 100) -> pd.DataFrame:
    """Scrape comments for a single Instagram post URL."""
    # Show which post is being processed (truncated shortcode)
    post_id_display = post_url.split('/')[-2] if post_url.endswith('/') else post_url.split('/')[-1]
    if len(post_id_display) > 12: # Shortcode is typically 11 chars, maybe slightly more
        post_id_display = post_id_display[:6] + "..." + post_id_display[-6:]


    payload = {
        "addParentData": False, # Typically False for comments, though depends on actor
        "directUrls": [ post_url ],
        "enhanceUserSearchWithFacebookPage": False,
        "isUserReelFeedURL": False,
        "isUserTaggedFeedURL": False,
        "resultsLimit": max_comments,
        "resultsType": "comments",
        "searchLimit": 1,
    }

    try:
        # print(f"Calling Apify Actor {APIFY_ACTOR_ID} for comments on {post_id_display}...") # Too noisy
        run = client.actor(APIFY_ACTOR_ID).call(run_input=payload)
        # print(f"Comment actor run started for {post_id_display} with ID: {run['id']}") # Too noisy

    except Exception as e:
        print(f"\nError calling Apify Actor {APIFY_ACTOR_ID} for post {post_id_display}. Error: {e}")
        # Return empty DataFrame if actor call fails for this post
        return pd.DataFrame() # Return empty DF instead of None

    # Fetch Actor results from the run's dataset
    data = []
    dataset_id = run["defaultDatasetId"]
    # print(f"Collecting comment data from dataset: {dataset_id} for post {post_id_display}...") # Too noisy

    try:
        # Note: TQDM not used here as it's per thread
        for item in client.dataset(dataset_id).iterate_items():
            data.append(item)
        # print(f"Collected {len(data)} comments for post {post_id_display}") # Too noisy
    except Exception as e:
         print(f"\nError fetching comment data from dataset {dataset_id} for post {post_id_display}: {e}")
         # Return partial data or empty df if fetching fails
         pass

    df = pd.DataFrame(data)

    # Add context columns IF data was collected
    if not df.empty:
        df['post_url'] = post_url
        if 'parentPostShortcode' in df.columns:
             # If parentPostShortcode is available, we can potentially link comments without relying on post_url
             pass # Already in DF
        if 'ownerUsername' in df.columns: # This might be the commenter's username, not post author
             pass # Already in DF

    # print(f"Found {len(df)} comments for post {post_id_display}") # Too noisy
    return df

# --- Helper function for ThreadPoolExecutor ---
def process_instagram_post_comments(args):
    """Helper function to process comments for a single Instagram post in a thread."""
    client, post_url, max_comments = args
    # Add other potential args needed by ScrapeComments like post_author_username
    return ScrapePostComments(client, post_url, max_comments)


# --- Modified ScrapeUserComentsAndPosts Function ---
def ScrapeUserComentsAndPosts(
    client,
    username: str,
    end_time: datetime.datetime,
    start_time: datetime.datetime, 
    path: Path,
    max_posts: int = 100,
    max_comments: int = 100,
    max_threads: int = 10 # New parameter for controlling concurrency
) -> tuple[pd.DataFrame, pd.DataFrame] | tuple[None, None]:
    """
    Scrape Instagram posts (newer than start_time) and their comments for a specific user.
    Uses threading for comment scraping and avoids re-scraping comments for posts already processed.

    Returns a tuple of (posts_df_this_run, combined_comments_df_for_this_user)
    or (None, None) if post scraping fails.
    """
    url = f"https://www.instagram.com/{username}/"

    print("-" * 60)
    print(f"--- Starting combined Instagram scrape process for user: {username} ---")
    print(f"Fetching posts newer than {start_time.strftime('%Y-%m-%d')}")
    print(f"Using {max_threads} threads for comment scraping.")
    print("-" * 60)

    # --- 1. Load existing data to identify already scraped items ---
    # We primarily need the list of post identifiers for which comments have already been saved
    existing_comments_df = load_existing_instagram_comments(path, username)
    # Identify unique identifiers for posts from the existing comments data
    existing_comment_post_identifiers = set() # Use post_url or parentPostShortcode
    if not existing_comments_df.empty:
        if 'post_url' in existing_comments_df.columns:
             existing_comment_post_identifiers.update(existing_comments_df['post_url'].dropna().unique())
        # If 'post_url' is not reliable or available, use 'parentPostShortcode'
        if 'parentPostShortcode' in existing_comments_df.columns:
             existing_comment_post_identifiers.update(existing_comments_df['parentPostShortcode'].dropna().unique())

    print(f"Identified {len(existing_comment_post_identifiers)} posts with existing comments data.")

    # --- 2. Scrape Posts newer than start_time ---
    # ScrapePosts saves its results independently. It returns posts from the specified criteria.
    scraped_posts_df = ScrapePosts(
        client=client,
        url=url,
        start_time=start_time,
        path=path,
        max_posts=max_posts
    )

    # Check if post scraping failed or returned no posts
    if scraped_posts_df is None:
        print("Instagram post scraping failed. Aborting comment scraping.")
        return None, None # Indicate critical failure

    if scraped_posts_df.empty or ("url" not in scraped_posts_df.columns and "shortcode" not in scraped_posts_df.columns):
        print("No posts scraped or required columns (url/shortcode) missing. No comments to scrape.")
        # Return scraped posts (empty) and existing comments
        final_comments_df = existing_comments_df # Or load again if preferred
        return scraped_posts_df, final_comments_df

    # Ensure necessary columns for comment scraping are present and not null
    posts_df_for_comments = scraped_posts_df.copy() # Work on a copy
    if 'url' not in posts_df_for_comments.columns:
         print("Warning: 'url' column not found in scraped Instagram posts. Attempting to use 'shortcode'.")
         if 'shortcode' in posts_df_for_comments.columns:
              # Attempt to construct URL from shortcode if possible, or use shortcode as 
              posts_df_for_comments['url'] = posts_df_for_comments['shortcode'].apply(
                  lambda sc: f"https://www.instagram.com/p/{sc}/" if pd.notna(sc) else None
              )
         else:
              print("Error: Neither 'url' nor 'shortcode' found in scraped posts. Cannot scrape comments.")
              # Return scraped posts (potentially with missing columns) and existing comments
              final_comments_df = existing_comments_df
              return scraped_posts_df, final_comments_df

    # Remove posts without a usable identifier (url or shortcode leading to url)
    posts_df_for_comments = posts_df_for_comments.dropna(subset=['url'])
    if posts_df_for_comments.empty:
         print("No Instagram posts with valid URLs found after scraping. Cannot scrape comments.")
         final_comments_df = existing_comments_df
         return scraped_posts_df, final_comments_df


    # --- 3. Determine which posts need comments scraped ---
    posts_to_scrape_comments_df = posts_df_for_comments[
        (~posts_df_for_comments['url'].isin(existing_comment_post_identifiers))
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    all_post_urls_from_scrape = posts_df_for_comments['url'].tolist()
    total_posts_with_urls = len(all_post_urls_from_scrape)
    posts_to_scrape_comments_count = len(posts_to_scrape_comments_df)

    # Calculate how many were skipped from the posts we *could* scrape comments for
    already_scraped_count = len(existing_comment_post_identifiers.intersection(set(posts_df_for_comments['url'].tolist())))
    skipped_post_count = total_posts_with_urls - posts_to_scrape_comments_count - (total_posts_with_urls - len(posts_df_for_comments)) # Posts without URL + those with existing comments

    print(f"Total posts found in this scrape run: {len(scraped_posts_df)}")
    print(f"Posts from this run with valid URLs/Shortcodes for comments: {len(posts_df_for_comments)}")
    print(f"Posts with existing comments data identified: {already_scraped_count} (among those with URLs)")

    # Recalculate skipped count more simply: Total posts with URLs minus those we will attempt to scrape
    skipped_count_from_urls = len(posts_df_for_comments) - posts_to_scrape_comments_count
    if skipped_count_from_urls > 0:
        print(f"Skipping comment scraping for {skipped_count_from_urls} posts from this run based on existing data.")
    print(f"Proceeding to scrape comments for {posts_to_scrape_comments_count} posts.")


    newly_scraped_comments_list = []

    # --- 4. Scrape Comments using Threading ---
    if not posts_to_scrape_comments_df.empty:
        print(f"Starting Instagram comment scraping using {max_threads} threads...")

        # Prepare arguments list for the thread pool
        # Columns needed: 'url' (for scraper), maybe 'ownerUsername' if needed by ScrapeComments
        process_args = [
            (client, row['url'], max_comments) # Add row['ownerUsername'] if needed
            for _, row in posts_to_scrape_comments_df.iterrows()
        ]

        # Use ThreadPoolExecutor for concurrent comment scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Create a dictionary to map future objects to post URLs for easier tracking
            future_to_url = {
                executor.submit(process_instagram_post_comments, args): args[1] # args[1] is post_url
                for args in process_args
            }

            # Use tqdm with as_completed to show progress
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_url),
                                total=len(future_to_url),
                                desc=f"Scraping comments for {username}",
                                unit="post",
                                leave=True)

            for future in progress_bar:
                post_url = future_to_url[future]
                try:
                    comments_df = future.result() # Retrieves the DataFrame or raises exception
                    if not comments_df.empty:
                        # Metadata like post_url is added inside ScrapeComments
                        newly_scraped_comments_list.append(comments_df)

                except Exception as exc:
                    # Handle exceptions raised by ScrapeComments for individual posts
                    post_id_display = post_url.split('/')[-2] if post_url.endswith('/') else post_url.split('/')[-1]
                    if len(post_id_display) > 12:
                        post_id_display = post_id_display[:6] + "..." + post_id_display[-6:]
                    print(f"\nPost {post_id_display} ({post_url}) generated an exception during comment scraping: {exc}")
                    # Continue processing other posts

            progress_bar.close()

        # --- 5. Combine newly scraped comments ---
        newly_scraped_comments_df = pd.DataFrame() # Initialize as empty
        if newly_scraped_comments_list:
             newly_scraped_comments_df = pd.concat(newly_scraped_comments_list, ignore_index=True)
             print(f"Successfully scraped comments for {len(newly_scraped_comments_list)} posts in this run.")
             print(f"Collected {len(newly_scraped_comments_df)} new comments in this run.")
             # Add the instagram username to the newly scraped comments
             newly_scraped_comments_df['instagram username'] = username
        else:
            print("No new comments were successfully scraped for the selected posts in this run.")

        # --- 6. Combine with existing comments and save ---
        # Use the initially loaded existing_comments_df
        if not existing_comments_df.empty:
             # Combine existing and new comments
             combined_comments_df = pd.concat([existing_comments_df, newly_scraped_comments_df], ignore_index=True)
             print(f"Combined new comments with {len(existing_comments_df)} existing comments.")

             # Drop duplicates in the final combined set
             # Use a unique identifier for comments. 'id' from the actor is common.
             id_col = 'id' # Assuming 'id' is the unique comment identifier returned by the actor
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
        output_filename = comments_dir / f"{username}_instagram_comments_combined.xlsx"

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
