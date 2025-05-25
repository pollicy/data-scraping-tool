import pandas as pd
import datetime
from tqdm import tqdm
from pathlib import Path
import concurrent.futures
import time
from apify_client import ApifyClient

POSTS_ACTOR_ID = "KoJrdxJCTtpon81KY" 
COMMENTS_ACTOR_ID = "thDyWzaBBQxt4VOfW" 

# Helper function to load existing posts data
def load_existing_posts(path: Path, facebook_handle: str) -> pd.DataFrame:
    """Loads existing posts data from saved Excel files for a handle."""
    posts_dir = path / "posts"
    if not posts_dir.exists():
        print("No existing posts directory found.")
        return pd.DataFrame()


    pattern = f"*{facebook_handle}*.xlsx"
    existing_files = list(posts_dir.glob(pattern))

    if not existing_files:
        print(f"No existing posts files found for handle: {facebook_handle}.")
        return pd.DataFrame()

    all_existing_posts = []
    print(f"Found {len(existing_files)} potential existing posts files for {facebook_handle}. Loading...")

    for f in existing_files:
        try:
            df = pd.read_excel(f)
            # Ensure the necessary columns exist
            if 'url' in df.columns and 'text' in df.columns:
                all_existing_posts.append(df)
            else:
                 print(f"Warning: Existing posts file {f} does not contain required columns ('url', 'text'). Skipping.")
        except Exception as e:
            print(f"Warning: Could not load existing posts file {f}: {e}")

    if all_existing_posts:
        combined_df = pd.concat(all_existing_posts, ignore_index=True)
        # Drop duplicates based on post URL
        initial_count = len(combined_df)
        combined_df.drop_duplicates(subset=['url'], inplace=True)
        if len(combined_df) < initial_count:
            print(f"Removed {initial_count - len(combined_df)} duplicate post entries based on URL across files.")
        print(f"Loaded {len(combined_df)} unique existing posts from {len(all_existing_posts)} file(s).")
        return combined_df
    else:
        print("No valid data loaded from existing posts files.")
        return pd.DataFrame()

# Helper function to load existing comments data
def load_existing_comments(path: Path, facebook_handle: str) -> pd.DataFrame:
    """Loads existing comments data from the combined Excel file for a handle."""
    comments_dir = path / "comments"
    if not comments_dir.exists():
        print("No existing comments directory found.")
        return pd.DataFrame()

    # We expect a single combined file for comments
    # Assuming the combined file name is {handle}_facebook_comments_combined.xlsx
    combined_file_path = comments_dir / f"{facebook_handle}_facebook_comments_combined.xlsx"

    if not combined_file_path.exists():
        print(f"No combined comments file found at {combined_file_path}.")
        return pd.DataFrame()

    try:
        df = pd.read_excel(combined_file_path)
        # Ensure the necessary columns exist for tracking which posts have comments and post text
        if 'post_url' in df.columns and 'post_text' in df.columns:
             print(f"Loaded {len(df)} existing comments from {combined_file_path}.")
             return df
        else:
             print(f"Warning: Existing comments file {combined_file_path} does not contain required columns ('post_url', 'post_text'). Cannot use for skipping or post text lookup.")
             # Return empty DataFrame if crucial columns are missing, as we can't rely on it
             return pd.DataFrame()
    except Exception as e:
        print(f"Warning: Could not load existing comments file {combined_file_path}: {e}")
        return pd.DataFrame()

# Keep ScrapePosts focused, but it will save to a unique file per run
def ScrapePosts(client: ApifyClient, facebook_handle: str, start_time: datetime.datetime, end_time: datetime.datetime, path: Path, max_posts: int = 100) -> pd.DataFrame | None:
    """Scrapes posts for a given Facebook handle and date range."""
    url = f"https://www.facebook.com/{facebook_handle}"
    print(f"\n--- Starting post scrape for Facebook handle: {facebook_handle} ---")
    print(f"Fetching posts from {url} between {start_time.strftime('%Y-%m-%d')} and {end_time.strftime('%Y-%m-%d')}")

    payload = {
        "startUrls": [
            {
            "url": url,
            }
        ],
        "resultsLimit": max_posts,
        # Actors typically filter by date based on post creation date, not scrape date
        # Using ISO format as it's generally safer
        "onlyPostsNewerThan": start_time.isoformat(),
        "onlyPostsOlderThan": end_time.isoformat(),
    }

    try:
        print(f"Calling Apify Actor {POSTS_ACTOR_ID} for posts...")
        run = client.actor(POSTS_ACTOR_ID).call(run_input=payload)
        print(f"Actor run started with ID: {run['id']}")

    except Exception as e:
        print(f"Error calling Apify Actor {POSTS_ACTOR_ID}. Please check API key/Actor ID/Permissions. Error: {e}")
        return None

    # Fetch Actor results from the run's dataset
    data = []
    dataset_id = run["defaultDatasetId"]
    print(f"Collecting post data from dataset: {dataset_id}...")

    try:
        # Attempt to get item count for tqdm total
        dataset_info = client.dataset(dataset_id).get()
        total_items = dataset_info.get('itemCount')
        if total_items is None: total_items = 0 # Handle cases where count isn't immediately available

        # Use iterate_items() for potentially large datasets
        for item in tqdm(client.dataset(dataset_id).iterate_items(), total=total_items, desc=f"Processing posts for {facebook_handle}", unit="post"):
            data.append(item)

    except Exception as e:
        print(f"Error fetching data from dataset {dataset_id}: {e}")
        # Even if fetching fails partially, return what we got
        pass

    df = pd.DataFrame(data)
    print(f"Collected {len(df)} posts from the dataset.")

    if df.empty:
        print("No posts found within the specified date range by the actor.")
        return df # Return empty DataFrame instead of None

    # Save results to a unique file based on handle, date range, and timestamp
    save_dir = path / "posts"
    save_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists

    # Clean handle for filename
    handle_cleaned = facebook_handle.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")
    current_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = save_dir / f"{handle_cleaned}_facebook_posts_{start_time.strftime('%Y-%m-%d')}_to_{end_time.strftime('%Y-%m-%d')}_{current_timestamp}.xlsx"

    print(f"Saving newly scraped post data ({len(df)} posts) to {output_filename}...")
    try:
        # Ensure 'text' column exists before saving
        if 'text' not in df.columns:
             print("Warning: 'text' column not found in scraped posts data.")
             df['text'] = None # Add it with None values if missing

        df.to_excel(output_filename, index=False)
        print("Post data saved successfully.")
    except Exception as e:
        print(f"Error saving post data to {output_filename}: {e}")

    return df

# Keep ScrapePostComments focused on scraping a single post's comments
def ScrapePostComments(client: ApifyClient, post_url: str, max_comments: int = 100) -> pd.DataFrame:
    """Scrapes comments for a single post URL."""
    payload = {
        "post_url": post_url,
        "count": max_comments,
    }

    try:
        run = client.actor(COMMENTS_ACTOR_ID).call(run_input=payload)
        # print(f"Comment actor run started for {post_id_display} with ID: {run['id']}") # Too noisy

    except Exception as e:
        return pd.DataFrame()

    # Fetch Actor results from the run's dataset
    data = []
    dataset_id = run["defaultDatasetId"]
    # print(f"Collecting comment data from dataset: {dataset_id} for post {post_id_display}...") # Too noisy

    try:
        for item in client.dataset(dataset_id).iterate_items():
            data.append(item)
        # print(f"Collected {len(data)} comments for post {post_id_display}") # Too noisy
    except Exception as e:
         pass


    df = pd.DataFrame(data)
    # print(f"Found {len(df)} comments for post {post_id_display}") # Too noisy
    return df

# Orchestrator function with threading and duplicate check
def ScrapePostsAndComments(
    client: ApifyClient,
    facebook_handle: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    path: Path,
    max_posts: int = 100,
    max_comments: int = 100,
    max_threads: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame] | tuple[None, None]:
    """
    Scrape posts and their comments for a specific Facebook handle.
    Uses threading for comment scraping and avoids re-scraping comments for posts already processed.

    Returns a tuple of (posts_df, comments_df) or (None, None) if post scraping fails.
    """
    print("-" * 60)
    print(f"--- Starting combined scrape process for {facebook_handle} ---")
    print(f"Posts between {start_time.strftime('%Y-%m-%d')} and {end_time.strftime('%Y-%m-%d')}")
    print("-" * 60)

    # --- 1. Load existing data to identify already scraped items ---
    # We primarily need the list of post_urls for which comments have already been saved
    existing_comments_df = load_existing_comments(path, facebook_handle)
    # Ensure 'post_url' exists before trying to get unique values
    existing_comment_post_urls = set(existing_comments_df['post_url'].unique() if not existing_comments_df.empty and 'post_url' in existing_comments_df.columns else [])
    print(f"Identified {len(existing_comment_post_urls)} posts with existing comments data.")

    # --- 2. Scrape Posts ---
    # Get posts from the specified date range in this run
    posts_df_this_run = ScrapePosts( # Renamed variable to be clear this is posts from *this* scrape
        client=client,
        facebook_handle=facebook_handle,
        start_time=start_time,
        end_time=end_time,
        path=path,
        max_posts=max_posts
    )

    # Check if post scraping failed or returned no posts
    if posts_df_this_run is None:
        print("Post scraping failed. Aborting comment scraping.")
        return None, None # Indicate failure for both

    if posts_df_this_run.empty or "url" not in posts_df_this_run.columns:
        print("No posts scraped in this run or 'url' column missing. No new comments to scrape.")
        # Load existing comments just in case
        final_comments_df = load_existing_comments(path, facebook_handle)
        # Return the scraped posts_df (even if empty) and the loaded comments_df
        return posts_df_this_run, final_comments_df # Return the posts df from this run


    # --- 3. Determine which posts need comments scraped ---
    # Ensure 'url' column exists before accessing it
    if 'url' not in posts_df_this_run.columns:
         print("Error: 'url' column missing in newly scraped posts DataFrame. Cannot proceed with comment scraping.")
         final_comments_df = load_existing_comments(path, facebook_handle) # Load existing comments as fallback
         return posts_df_this_run, final_comments_df # Return scraped posts (which is missing 'url') and loaded comments

    all_post_urls_from_scrape = posts_df_this_run['url'].tolist()

    # Filter out post URLs for which we already have comments data
    post_urls_to_scrape_comments = [
        url for url in all_post_urls_from_scrape
        if url and url not in existing_comment_post_urls # Ensure url is not None/empty
    ]

    skipped_post_count = len(all_post_urls_from_scrape) - len(post_urls_to_scrape_comments)
    print(f"Total posts found in this scrape run: {len(all_post_urls_from_scrape)}")
    if skipped_post_count > 0:
        print(f"Skipping comment scraping for {skipped_post_count} posts as comments already exist.")
    print(f"Proceeding to scrape comments for {len(post_urls_to_scrape_comments)} posts.")

    newly_scraped_comments_list = []

    # --- 4. Scrape Comments using Threading ---
    if post_urls_to_scrape_comments:
        print(f"Starting comment scraping using {max_threads} threads...")

        # Use ThreadPoolExecutor for concurrent comment scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Create a dictionary to map future objects to post URLs for easier tracking and error reporting
            future_to_url = {
                executor.submit(ScrapePostComments, client, post_url, max_comments): post_url
                for post_url in post_urls_to_scrape_comments
            }

            # Use tqdm with as_completed for progress tracking
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_url),
                                total=len(future_to_url),
                                desc=f"Scraping comments for {facebook_handle}'s posts",
                                unit="post",
                                # Leave the progress bar after completion
                                leave=True)

            for future in progress_bar:
                post_url = future_to_url[future]
                try:
                    comments_df = future.result() # This retrieves the return value (DataFrame) or raises exception
                    if not comments_df.empty:
                        # --- START MODIFICATION: Add post_url and post_text ---
                        # Add metadata to comments before appending
                        comments_df['post_url'] = post_url
                        comments_df['Author Handle'] = facebook_handle

                        # Find the corresponding post text from the posts_df_this_run
                        # Using .query() can sometimes be cleaner
                        post_row = posts_df_this_run.query('url == @post_url')
                        # Alternatively: post_row = posts_df_this_run[posts_df_this_run['url'] == post_url]


                        # Check if the post was found and has a 'text' column
                        if not post_row.empty and 'text' in post_row.columns:
                            # Extract the text (handle potential missing text for the post)
                            # Use .iat[0, column_index] or .iloc[0]['text'] for robust access
                            post_text_value = post_row['text'].iloc[0]
                            # Add the post text to all comments for this post
                            comments_df['post_text'] = post_text_value
                        else:
                             # If post text isn't found or column is missing, add a placeholder
                             # print(f"Warning: Could not find post text for URL: {post_url} in posts_df_this_run or 'text' column missing. Adding None.") # Too noisy
                             comments_df['post_text'] = None # or pd.NA or ''

                        # --- END MODIFICATION ---

                        newly_scraped_comments_list.append(comments_df)

                except Exception as exc:
                     pass 

            # Ensure progress bar completes
            progress_bar.close()

        # --- 5. Combine newly scraped comments ---
        newly_scraped_comments_df = pd.DataFrame() # Initialize as empty DataFrame
        if newly_scraped_comments_list:
             newly_scraped_comments_df = pd.concat(newly_scraped_comments_list, ignore_index=True)
             print(f"Successfully scraped comments for {len(newly_scraped_comments_list)} posts in this run.")
             print(f"Collected {len(newly_scraped_comments_df)} new comments in this run.")

             # Ensure 'post_text' column exists even if no text was found for any post
             if 'post_text' not in newly_scraped_comments_df.columns:
                 newly_scraped_comments_df['post_text'] = None


        else:
            print("No new comments were successfully scraped for the selected posts.")


        if not existing_comments_df.empty:
            # Combine existing and new comments
            # Ensure consistent columns before concat, adding 'post_text' to existing if missing (less likely)
            if 'post_text' not in existing_comments_df.columns:
                 print("Warning: 'post_text' column missing in existing comments data. Adding it with None.")
                 existing_comments_df['post_text'] = None

            # Harmonize columns before concat - this is important if new and old scraped data had different columns
            all_columns = list(set(existing_comments_df.columns) | set(newly_scraped_comments_df.columns))
            existing_comments_df = existing_comments_df.reindex(columns=all_columns)
            newly_scraped_comments_df = newly_scraped_comments_df.reindex(columns=all_columns)


            combined_comments_df = pd.concat([existing_comments_df, newly_scraped_comments_df], ignore_index=True)
            print(f"Combined new comments with {len(existing_comments_df)} existing comments.")

            # Drop duplicates in the final combined set if possible (based on a unique comment ID if the actor provides one)
            # Assuming 'id' is a unique comment ID from the actor
            if 'id' in combined_comments_df.columns:
                 initial_count = len(combined_comments_df)
                 subset_cols = ['id']

                 combined_comments_df.drop_duplicates(subset=subset_cols, inplace=True)
                 if len(combined_comments_df) < initial_count:
                      print(f"Removed {initial_count - len(combined_comments_df)} duplicate comments based on {subset_cols} in the combined dataset.")
            else:
                 print("Warning: 'id' column not found in comments for robust duplicate checking. Only avoiding re-scraping posts.")

        else:
            # No existing comments, the combined dataframe is just the new ones
            combined_comments_df = newly_scraped_comments_df
            print("No existing comments found. Saving only newly scraped comments.")


        # Save the combined comments data to the single cumulative file
        comments_dir = path / "comments"
        comments_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists
        output_filename = comments_dir / f"{facebook_handle}_facebook_comments_combined.xlsx"

        print(f"Saving combined comments data ({len(combined_comments_df)} total unique comments) to {output_filename}...")
        try:
            combined_comments_df.to_excel(output_filename, index=False)
            print("Combined comments data saved successfully.")
            final_comments_df = combined_comments_df
        except Exception as e:
            print(f"Error saving combined comments data to {output_filename}: {e}")
            # If saving fails, return the combined df before saving
            final_comments_df = combined_comments_df


    else: # This else corresponds to `if post_urls_to_scrape_comments:` being empty
        print("No posts required comment scraping based on previous runs.")
        # If no posts needed scraping, the final comments dataframe is just the existing one
        final_comments_df = existing_comments_df
        print(f"Loaded existing comments dataframe has {len(final_comments_df)} rows.")

    return posts_df_this_run, final_comments_df
