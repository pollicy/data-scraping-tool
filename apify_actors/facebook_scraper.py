import pandas as pd
import datetime
from tqdm import tqdm
from pathlib import Path

POSTS_ACTOR_ID = "KoJrdxJCTtpon81KY"
COMMENTS_ACTOR_ID = "thDyWzaBBQxt4VOfW"

def ScrapePosts(client, url, end_time:datetime.datetime, path : Path, max_posts=100):
    payload = {
        "startUrls": [
            {
            "url": url,
            }
        ],
        "resultsLimit": max_posts,
        "onlyPostsNewerThan": end_time.strftime("%Y-%m-%d"),
    }
    
    print(f"Scraping posts from {url}...")
    try:
        run = client.actor(POSTS_ACTOR_ID).call(run_input=payload)
    
    except Exception as e:
        print(f"Invalid API key or Actor ID. Please check your credentials. Error: {e}")
        return None
    
    # Fetch Actor results from the run's dataset
    data = []
    print(f"Collecting post data...")
    for item in tqdm(client.dataset(run["defaultDatasetId"]).iterate_items(), desc="Processing posts", unit="post"):
        data.append(item)
        
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    
    save_to_path = path / "posts" / f"{url.split('/')[-1]}_facebook_{end_time.strftime('%Y-%m-%d')}.xlsx"
    print(f"Saving data to {save_to_path}...")
    df.to_excel(save_to_path, index=False)
    return df

def ScrapePostComments(client, post_url, max_comments=100) -> pd.DataFrame:
    
    payload = {
        "post_url": post_url,
        "count": max_comments,
    }
    
    # Show which post is being processed
    post_id = post_url.split("/")[-1] if "/" in post_url else post_url
    print(f"Scraping comments for post: {post_id}")
    
    run = client.actor(COMMENTS_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    for item in tqdm(client.dataset(run["defaultDatasetId"]).iterate_items(), desc="Processing comments", unit="comment"):
        data.append(item)
        
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    print(f"Found {len(df)} comments for post {post_id}")
    return df
 
def ScrapePostsAndComments(client, facebook_handle, end_time:datetime.datetime, path : Path, max_posts=100, max_comments=100) -> pd.DataFrame:
    
    url = f"https://www.facebook.com/{facebook_handle}"
    print(f"Starting scrape for Facebook handle: {facebook_handle}")
    
    posts_df = ScrapePosts(client, url, end_time, path, max_posts)
    
    if posts_df is None:
        print(f"Invalid API key or Actor ID. Please check your credentials.")
        return None
    
    all_comments_df = pd.DataFrame()
    
    if "url" in posts_df.columns and len(posts_df) > 0:
        post_urls = posts_df['url'].tolist()
        
        print(f"Found {len(post_urls)} posts to process for comments")
        
        for post_url in tqdm(post_urls, desc=f"Scraping comments for {facebook_handle}'s posts", unit="post"):
            comments_df = ScrapePostComments(client, post_url, max_comments)
            if not comments_df.empty:
                # Add post_url to each comment record
                comments_df['post_url'] = post_url
                all_comments_df = pd.concat([all_comments_df, comments_df], ignore_index=True)
            
        all_comments_df['Author Handle'] = facebook_handle
        
        output_filename = path / "comments" / f"{facebook_handle}_facebook_{end_time.strftime('%Y-%m-%d')}.xlsx"
        print(f"Saving comments data to {output_filename}...")
        all_comments_df.to_excel(output_filename, index=False)
        print(f"Completed! Scraped {len(posts_df)} posts and {len(all_comments_df)} comments for user {facebook_handle}.")
        
        return all_comments_df
    
    else:
        print(f"No posts found for user {facebook_handle} within the specified date range.")
        return pd.DataFrame()