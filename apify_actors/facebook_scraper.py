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
    
    run = client.actor(POSTS_ACTOR_ID).call(run_input=payload)
    
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

def ScrapePostComments(client, post_url, end_time:datetime.datetime, max_comments=100) -> pd.DataFrame:
    
    payload = {
        "post_url": post_url,
        "count": max_comments,
    }
    
    run = client.actor(COMMENTS_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    print(f"Collecting post comments...")
    for item in tqdm(client.dataset(run["defaultDatasetId"]).iterate_items(), desc="Processing comments", unit="comment"):
        data.append(item)
        
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    return df


def ScrapePostsAndComments(client, facebook_handle, end_time:datetime.datetime, path : Path, max_posts=100, max_comments=100) -> pd.DataFrame:
    
    url = f"https://www.facebook.com/{facebook_handle}"
    posts_df = ScrapePosts(client, url, end_time, path, max_posts)
    
    all_comments_df = pd.DataFrame()
    
    if "url" in posts_df.columns and len(posts_df) > 0:
        post_urls = posts_df['url'].tolist()
        
        for post_url in tqdm(post_urls, desc="Processing posts", unit="post"):
            comments_df = ScrapePostComments(client, post_url, end_time, max_comments)
            all_comments_df = pd.concat([all_comments_df, comments_df], ignore_index=True)
            
        all_comments_df['post_url'] = [post_url] * len(all_comments_df)
        all_comments_df['Author Handle'] = [facebook_handle] * len(all_comments_df)
        
        output_filename = path / "comments" / f"{facebook_handle}_facebook_{end_time.strftime('%Y-%m-%d')}.xlsx"
        print(f"Saving data to {output_filename}...")
        all_comments_df.to_excel(output_filename, index=False)
        print(f"Completed! Scraped {len(posts_df)} posts and {len(all_comments_df)} comments for user {facebook_handle}.")
        
    else:
        print(f"No posts found for user {facebook_handle} within the specified date range.")