import pandas as pd
import datetime
from tqdm import tqdm
from pathlib import Path

APIFY_ACTOR_ID = "shu8hvrXbJbY3Eb9W"

def ScrapePosts(client, url, end_time:datetime.datetime, path : Path, max_posts=100):
    end_time = end_time.strftime("%Y-%m-%d")
    payload = {
        "addParentData": False,
        "directUrls": [
            url,
        ],
        "enhanceUserSearchWithFacebookPage": False,
        "isUserReelFeedURL": False,
        "isUserTaggedFeedURL": False,
        "onlyPostsNewerThan": end_time,
        "resultsLimit": max_posts,
        "resultsType": "posts",
        "searchLimit": 1,
        "searchType": "hashtag"
    }
    
    print(f"Scraping posts from {url}...")
    # Run the Actor and wait for it to finish
    run = client.actor(APIFY_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    print(f"Collecting post data...")
    for item in tqdm(client.dataset(run["defaultDatasetId"]).iterate_items(), desc="Processing posts", unit="post"):
        data.append(item)
    
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    
    save_to_path = path / "posts" / f"{url.split('/')[-1]}_instagram_{end_time}.xlsx"
    print(f"Saving data to {save_to_path}...")
    df.to_excel(save_to_path, index=False)
    
    return df
 
def ScrapePostComments(client, post_url, max_comments=100) -> pd.DataFrame:
    
    post_id = post_url.split('/')[-2] if '/' in post_url else post_url
    print(f"Scraping comments for post: {post_id}")
    
    payload = {
        "addParentData": False,
        "directUrls": [
            post_url,
        ],
        "enhanceUserSearchWithFacebookPage": False,
        "isUserReelFeedURL": False,
        "isUserTaggedFeedURL": False,
        "resultsLimit": max_comments,
        "resultsType": "comments",
        "searchLimit": 1,
        "searchType": "hashtag"
    }
    
    run = client.actor(APIFY_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    for item in tqdm(client.dataset(run["defaultDatasetId"]).iterate_items(), desc=f"Processing comments for post {post_id}", unit="comment"):
        data.append(item)
    
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    
    df['post_url'] = post_url
    print(f"Found {len(df)} comments for post {post_id}")
    
    return df

def ScrapeUserComentsAndPosts(client, username, end_time:datetime.datetime, path : Path, max_posts=100, max_comments=100):
    url = f"https://www.instagram.com/{username}/"
    
    print(f"Starting scrape for Instagram user: {username}")
    
    scraped_posts_df = ScrapePosts(client, url, end_time, path, max_posts)
    
    all_comments_df = pd.DataFrame()
    
    if "url" in scraped_posts_df.columns and len(scraped_posts_df) > 0:
        # Filter out posts without URLs
        post_urls = scraped_posts_df['url'].values
        
        print(f"Found {len(post_urls)} posts to process for comments")
        
        for post_url in tqdm(post_urls, desc=f"Scraping comments for {username}'s posts", unit="post"):
            comments_df = ScrapePostComments(client, post_url, max_comments)
            if not comments_df.empty:
                all_comments_df = pd.concat([all_comments_df, comments_df], ignore_index=True)
        
        all_comments_df['Author Handle'] = username
        
        output_filename = path / "comments" / f"{username}_instagram_{end_time.strftime('%Y-%m-%d')}.xlsx"
        print(f"Saving data to {output_filename}...")
        all_comments_df.to_excel(output_filename, index=False)
        print(f"Completed! Scraped {len(scraped_posts_df)} posts and {len(all_comments_df)} comments for user {username}.")
        
        return all_comments_df
    
    else:
        print(f"No posts found for user {username}.")
        return pd.DataFrame()