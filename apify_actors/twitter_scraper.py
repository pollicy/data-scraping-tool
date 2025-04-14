import pandas as pd
import datetime
from tqdm import tqdm
from pathlib import Path

POSTS_ACTOR_ID = "nfp1fpt5gUlBwPcor"
COMMENTS_ACTOR_ID = "qhybbvlFivx7AP0Oh"

def ScrapePosts(client, username, end_time:datetime.datetime, max_posts=100, start_time=datetime.datetime.now()):
    start_time = start_time.strftime("%Y-%m-%d")
    end_time = end_time.strftime("%Y-%m-%d")
    payload = {
            "end": end_time,
            "maxItems": max_posts,
            "sort": "Latest",
            "start": start_time,
            "twitterHandles": [
                f"{username}"
            ]
        }
        
    # Run the Actor and wait for it to finish
    print(f"Scraping posts for {username}...")
    run = client.actor(POSTS_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    for item in tqdm(client.dataset(run["defaultDatasetId"]).iterate_items(), desc="Collecting posts"):
        data.append(item)
    
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
        
    df.to_excel(f"{username}_{start_time}_to_{end_time}_posts.xlsx", index=False)
        
    return df
 
def ScrapeComments(client, post_url, max_comments=100):
    payload = {
        "postUrls": [
            f"{post_url}"
        ],
        "resultsLimit": max_comments,
        }
        
    # Run the Actor and wait for it to finish
    run = client.actor(COMMENTS_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        data.append(item)
    
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
        
    df.to_excel(f"{post_url.split('/')[-1]}_comments.xlsx", index=False)
        
    return df
         
def ScrapePostsAndComments(client, username, end_time:datetime.datetime, path : Path, max_posts=100, max_comments=100):
    scraped_posts_df = ScrapePosts(client, username, end_time, max_posts)
        
    all_comments_df = pd.DataFrame()
        
    if "twitterUrl" in scraped_posts_df.columns and len(scraped_posts_df) > 0:
                
        for post_url, post in tqdm(zip(scraped_posts_df['twitterUrl'], scraped_posts_df['text']), 
                                 total=len(scraped_posts_df), 
                                 desc=f"Scraping comments for {username}'s posts"):
            # Scrape comments for each post URL
            comments_df = ScrapeComments(client, post_url, max_comments)
            comments_df['tweet'] = [post] * len(comments_df)
            all_comments_df = pd.concat([all_comments_df, comments_df], ignore_index=True)
                    
        all_comments_df['twitter username'] = [username] * len(all_comments_df)
                
        output_filename = path / "comments" / f"{username}_instagram_{end_time.strftime('%Y-%m-%d')}.xlsx"
        print(f"Saving data to {output_filename}...")
        all_comments_df.to_excel(output_filename, index=False)
        print(f"Completed! Scraped {len(scraped_posts_df)} posts and {len(all_comments_df)} comments for user {username}.")
                
        return all_comments_df
        
    else:
        print(f"No posts found for user {username}.")
        return pd.DataFrame()