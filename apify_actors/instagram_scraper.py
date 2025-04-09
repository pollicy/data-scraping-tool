import pandas as pd
import datetime
from tqdm import tqdm

APIFY_ACTOR_ID = "shu8hvrXbJbY3Eb9W"

def ScrapePosts(client, url, end_time:datetime.datetime, max_posts=100):
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
    
    return df


def ScrapePostComments(client, post_url, end_time:datetime.datetime, max_comments=100) -> pd.DataFrame:
    
    payload = {
        "addParentData": False,
        "directUrls": [
            post_url,
        ],
        "enhanceUserSearchWithFacebookPage": False,
        "isUserReelFeedURL": False,
        "isUserTaggedFeedURL": False,
        "onlyPostsNewerThan": end_time.strftime("%Y-%m-%d"),
        "resultsLimit": max_comments,
        "resultsType": "comments",
        "searchLimit": 1,
        "searchType": "hashtag"
    }
    
    run = client.actor(APIFY_ACTOR_ID).call(run_input=payload)
    
    # Fetch Actor results from the run's dataset
    data = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        data.append(item)
        
    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    
    df['post_url'] = [post_url] * len(df)
    
    return df

def ScrapeUserComentsAndPosts(client, username, end_time:datetime.datetime, max_posts=100):
    url = f"https://www.instagram.com/{username}/"
    
    scraped_posts_df = ScrapePosts(client, url, end_time, max_posts)
    
    all_comments_df = pd.DataFrame()
    post_urls = scraped_posts_df['url'].values
    
    print(f"Scraping comments from {len(post_urls)} posts...")
    for post_url in tqdm(post_urls, desc="Processing comments", unit="post"):
        comments_df = ScrapePostComments(client, post_url, end_time, max_posts)
        all_comments_df = pd.concat([all_comments_df, comments_df], ignore_index=True)
        
    all_comments_df['Author Handle'] = [username] * len(all_comments_df)
    
    output_filename = f"{username}_instagram_{end_time.strftime('%Y-%m-%d')}.xlsx"
    print(f"Saving data to {output_filename}...")
    all_comments_df.to_excel(output_filename, index=False)
    print(f"Completed! Scraped {len(scraped_posts_df)} posts and {len(all_comments_df)} comments for user {username}.")