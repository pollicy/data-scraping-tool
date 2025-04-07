import pandas as pd
import datetime

ACTOR_ID="61RPP7dywgiy0JPD0"

def ScrapePosts(client, username, end_time:datetime.datetime, max_posts=100, start_time=datetime.datetime.now()):
    start_time = start_time.strftime("%Y-%m-%d")
    end_time = end_time.strftime("%Y-%m-%d")
    payload = {
        "author": "apify",
        "customMapFunction": "(object) => { return {...object} }",
        "end": end_time,
        "maxItems": max_posts,
        "onlyImage": False,
        "onlyQuote": False,
        "onlyTwitterBlue": False,
        "onlyVerifiedUsers": False,
        "onlyVideo": False,
        "sort": "Latest",
        "start": start_time,
        "startUrls": [
            f"https://x.com/{username}"
        ],
        "tweetLanguage": "en",
    }
    
    # Run the Actor and wait for it to finish
    run = client.actor(ACTOR_ID).call(run_input=payload)

    # Fetch Actor results from the run's dataset
    data = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        data.append(item)

    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    
    df.to_excel(f"{username}_{start_time}_to_{end_time}_posts.xlsx", index=False)


def ScrapeComments(client, post_url, end_time:datetime.datetime, max_comments=100, start_time=datetime.datetime.now()):
    start_time = start_time.strftime("%Y-%m-%d")
    end_time = end_time.strftime("%Y-%m-%d")
    payload = {
        "author": "apify",
        "customMapFunction": "(object) => { return {...object} }",
        "end": end_time,
        "maxItems": max_comments,
        "onlyImage": False,
        "onlyQuote": False,
        "onlyTwitterBlue": False,
        "onlyVerifiedUsers": False,
        "onlyVideo": False,
        "sort": "Latest",
        "start": start_time,
        "startUrls": [
            post_url
        ],
        "tweetLanguage": "en",
    }
    
    # Run the Actor and wait for it to finish
    run = client.actor(ACTOR_ID).call(run_input=payload)

    # Fetch Actor results from the run's dataset
    data = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        data.append(item)

    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    
    df.to_excel(f"{post_url.split('/')[-1]}_{start_time}_to_{end_time}_comments.xlsx", index=False)