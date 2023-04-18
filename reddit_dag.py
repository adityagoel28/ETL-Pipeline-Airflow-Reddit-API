from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
# from reddit_etl import reddit_extract
import requests
from decouple import config
from dotenv import load_dotenv
import os
import pandas as pd

# load_dotenv()
# my_variable_value = os.environ.get('SECRET_TOKEN')

SECRET_TOKEN = config("SECRET_TOKEN")
CLIENT_ID = config("CLIENT_ID") 
username = config("USERNAME")
password = config("PASSWORD")

def df_from_response(data):

    df = pd.DataFrame()

    for post in data:
        df = df._append({
            'subreddit': post['data']['subreddit'],
            'title': post['data']['title'],
            'selftext': post['data']['selftext'],
            'upvote_ratio': post['data']['upvote_ratio'],
            'ups': post['data']['ups'],
            'downs': post['data']['downs'],
            'score': post['data']['score'],
            'created_utc': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'id': post['data']['id'],
            'kind': post['kind']
        }, ignore_index = True)
        
    print(df)
    df.to_csv('data.csv')
    
    return df

# authenticate API
auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_TOKEN)

data = {'grant_type': 'password',
        'username': username,
        'password': password}

# setup our header info, which gives reddit a brief description of our app
headers = {'User-Agent': 'ETL_Airflow/0.0.1'}

# send authentication request
res = requests.post('https://www.reddit.com/api/v1/access_token', auth = auth, data = data, headers = headers)
TOKEN = res.json()['access_token']

# add authorization to our headers dictionary by updating API headers with authorization (bearer token)
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

reddit_data = pd.DataFrame()
params = {'limit': 100}

for i in range(10):
    res = requests.get("https://oauth.reddit.com/r/python/new", headers = headers, params = params)
    
    data = res.json()['data']['children']
    new_df = df_from_response(data)
    # take the final row (oldest entry)
    row = new_df.iloc[len(new_df)-1]
    # create fullname
    fullname = row['kind'] + '_' + row['id']
    # add/update fullname in params
    params['after'] = fullname
    
    # append new_df to reddit_data
    reddit_data = reddit_data._append(new_df, ignore_index = True)


reddit_data.to_csv('reddit_data.csv')
print(reddit_data)