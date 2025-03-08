import pandas as pd
import numpy as np
from datetime import datetime
import random
from dateutil import parser
import pytz

tzinfos = {"PDT": pytz.timezone("America/Los_Angeles")}

print("Loading dataset...")
df = pd.read_csv('../data/tweets.csv', encoding='latin-1', header=None,
                 names=['sentiment', 'id', 'date', 'flag', 'user', 'text'])

print("Processing dataset...")
df['sentiment'] = df['sentiment'].map({0: 'NEGATIVE', 4: 'POSITIVE'})

def safe_parse_date(date_str):
    try:
        return parser.parse(date_str, tzinfos=tzinfos)
    except:
        return pd.Timestamp.now()

df['created_at'] = df['date'].apply(safe_parse_date)
df['retweet_count'] = np.random.randint(0, 10000, size=len(df))
df['favorite_count'] = np.random.randint(0, 20000, size=len(df))

df_processed = df[[
    'id', 'text', 'created_at', 'user', 
    'retweet_count', 'favorite_count', 'sentiment'
]]

print("Saving processed dataset...")
df_processed.to_csv('../data/processed_tweets.csv', index=False)
print(f"Created dataset with {len(df_processed)} tweets")
print(df_processed.head()) 