import json
import time
import random
from kafka import KafkaProducer
import pandas as pd
from pathlib import Path
from ..utils.config_loader import get_broker_list, get_producer_config

class FileProducer:
    def __init__(self):
        print("Initializing FileProducer...")
        print(f"Getting broker list...")
        broker_list = get_broker_list()
        print(f"Broker list: {broker_list}")
        
        print(f"Getting producer config...")
        config = get_producer_config()
        print(f"Producer config: {config}")
        
        print("Creating Kafka producer...")
        self.producer = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            **config
        )
        print("Kafka producer created successfully")
        
        self.data_path = Path(__file__).parent.parent.parent.parent / 'data' / 'processed_tweets.csv'
        print(f"Data path set to: {self.data_path}")
        
    def produce_tweets(self, topic='raw_tweets', mode='sequential', delay_range=(1, 5)):
        print(f"Starting to produce tweets in {mode} mode to topic {topic}")
        print(f"Loading tweets from {self.data_path}")
        
        if not self.data_path.exists():
            print(f"ERROR: Data file not found at {self.data_path}")
            return
            
        df = pd.read_csv(self.data_path)
        print(f"Loaded {len(df)} tweets")
        
        tweet_count = 0
        while True:
            try:
                if mode == 'random':
                    tweet = df.sample(1).iloc[0]
                else:
                    for _, tweet in df.iterrows():
                        message = {
                            'id': str(tweet['id']),
                            'text': tweet['text'],
                            'created_at': tweet['created_at'],
                            'user': tweet['user'],
                            'retweet_count': int(tweet['retweet_count']),
                            'favorite_count': int(tweet['favorite_count']),
                            'sentiment': tweet['sentiment']
                        }
                        self.producer.send(topic, value=message)
                        tweet_count += 1
                        if tweet_count % 100 == 0:
                            print(f"Sent {tweet_count} tweets")
                        time.sleep(random.uniform(*delay_range))
                    
                    print("Finished sending all tweets, starting over...")
                    continue
                
                message = {
                    'id': str(tweet['id']),
                    'text': tweet['text'],
                    'created_at': tweet['created_at'],
                    'user': tweet['user'],
                    'retweet_count': int(tweet['retweet_count']),
                    'favorite_count': int(tweet['favorite_count']),
                    'sentiment': tweet['sentiment']
                }
                self.producer.send(topic, value=message)
                tweet_count += 1
                if tweet_count % 100 == 0:
                    print(f"Sent {tweet_count} tweets")
                time.sleep(random.uniform(*delay_range))
                
            except Exception as e:
                print(f"Error producing tweets: {str(e)}")
                time.sleep(5)

if __name__ == "__main__":
    producer = FileProducer()
    producer.produce_tweets(mode='sequential') 