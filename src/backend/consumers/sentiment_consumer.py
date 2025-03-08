import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
from transformers import pipeline
import pandas as pd

class SentimentConsumer:
    def __init__(self, bootstrap_servers=['kafka1:9092', 'kafka2:9093', 'kafka3:9094']):
        self.consumer = KafkaConsumer(
            'raw_tweets',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='sentiment_analysis_group'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.sentiment_analyzer = pipeline('sentiment-analysis')
        
    def process_tweets(self):
        for message in self.consumer:
            tweet = message.value
            
            sentiment = self.sentiment_analyzer(tweet['text'])[0]
            
            enriched_tweet = {
                **tweet,
                'sentiment': sentiment['label'],
                'sentiment_score': sentiment['score']
            }
            
            self.producer.send('processed_tweets', value=enriched_tweet)
            print(f"Processed tweet {tweet['id']} with sentiment {sentiment['label']}")

if __name__ == "__main__":
    consumer = SentimentConsumer()
    consumer.process_tweets() 