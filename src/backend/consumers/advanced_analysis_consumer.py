import json
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
import pandas as pd
import numpy as np
from collections import defaultdict
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from textblob import TextBlob
import re
from sklearn.feature_extraction.text import CountVectorizer
from datetime import datetime, timedelta
import sys

class AdvancedAnalysisConsumer:
    def __init__(self, bootstrap_servers=['kafka1:9092', 'kafka2:9093', 'kafka3:9094']):
        print("Initializing AdvancedAnalysisConsumer...", file=sys.stderr)
        
        print("Creating Kafka consumer...", file=sys.stderr)
        self.consumer = KafkaConsumer(
            'raw_tweets',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='advanced_analysis_group',
            auto_offset_reset='earliest'
        )
        print("Kafka consumer created successfully", file=sys.stderr)
        
        print("Creating Kafka producer...", file=sys.stderr)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Kafka producer created successfully", file=sys.stderr)
        
        print("Loading emotion analyzer model...", file=sys.stderr)
        self.emotion_analyzer = pipeline('text-classification', 
                                      model='bhadresh-savani/distilbert-base-uncased-emotion', 
                                      return_all_scores=True)
        print("Emotion analyzer model loaded successfully", file=sys.stderr)
        
        self.trending_topics = defaultdict(int)
        self.topic_vectorizer = CountVectorizer(stop_words='english', 
                                              ngram_range=(1, 2),
                                              max_features=1000)
        self.recent_tweets = []
        self.last_trends_update = datetime.now()
        print("AdvancedAnalysisConsumer initialized successfully", file=sys.stderr)
        
    def clean_text(self, text):
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\@\w+|\#\w+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
        
    def analyze_sentiment(self, text):
        analysis = TextBlob(text)
        polarity = analysis.sentiment.polarity
        
        emotions = self.emotion_analyzer(text)[0]
        emotion_scores = {item['label']: item['score'] for item in emotions}
        dominant_emotion = max(emotion_scores.items(), key=lambda x: x[1])
        
        if polarity > 0 or (dominant_emotion[0] in ['joy', 'love'] and dominant_emotion[1] > 0.5):
            sentiment = 'POSITIVE'
        else:
            sentiment = 'NEGATIVE'
        
        print(f"Text: '{text[:100]}...'", file=sys.stderr)
        print(f"TextBlob polarity: {polarity}", file=sys.stderr)
        print(f"Dominant emotion: {dominant_emotion[0]} ({dominant_emotion[1]:.3f})", file=sys.stderr)
        print(f"Final sentiment: {sentiment}", file=sys.stderr)
        return sentiment
        
    def analyze_emotions(self, text):
        emotions = self.emotion_analyzer(text)[0]
        return {item['label']: item['score'] for item in emotions}
    
    def analyze_subjectivity(self, text):
        analysis = TextBlob(text)
        return {
            'subjectivity': analysis.sentiment.subjectivity,
            'polarity': analysis.sentiment.polarity
        }
    
    def update_trends(self, text):
        self.recent_tweets.append(text)
        
        if (datetime.now() - self.last_trends_update) > timedelta(minutes=5):
            if len(self.recent_tweets) > 100: 
                print(f"Updating trends with {len(self.recent_tweets)} tweets...", file=sys.stderr)
                X = self.topic_vectorizer.fit_transform(self.recent_tweets)
                features = self.topic_vectorizer.get_feature_names_out()
                frequencies = np.asarray(X.sum(axis=0)).ravel()
                
                self.trending_topics.clear()
                for topic, freq in zip(features, frequencies):
                    if freq > 3: 
                        self.trending_topics[topic] = int(freq)
                
                print(f"Updated {len(self.trending_topics)} trending topics", file=sys.stderr)
                self.recent_tweets = []
                self.last_trends_update = datetime.now()
    
    def process_tweets(self):
        print("Starting tweet processing...", file=sys.stderr)
        processed_count = 0
        
        try:
            for message in self.consumer:
                tweet = message.value
                print(f"\nProcessing tweet {tweet.get('id', 'unknown')}...", file=sys.stderr)
                clean_text = self.clean_text(tweet['text'])
                
                try:
                    print("Analyzing sentiment...", file=sys.stderr)
                    sentiment = self.analyze_sentiment(clean_text)
                    print("Analyzing emotions...", file=sys.stderr)
                    emotions = self.analyze_emotions(clean_text)
                    print("Analyzing subjectivity...", file=sys.stderr)
                    subjectivity = self.analyze_subjectivity(clean_text)
                    
                    print("Updating trends...", file=sys.stderr)
                    self.update_trends(clean_text)
                    
                    enriched_tweet = {
                        **tweet,
                        'sentiment': sentiment,
                        'emotions': emotions,
                        'subjectivity': subjectivity['subjectivity'],
                        'polarity': subjectivity['polarity'],
                        'trending_topics': dict(self.trending_topics)
                    }
                    
                    print("Sending enriched tweet...", file=sys.stderr)
                    self.producer.send('analyzed_tweets', value=enriched_tweet)
                    processed_count += 1
                    print(f"Processed tweet {tweet['id']} with sentiment: {sentiment}, emotions: {max(emotions.items(), key=lambda x: x[1])[0]}", file=sys.stderr)
                    print(f"Total processed tweets: {processed_count}", file=sys.stderr)
                    
                except Exception as e:
                    print(f"Error processing tweet {tweet['id']}: {str(e)}", file=sys.stderr)
                    continue
                    
        except Exception as e:
            print(f"Fatal error in process_tweets: {str(e)}", file=sys.stderr)
            raise

if __name__ == "__main__":
    consumer = AdvancedAnalysisConsumer()
    consumer.process_tweets() 