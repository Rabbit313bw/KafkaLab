import streamlit as st
import json
from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from collections import deque
import time
import sys
import uuid

def wait_for_topic(consumer, topic_name, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        topics = consumer.topics()
        if topic_name in topics:
            return True
        print(f"Waiting for topic '{topic_name}' to be created... Available topics: {topics}", file=sys.stderr)
        time.sleep(2)
    return False

def ensure_topic_exists(brokers, topic_name):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=brokers,
            client_id='frontend_admin'
        )
        
        topic = NewTopic(
            name=topic_name,
            num_partitions=3,
            replication_factor=2
        )
        admin_client.create_topics([topic])
        print(f"Created topic: {topic_name}", file=sys.stderr)
    except TopicAlreadyExistsError:
        print(f"Topic {topic_name} already exists", file=sys.stderr)
    except Exception as e:
        print(f"Error creating topic: {str(e)}", file=sys.stderr)
    finally:
        admin_client.close()

if 'instance_id' not in st.session_state:
    st.session_state.instance_id = str(uuid.uuid4())
    print(f"Generated instance ID: {st.session_state.instance_id}", file=sys.stderr)

if 'initialized' not in st.session_state:
    print("Initializing session state...", file=sys.stderr)
    st.session_state.initialized = True
    st.session_state.tweets_data = deque(maxlen=1000)
    st.session_state.sentiment_counts = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
    st.session_state.emotion_counts = {
        'joy': 0, 'sadness': 0, 'anger': 0, 
        'fear': 0, 'surprise': 0, 'love': 0
    }
    st.session_state.trending_topics = {}
    st.session_state.error = None
    st.session_state.consumer = None
    st.session_state.connection_attempts = 0
    st.session_state.last_poll_time = None
    st.session_state.total_messages = 0

st.title('Twitter Advanced Analysis Dashboard')

tab1, tab2, tab3, tab4 = st.tabs(['Основные метрики', 'Эмоции', 'Тренды', 'Твиты'])

st.sidebar.write('Статус подключения:')

try:
    if st.session_state.consumer is None:
        st.sidebar.info('Подключение к Kafka...')
        print(f"Attempt #{st.session_state.connection_attempts + 1} to connect to Kafka...", file=sys.stderr)
        st.session_state.connection_attempts += 1
        
        brokers = ['kafka1:9092', 'kafka2:9093', 'kafka3:9094']
        print(f"Using brokers: {brokers}", file=sys.stderr)
        
        ensure_topic_exists(brokers, 'analyzed_tweets')
        
        group_id = f'visualization_group_{st.session_state.instance_id}'
        print(f"Using consumer group: {group_id}", file=sys.stderr)
        
        st.session_state.consumer = KafkaConsumer(
            bootstrap_servers=brokers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=group_id,
            enable_auto_commit=True,
            auto_offset_reset='earliest',  
            auto_commit_interval_ms=1000,
            consumer_timeout_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
            max_poll_records=100
        )
        
        if not wait_for_topic(st.session_state.consumer, 'analyzed_tweets'):
            raise Exception("Timeout waiting for topic 'analyzed_tweets' to be created")
        
        partitions = st.session_state.consumer.partitions_for_topic('analyzed_tweets')
        if not partitions:
            raise Exception("No partitions found for topic 'analyzed_tweets'")
        
        topic_partitions = [TopicPartition('analyzed_tweets', p) for p in partitions]
        st.session_state.consumer.assign(topic_partitions)
        
        for tp in topic_partitions:
            st.session_state.consumer.seek_to_beginning(tp)
        
        print("Successfully connected to Kafka", file=sys.stderr)
        st.session_state.last_poll_time = time.time()

    st.sidebar.success('Connect Kafka')
    
    messages = st.session_state.consumer.poll(timeout_ms=1000)
    st.session_state.last_poll_time = time.time()

    messages_count = sum(len(partition_messages) for partition_messages in messages.values())
    print(f"Received {messages_count} messages", file=sys.stderr)
    st.session_state.total_messages += messages_count
    
    time_since_last_poll = time.time() - st.session_state.last_poll_time if st.session_state.last_poll_time else 0
    
    if not messages:
        st.warning(f'''
        Waiting data...
        
        Status:
        - Trying...: {st.session_state.connection_attempts}
        - Overall messages: {st.session_state.total_messages}
        - Procceded tweets: {len(st.session_state.tweets_data)}
        - Time till last succeed getting : {time_since_last_poll:.2f}s
        ''')
    
    new_data = False
    for topic_partition, partition_messages in messages.items():
        for message in partition_messages:
            tweet = message.value
            print(f"Processing tweet: {tweet.get('id', 'unknown')}", file=sys.stderr)
            st.session_state.tweets_data.append(tweet)
            st.session_state.sentiment_counts[tweet['sentiment']] += 1
            
            if 'emotions' in tweet:
                dominant_emotion = max(tweet['emotions'].items(), key=lambda x: x[1])[0]
                st.session_state.emotion_counts[dominant_emotion] += 1
            
            if 'trending_topics' in tweet:
                st.session_state.trending_topics = tweet['trending_topics']
            new_data = True
    
    if new_data or len(st.session_state.tweets_data) > 0:
        with tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Positive Tweets", st.session_state.sentiment_counts['POSITIVE'])
            with col2:
                st.metric("Negative Tweets", st.session_state.sentiment_counts['NEGATIVE'])
            
            df = pd.DataFrame(list(st.session_state.tweets_data))
            if len(df) > 0:
                fig1 = px.pie(
                    values=[st.session_state.sentiment_counts[s] for s in ['POSITIVE', 'NEGATIVE', 'NEUTRAL']],
                    names=['Positive', 'Negative', 'Neutral'],
                    title='Distribution Sentiments'
                )
                st.plotly_chart(fig1)
                
                if 'subjectivity' in df.columns and 'polarity' in df.columns:
                    fig2 = px.scatter(
                        df,
                        x='subjectivity',
                        y='polarity',
                        color='sentiment',
                        title='Subjectivity vs Polarity',
                        hover_data=['text']
                    )
                    st.plotly_chart(fig2)
        
        with tab2:
            if len(df) > 0 and 'emotions' in df.columns:
                emotions_df = pd.DataFrame([
                    {'emotion': emotion, 'count': count}
                    for emotion, count in st.session_state.emotion_counts.items()
                ])
                fig3 = px.bar(
                    emotions_df,
                    x='emotion',
                    y='count',
                    title='Distribution emotions',
                    color='emotion'
                )
                st.plotly_chart(fig3)
        
        with tab3:
            if st.session_state.trending_topics:
                trends_df = pd.DataFrame([
                    {'topic': topic, 'frequency': freq}
                    for topic, freq in st.session_state.trending_topics.items()
                ]).sort_values('frequency', ascending=False)
                
                if len(trends_df) > 0:
                    fig5 = px.bar(
                        trends_df.head(20),
                        x='topic',
                        y='frequency',
                        title='Top 20 trends',
                        color='frequency'
                    )
                    fig5.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig5)
        
        with tab4:
            if len(df) > 0:
                st.dataframe(
                    df[['text', 'sentiment', 'subjectivity', 'polarity']]
                    .tail(10)
                    .sort_values('polarity', ascending=False)
                )

except Exception as e:
    error_msg = f'Error to connect Kafka: {str(e)}'
    print(error_msg, file=sys.stderr)
    st.error(error_msg)
    st.session_state.error = str(e)

st.sidebar.write('Status system:')
if st.session_state.error:
    st.sidebar.error(f'Error: {st.session_state.error}')
else:
    st.sidebar.success('System work stable')

st.sidebar.write('Statistics:')
st.sidebar.write(f'Procceded tweets: {len(st.session_state.tweets_data)}')
st.sidebar.write(f'Positive: {st.session_state.sentiment_counts["POSITIVE"]}')
st.sidebar.write(f'Negative: {st.session_state.sentiment_counts["NEGATIVE"]}')
st.sidebar.write(f'Trying connect...: {st.session_state.connection_attempts}')

st.empty()
time.sleep(1) 