from src.backend.producers.file_producer import FileProducer
from src.backend.consumers.advanced_analysis_consumer import AdvancedAnalysisConsumer
import threading
import sys
import time

def run_producer():
    print("Starting producer thread...", file=sys.stderr)
    try:
        producer = FileProducer()
        producer.produce_tweets(mode='sequential')
    except Exception as e:
        print(f"Error in producer thread: {str(e)}", file=sys.stderr)
        raise

def run_consumer():
    print("Starting consumer thread...", file=sys.stderr)
    try:
        print("Waiting for producer to initialize...", file=sys.stderr)
        time.sleep(5)
        
        print("Creating consumer...", file=sys.stderr)
        consumer = AdvancedAnalysisConsumer()
        print("Starting tweet processing...", file=sys.stderr)
        consumer.process_tweets()
    except Exception as e:
        print(f"Error in consumer thread: {str(e)}", file=sys.stderr)
        raise

if __name__ == "__main__":
    print("Starting main application...", file=sys.stderr)
    
    producer_thread = threading.Thread(target=run_producer, name="ProducerThread")
    consumer_thread = threading.Thread(target=run_consumer, name="ConsumerThread")
    
    print("Starting threads...", file=sys.stderr)
    producer_thread.start()
    print("Producer thread started", file=sys.stderr)
    consumer_thread.start()
    print("Consumer thread started", file=sys.stderr)
    
    print("Waiting for threads to complete...", file=sys.stderr)
    
    while producer_thread.is_alive() or consumer_thread.is_alive():
        print(f"Thread status - Producer: {'alive' if producer_thread.is_alive() else 'dead'}, Consumer: {'alive' if consumer_thread.is_alive() else 'dead'}", file=sys.stderr)
        time.sleep(5)
    
    producer_thread.join()
    consumer_thread.join()
    
    print("Application finished.", file=sys.stderr) 