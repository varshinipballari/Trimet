import os
import json
import logging
import time
from datetime import datetime
from typing import List
from threading import Lock
from io import StringIO
import psycopg2
import pandas as pd
from google.cloud import pubsub_v1
from Gtransformation import Transformation, TripInfoBuilder
from dotenv import load_dotenv

# Configuration
load_dotenv(".env")
google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
project_id = os.getenv("GCP_PROJECT_ID")
subscription_id = os.getenv("GCP_SUBSCRIPTION_ID")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD")
}

class DatabaseHandler:
    @staticmethod
    def save_data(validated_df: pd.DataFrame, trip_metadata_df: pd.DataFrame):
        """Save transformed data to PostgreSQL using bulk inserts"""
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()

            # Prepare breadcrumb data
            validated_df["tstamp"] = validated_df["timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
            breadcrumb_data = validated_df[["tstamp", "latitude", "longitude", "speed", "trip_id"]]

            # Prepare trip metadata for bulk insert
            trip_metadata_df = trip_metadata_df[["trip_id", "route_id", "vehicle_id", "service_key", "direction"]]

            if not trip_metadata_df.empty:
                trip_buffer = StringIO()
                trip_metadata_df.to_csv(trip_buffer, index=False, header=False, sep='\t')
                trip_buffer.seek(0)

           
            # Bulk insert breadcrumb data
            if not breadcrumb_data.empty:
                breadcrumb_buffer = StringIO()
                breadcrumb_data.to_csv(breadcrumb_buffer, index=False, header=False, sep='\t')
                breadcrumb_buffer.seek(0)

                cursor.copy_from(breadcrumb_buffer, 'breadcrumb', null="", columns=(
                    'tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))

            connection.commit()
            return len(breadcrumb_data)
        except Exception as e:
            logging.error(f"Database error: {e}")
            raise
        finally:
            if connection:
                cursor.close()
                connection.close()


class PubSubProcessor:
    def __init__(self):
        self.transformer = Transformation()
        self.metadata_extractor = TripInfoBuilder()
        self.buffer = []
        self.buffer_lock = Lock()
        self.total_received = 0
        self.total_processed = 0
        self.last_message_time = datetime.now()

    def process_message(self, message):
        """Process individual Pub/Sub message"""
        try:
            data = json.loads(message.data.decode("utf-8"))
            with self.buffer_lock:
                self.buffer.append(data)
                self.total_received += 1
                self.last_message_time = datetime.now()
            message.ack()
        except Exception as e:
            logging.error(f"Message processing failed: {e}")
            message.nack()

    def process_batch(self):
        """Process a batch of messages"""
        with self.buffer_lock:
            if not self.buffer:
                return
            batch = self.buffer.copy()
            self.buffer.clear()

        try:
            # Transform data
            validated_df = self.transformer.process(batch)
            trip_metadata = self.metadata_extractor.build_summary(batch)
            
            # Save to database
            if not validated_df.empty:
                count = DatabaseHandler.save_data(validated_df, trip_metadata)
                self.total_processed += count
                logging.info(f"Processed batch of {count} records")
        except Exception as e:
            logging.error(f"Batch processing failed: {e}")


def callback(message,processor, BATCH_SIZE):
        processor.process_message(message)
        if len(processor.buffer) >= BATCH_SIZE:
            processor.process_batch()

def main():
    # Initialize logging
    logging.basicConfig(
        filename="batched_pipeline.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

   

    # Pub/Sub setup
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    processor = PubSubProcessor()
    BATCH_SIZE = 1000
    IDLE_TIMEOUT = 60  # seconds



    try:
        streaming_pull = subscriber.subscribe(subscription_path, callback=lambda msg: callback(msg, processor, BATCH_SIZE))
        logging.info("Subscriber started")

        while True:
            time.sleep(5)
            # Process any remaining messages in buffer
            processor.process_batch()
            
            # Check for idle timeout
            idle_time = (datetime.now() - processor.last_message_time).total_seconds()
            if idle_time > IDLE_TIMEOUT:
                logging.info(f"No messages for {IDLE_TIMEOUT} seconds. Exiting...")
                break

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        streaming_pull.cancel()
        subscriber.close()
        logging.info(f"Total received: {processor.total_received}")
        logging.info(f"Total processed: {processor.total_processed}")

if __name__ == "__main__":
    main()
