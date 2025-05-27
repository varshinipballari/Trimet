import os
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from google.cloud import pubsub_v1
import psycopg2
import pandas as pd
from io import StringIO
from threading import Lock
from dotenv import load_dotenv



def setup_logging(log_file: str):
    """Configures the logging for the application."""
    logging.basicConfig(
        filename=log_file,
        filemode='w',
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_environment_variables() -> Tuple[str, str, str, Dict, int, int]:
    """Loads environment variables and returns necessary configurations."""
    load_dotenv(".env")
    google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
    project_id = os.getenv("GCP_PROJECT_ID")
    subscription_id = os.getenv("GCP_SUBSCRIPTION_ID")

    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }

    
    batch_size = int(os.getenv("BATCH_SIZE", 1000))
    idle_timeout = int(os.getenv("IDLE_TIMEOUT", 60))


    if not all([google_credentials_path, project_id, subscription_id, db_config["host"]]):
        logging.error("Missing one or more required environment variables.")
        raise ValueError("Missing required environment variables for configuration.")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path
    return project_id, subscription_id, google_credentials_path, db_config, batch_size, idle_timeout


metrics = {
    "total_received": 0,
    "total_loaded": 0,
    "total_skipped": 0,
    "total_invalid": 0,
    "last_message_time": datetime.now(),
    "loaded_trip_ids": set(),
    "message_buffer": [],
    "buffer_lock": Lock()
}



class StopEventCleaner:
    def __init__(self):
        
        self.processed_combinations_in_batch = set()

    def _is_valid_record(self, record: Dict) -> bool:
        """Performs basic validation on a single record."""
        required_fields = ["trip_id", "route_number", "vehicle_id", "service_key", "direction"]
        for field in required_fields:
            if field not in record or record[field] is None:
                logging.warning(f"Missing or null field '{field}' in record: {record}")
                return False

        try:
            trip_id = int(record.get("trip_id"))
            if trip_id <= 0:
                logging.warning(f"Invalid trip_id: {trip_id} in {record}")
                return False
        except ValueError:
            logging.warning(f"Invalid trip_id : {record.get('trip_id')} in {record}")
            return False

        try:
            int(record.get("vehicle_id"))
        except ValueError:
            logging.warning(f"Invalid vehicle_id : {record.get('vehicle_id')} in {record}")
            return False

    
        current_record_key = (record.get("trip_id"), record.get("vehicle_id"))
        if current_record_key in self.processed_combinations_in_batch:
            logging.warning(f"Duplicate record found during individual validation: {current_record_key}")
            return False
        self.processed_combinations_in_batch.add(current_record_key)

        return True

    def process_and_validate(self, records: List[Dict]) -> pd.DataFrame:
        """Processes a list of raw records, validating and cleaning them."""
        self.processed_combinations_in_batch.clear() 
        cleaned_data = []
        initial_records_count = len(records)

        for record in records:
            if not self._is_valid_record(record):
                metrics["total_invalid"] += 1
                continue

            try:
                direction = 'Out' if str(record.get("direction")) == '0' else 'Back'
                service_key_map = {'W': 'Weekday', 'S': 'Saturday', 'U': 'Sunday'}
                service_key = service_key_map.get(record.get("service_key", "W"), 'Weekday')

                cleaned_data.append({
                    "trip_id": int(record["trip_id"]),
                    "route_id": int(record.get("route_number", 0)),
                    "vehicle_id": int(record["vehicle_id"]),
                    "service_key": service_key,
                    "direction": direction
                })
            except Exception as e:
                logging.warning(f"Failed to convert record {record.get('trip_id')} to valid types: {e}")
                metrics["total_invalid"] += 1

        logging.info(f"Processed {len(cleaned_data)} valid records out of {initial_records_count} received.")
        return pd.DataFrame(cleaned_data)

class StopEventPipelineHandler:
    def __init__(self):
        self.cleaner = StopEventCleaner()

    def process_data(self, records: List[Dict]) -> pd.DataFrame:
        """Orchestrates data cleaning and validation."""
        return self.cleaner.process_and_validate(records)

    def save_to_postgres(self, trip_metadata_df: pd.DataFrame, db_config: Dict):
        """Saves processed trip metadata to PostgreSQL."""
        connection = None
        try:
            if trip_metadata_df.empty:
                logging.info("No valid records to insert (DataFrame is empty).")
                return

            
            df_to_insert = trip_metadata_df[~trip_metadata_df["trip_id"].isin(metrics["loaded_trip_ids"])]
            metrics["total_skipped"] += len(trip_metadata_df) - len(df_to_insert) # Increment skipped for those already in memory

            if df_to_insert.empty:
                logging.info("All unique trip_ids in this batch already exist in memory. Skipping insert.")
                return

            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()

            
            df_to_insert = df_to_insert[["trip_id", "route_id", "vehicle_id", "service_key", "direction"]]
            buffer = StringIO()
            df_to_insert.to_csv(buffer, index=False, header=False, sep='\t')
            buffer.seek(0)

            # Use copy_from for efficient bulk insertion
            cursor.copy_from(
                file=buffer,
                table="trip", # Assuming the table name is 'trip' for trip metadata
                null="",
                sep='\t',
                columns=("trip_id", "route_id", "vehicle_id", "service_key", "direction")
            )

            connection.commit()
            metrics["loaded_trip_ids"].update(df_to_insert["trip_id"].tolist())
            metrics["total_loaded"] += len(df_to_insert)
            logging.info(f"{len(df_to_insert)} new unique records inserted into PostgreSQL.")

        except psycopg2.Error as e:
            logging.error(f"PostgreSQL error saving trip data: {e}")
            if connection:
                connection.rollback() # Rollback in case of DB error
        except Exception as e:
            logging.error(f"General error saving trip data to PostgreSQL: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()

# Pub/Sub Callback and Main Loop
pipeline_handler = StopEventPipelineHandler() 

def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
    """Callback function executed for each Pub/Sub message."""
    try:
        data = json.loads(message.data.decode("utf-8"))
        metrics["total_received"] += 1
        metrics["last_message_time"] = datetime.now()

        with metrics["buffer_lock"]:
            metrics["message_buffer"].append(data)
            if len(metrics["message_buffer"]) >= config_vars['batch_size']:
                batch = metrics["message_buffer"][:] 
                metrics["message_buffer"].clear() 

                logging.info(f"Processing batch of {len(batch)} messages.")
                trip_metadata_df = pipeline_handler.process_data(batch)
                pipeline_handler.save_to_postgres(trip_metadata_df, config_vars['db_config'])

        message.ack() 
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from message: {e}. Message data: {message.data.decode('utf-8')}")
        message.nack() 
    except Exception as e:
        logging.error(f"Error in Pub/Sub callback: {e}")
        message.nack() 

def monitor_and_subscribe(project_id: str, subscription_id: str, idle_timeout: int):
    """Manages the Pub/Sub subscription and idle timeout."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    log_and_print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscribing to {subscription_path}...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=pubsub_callback)

    with subscriber:
        try:
            while True:
                if (datetime.now() - metrics["last_message_time"]).total_seconds() > idle_timeout:
                    log_and_print(f"[{datetime.now().strftime('%H:%M:%S')}] No messages for {idle_timeout} seconds. Exiting...")
                    streaming_pull_future.cancel()
                    # Process any remaining messages in the buffer before exiting
                    with metrics["buffer_lock"]:
                        if metrics["message_buffer"]:
                            remaining_batch = metrics["message_buffer"][:]
                            metrics["message_buffer"].clear()
                            log_and_print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing final batch of {len(remaining_batch)} messages.")
                            trip_metadata_df = pipeline_handler.process_data(remaining_batch)
                            pipeline_handler.save_to_postgres(trip_metadata_df, config_vars['db_config'])
                    break
                time.sleep(5) 
        except TimeoutError:
            log_and_print(f"[{datetime.now().strftime('%H:%M:%S')}] Listening timed out.")
        except Exception as e:
            logging.error(f"Subscriber main loop crashed: {e}. Attempting clean shutdown...")
            streaming_pull_future.cancel()
        finally:
            try:
                streaming_pull_future.result(timeout=10) 
            except Exception as e:
                logging.warning(f"Error while waiting for streaming_pull_future to complete: {e}")

def log_and_print(message: str):
    """Helper function to print to console and log to file."""
    print(message)
    logging.info(message)


config_vars = {}

if __name__ == "__main__":
    start_pipeline_time = time.time()
    log_file_name = "stop_event_pipeline_batched.log"
    setup_logging(log_file_name)

    try:
        gcp_project_id, gcp_subscription_id, _, db_connection_config, batch_size_val, idle_timeout_val = load_environment_variables()

        # Store critical configurations in a global dictionary for access by callbacks
        config_vars['project_id'] = gcp_project_id
        config_vars['subscription_id'] = gcp_subscription_id
        config_vars['db_config'] = db_connection_config
        config_vars['batch_size'] = batch_size_val
        config_vars['idle_timeout'] = idle_timeout_val

        monitor_and_subscribe(gcp_project_id, gcp_subscription_id, config_vars['idle_timeout'])

    except ValueError as ve:
        logging.critical(f"Configuration error: {ve}")
    except Exception as e:
        logging.critical(f"An unhandled error occurred in the main pipeline: {e}", exc_info=True) 
        end_pipeline_time = time.time()
        elapsed_pipeline_time = end_pipeline_time - start_pipeline_time

        log_and_print("-" * 50)
        log_and_print("Pipeline Summary:")
        log_and_print(f"Total messages received: {metrics['total_received']}")
        log_and_print(f"Total records loaded into DB: {metrics['total_loaded']}")
        log_and_print(f"Total records skipped (already in DB/memory): {metrics['total_skipped']}")
        log_and_print(f"Total records failed validation/conversion: {metrics['total_invalid']}")
        log_and_print(f"Elapsed pipeline runtime: {elapsed_pipeline_time:.2f} seconds")

        if os.path.exists(log_file_name):
            file_size_kb = os.path.getsize(log_file_name) / 1024
            log_and_print(f"Log file: {log_file_name} ({file_size_kb:.2f} KB)")
        log_and_print("-" * 50)
