from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/varshi/data_engineering/credential.json"
project_id = "dataengineering-trimetproject"
subscription_id = "Trimetdata-sub"

timeout = 30.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
removed_count = 0

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global removed_count
    print(f"Removed Ids:{message.message_id}")
    message.ack()
    removed_count += 0

def main():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
       try:
           streaming_pull_future.result(timeout=timeout)
       except TimeoutError:
           streaming_pull_future.cancel()  
           streaming_pull_future.result()  

    print("removed_count", removed_count)


if __name__== "__main__":
    main()
