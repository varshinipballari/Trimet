from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
import time
import os
from datetime import datetime
from datetime import date

os.environ[" GOOGLE_APPLICATION_CREDENTIALS"]="/home/varshi/credential.json"
project_id = "dataengineering-trimetproject"
subscription_id = "Trimetdata-sub"
timeout = 500 
idle_timeout = 10  
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

today_str = date.today().isoformat()
output_filename = f"sub_data_{today_str}.json"
output_file = open(output_filename, 'a')

messagecount = 0
start_time = None
last_message_time = None

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global messagecount, last_message_time
    data_recived = message.data.decode("utf-8")
    json_data = json.loads(data_recived)
    output_file.write(json.dumps(json_data) + '\n')
    output_file.flush()
    message.ack()
    messagecount += 1
    last_message_time = time.time()
    if messagecount % 10000 == 0:
        print(f" Received {messagecount} messages")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Subscriber listening on {subscription_path}...")

with subscriber:
    try:
        start_time = time.time()
        last_message_time = start_time
        while True:
            current_time = time.time()

            if current_time - start_time > timeout:
                break
            if current_time - last_message_time > idle_timeout:
                break

            time.sleep(1)

        streaming_pull_future.cancel()
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

end_time = time.time()

file_size_bytes = os.path.getsize(output_filename)
file_size_kb = file_size_bytes / 1024
print(f" Total messages received: {messagecount}")
print(f"Time taken to receive all messages: {end_time - start_time:.2f} seconds")
print(f"File size: {file_size_kb:.2f} KB\n")
print( f"Breadcrumbs in {output_filename}\n")
