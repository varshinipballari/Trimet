import json, os, time

from concurrent import futures
from datetime import date
from google.cloud import pubsub_v1


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/varshi/data_engineering/credential.json"
)
# Configuration
project_id = "dataengineering-trimetproject"
topic_id = "Trimetdata"

message_count = 0


today_str = date.today().isoformat()
folder_path = "/home/varshi/data_engineering/bus_data"
filename = os.path.join(folder_path, f"vehicle_data_{today_str}.json")

batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024, max_latency=0.1, max_messages=1000
)


publisher = pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)


def future_callback(future):
    try:
        # Wait for the result of the publish operation.
        future.result()
    except Exception as e:
        print(f"An error occurred: {e}")


with open(filename, "r") as file:
    breadcrumb_records = json.load(file)

futures_list = []

starttime = time.time()

for vehicle_id, records in breadcrumb_records.items():
    for record in records:
        data = json.dumps(record).encode("utf-8")
        future = publisher.publish(topic_path, data)

        future.add_done_callback(future_callback)
        futures_list.append(future)

        message_count += 1
        if message_count % 10000 == 0:
            print(f"Messages published so far: {message_count}")

# Wait for all futures to complete
for _ in futures.as_completed(futures_list):
    continue

stoptime = time.time()
timetaken = stoptime - starttime

print(f"Published {len(futures_list)} messages to {topic_path}.")
print(f"Total breadcrumbs published: {message_count}")
print(f"Time taken to publish all messages: {timetaken:.2f} seconds")
