from google.cloud import pubsub_v1
import json
from datetime import datetime
import time
import os
from datetime import date

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/varshi/data_engineering/credential.json"
# Configuration
project_id = "dataengineering-trimetproject"
topic_id = "Trimetdata"

message_count = 0


today_str = date.today().isoformat()
folder_path = "/home/varshi/data_engineering/bus_data"
#filename =os.path.join(folder_path,f"vehicle_data_{today_str}.json")
filename = os.path.join(folder_path, "vehicle_data_2025-04-17.json")

batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024,
    max_latency=0.1,
    max_messages=1000
)


publisher = pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)


with open(filename, "r") as file:
    breadcrumb_records = json.load(file)

futures = []

starttime = time.time()

for vehicle_id, records in breadcrumb_records.items():
    for record in records:
        data = json.dumps(record).encode("utf-8")
        future = publisher.publish(topic_path, data)
        futures.append(future)
        message_count +=1
# Wait for all futures to complete
for future in futures:
    future.result()

stoptime = time.time()
timetaken = stoptime - starttime

print(f"Published {len(futures)} messages to {topic_path}.")
print(f"Total breadcrumbs published: {message_count}")
print(f"Time taken to publish all messages: {timetaken:.2f} seconds")
