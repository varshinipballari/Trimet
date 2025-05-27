import urllib.request
import json,os,time
from concurrent import futures
from google.cloud import pubsub_v1
import logging
from dotenv import load_dotenv


# Set up credentials and Pub/Sub config
load_dotenv(".env")
google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
project_id = os.getenv("GCP_PROJECT_ID")
topic_id = os.getenv("GCP_TOPIC_ID")

# Vehicle IDs and API
vehicle_ids = [2903, 2910, 2912, 2914, 2918, 2919, 2927,
 2928, 2930, 2932, 2935, 2936, 2940, 3006, 3008, 3011, 3022,
 3024, 3026, 3028, 3034, 3042, 3055, 3057, 3058, 3103, 3104,
 3105, 3106, 3109, 3114, 3115, 3116, 3118, 3119, 3121, 3123,
 3125, 3127, 3137, 3139, 3141, 3142, 3147, 3149, 3150, 3157,
 3162, 3168, 3203, 3205, 3207, 3210, 3211, 3212, 3215, 3217, 
3221, 3223, 3227, 3231, 3232, 3244, 3246, 3247, 3255, 3266,
 3302, 3305, 3309, 3312, 3313, 3323, 3324, 3326, 3327, 3328,
 3330, 3405, 3408, 3415, 3416, 3418, 3503, 3506, 3509, 3510,
 3513, 3514, 3515, 3523, 3524, 3526, 3527, 3530, 3536, 3537,
 3543, 3546, 3548, 3550, 3553, 3556, 3557, 3565, 3567, 3568, 
3569, 3574, 3577, 3607, 3610, 3612, 3615, 3619, 3621, 3624,
 3629, 3630, 3631, 3633, 3635, 3639, 3640, 3644, 3650, 3703,
 3708, 3709, 3715, 3717, 3718, 3723, 3724, 3726, 3728, 3731,
 3734, 3738, 3746, 3751, 3755, 3757, 3801, 3804, 3902, 3910,
 3912, 3916, 3921, 3923, 3930, 3935, 3942, 3944, 3945, 3948,
 3949, 3953, 3956, 3958, 3962, 3963, 3964, 4001, 4006, 4009,
 4010, 4013, 4017, 4027, 4028, 4030, 4031, 4036, 4037, 4041,
 4044, 4047, 4048, 4054, 4065, 4068, 4069, 4206, 4214, 4215,
 4219, 4220, 4222, 4230, 4235, 4236, 4302, 4508, 4513, 4517, 4519, 4526, 4527]

bus_url = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={}'

# Pub/Sub setup
batch_settings = pubsub_v1.types.BatchSettings(max_bytes=1024 * 1024, max_latency=0.1, max_messages=1000)
publisher = pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)

# Logging and counters
all_data = {}
msg404_errorcount = 0
url_errorcount = 0
other_errorcount = 0
message_count = 0
futures_list = []

def future_callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"An error occurred while publishing: {e}")

start_time = time.time()

# Fetch and publish
for vehicle_id in vehicle_ids:
    try:
        with urllib.request.urlopen(bus_url.format(vehicle_id)) as response:
            data = json.loads(response.read().decode())
            all_data[vehicle_id] = data

            for record in data:
                data_bytes = json.dumps(record).encode("utf-8")
                future = publisher.publish(topic_path, data_bytes)
                future.add_done_callback(future_callback)
                futures_list.append(future)

                message_count += 1
                
    except Exception as e:
        logging.error(f"Error fetching and publish{e}")


for _ in futures.as_completed(futures_list):
    continue

end_time = time.time()
time_taken = end_time - start_time

# Final report
print(f"Data collection and publishing complete.")
print(f"Total vehicles fetched: {len(all_data)}")
print(f"Total messages published: {message_count}")
print(f"Time taken: {time_taken:.2f} seconds")

