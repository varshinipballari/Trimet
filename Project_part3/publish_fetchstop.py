import requests
from bs4 import BeautifulSoup
import json
import os
import re
from google.cloud import pubsub_v1
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv


load_dotenv(".env")
google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
project_id = os.getenv("GCP_PROJECT_ID")
subscription_id = os.getenv("GCP_SUBSCRIPTION_ID")

batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024,
    max_latency=0.1,
    max_messages=1000
)
publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)
data_list = []

base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={}"
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

today_str = datetime.now().strftime("%Y-%m-%d")
output_file = f"stop_event_{today_str}.json"


def publish_event(event):
    try:
        message_bytes = json.dumps(event).encode('utf-8')
        future = publisher.publish(topic_path, message_bytes)
        future.add_done_callback(lambda f: f.exception() and print(f"Publish error: {f.exception()}"))
        return future
    except Exception as e:
        print(f"Failed to publish event: {e}")


def fetch_vehicle_data(vehicle_id):
    vehicle_events = []
    try:
        response = requests.get(base_url.format(vehicle_id), timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f'Failed to fetch data for vehicle {vehicle_id}: {e}')
        return vehicle_events

    soup = BeautifulSoup(response.text, 'html.parser')
    h2_tags = soup.find_all('h2')

    for h2 in h2_tags:
        h2_text = h2.text.strip()
        match = re.search(r'PDX_TRIP\s+(\d+)', h2_text)
        trip_id = match.group(1) if match else None

        if not trip_id:
            print(f"Could not find trip_id in header: '{h2_text}'")
            continue

        table = h2.find_next_sibling('table')
        if not table:
            continue

        rows = table.find_all('tr')
        headers = [cell.text.strip() for cell in rows[0].find_all(['th', 'td'])]

        for row in rows[1:]:
            cells = row.find_all(['th', 'td'])
            if len(cells) != len(headers):
                continue

            row_data = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
            row_data['trip_id'] = trip_id
            row_data['vehicle_id'] = vehicle_id
            vehicle_events.append(row_data)

    return vehicle_events


def main():
    print(f"Fetching stop events for {len(vehicle_ids)} vehicles...")

    publish_futures = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_vehicle_data, vid): vid for vid in vehicle_ids}
        for future in as_completed(futures):
            vid = futures[future]
            try:
                events = future.result()
                print(f"Vehicle {vid}: {len(events)} events fetched")
                data_list.extend(events)

                for event in events:
                    if event.get("trip_id") and event.get("vehicle_id"):
                        future = publish_event(event)
                        publish_futures.append(future)
                    else:
                        print(f"Skipping invalid event: {event}")
            except Exception as e:
                print(f"Error processing vehicle {vid}: {e}")

    # Wait for all publish calls to complete
    for f in as_completed(publish_futures):
        try:
            f.result()
        except Exception as e:
            print(f"Publish failed: {e}")

    # Save locally
    try:
        with open(output_file, 'w') as f:
            json.dump(data_list, f, indent=4)
        print(f"\nData saved to {output_file}")
    except Exception as e:
        print(f"Failed to save JSON: {e}")

    print(f"\nFinished. Published {len(publish_futures)} messages.")

if __name__ == "__main__":
    main()
