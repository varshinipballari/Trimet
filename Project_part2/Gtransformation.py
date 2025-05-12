import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
from validation import Validation


class Transformation:
    def __init__(self):
        self.validator = Validation()

    def process(self, raw_data: List[Dict]) -> pd.DataFrame:
        processed = []

        for entry in raw_data:
            if not self.validator.validate_records(entry):
                continue

            try:
                ts = self._combine_date_time(entry)
                processed.append({
                    "timestamp": ts,
                    "latitude": entry["GPS_LATITUDE"],
                    "longitude": entry["GPS_LONGITUDE"],
                    "trip_id": entry["EVENT_NO_TRIP"],
                    "meters": entry["METERS"]
                })
            except Exception:
                continue

        if not processed:
            return pd.DataFrame()

        df = pd.DataFrame(processed)
        return self._append_speed(df)

    def _combine_date_time(self, entry: Dict) -> datetime:
        date_part = entry["OPD_DATE"].split(":")[0]
        base = datetime.strptime(date_part, "%d%b%Y")
        return base + timedelta(seconds=entry["ACT_TIME"])

    def _append_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        df.sort_values(by=["trip_id", "timestamp"], inplace=True)
        df["dist_delta"] = df.groupby("trip_id")["meters"].diff()
        df["time_delta"] = df.groupby("trip_id")["timestamp"].diff().dt.total_seconds()
        df["speed"] = df["dist_delta"] / df["time_delta"]
        df["speed"] = df.groupby("trip_id")["speed"].ffill()

        return df[["timestamp", "latitude", "longitude", "speed", "trip_id"]]


class TripInfoBuilder:
    def __init__(self):
        self.summary = {}

    def build_summary(self, records: List[Dict]) -> pd.DataFrame:
        for data in records:
            trip = data.get("EVENT_NO_TRIP")
            vehicle = data.get("VEHICLE_ID")
            route = data.get("ROUTE_ID", 0)

            try:
                date_only = data["OPD_DATE"].split(":")[0]
                day_name = datetime.strptime(date_only, "%d%b%Y").strftime("%A")
                service_type = "Weekday" if day_name not in ["Saturday", "Sunday"] else day_name
            except Exception:
                service_type = "Weekday"

            if trip not in self.summary:
                self.summary[trip] = {
                    "trip_id": trip,
                    "route_id": route,
                    "vehicle_id": vehicle,
                    "service_key": service_type,
                    "direction": "Out"
                }

        return pd.DataFrame(self.summary.values())
