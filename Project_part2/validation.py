import logging
from datetime import datetime
from typing import Dict, Optional

class Validation:
    def __init__(self):
        self.processed_combinations = set()
        self.first_op_date: Optional[str] = None

    def validate_records(self, record: Dict) -> bool:
        try:
            self._validate_required_fields(record)
            self._gps_validator(record)
            self._validate_speed_plausibility(record)
            self._validate_vehicle_data(record)
            self._check_geographic_jump(record)
            self._validate_temporal_data(record)
            self._duplicates_check(record)
            return True
        except AssertionError as e:
            logging.warning(f"Validation failed for trip {record.get('EVENT_NO_TRIP')}: {e}")
            return False

    def _validate_required_fields(self, record: Dict):
        required_fields = {
            'EVENT_NO_TRIP': (int, "Trip ID must be an integer"),
            'OPD_DATE': (str, "Operation date must be a string"),
            'ACT_TIME': ((int, float), "Activity time must be numeric"),
            'VEHICLE_ID': (int, "Vehicle ID must be an integer"),
            'GPS_LATITUDE': ((int, float), "Latitude must be numeric"),
            'GPS_LONGITUDE': ((int, float), "Longitude must be numeric"),
            'METERS': ((int, float), "Odometer must be numeric")
        }

        try:
            missing_fields = [f for f in required_fields if f not in record]
            if missing_fields:
                raise AssertionError(f"Missing required fields: {', '.join(missing_fields)}")
        except AssertionError as e:
            logging.warning(f"[Required] {e}")
            raise

        for field, (expected_type, error_msg) in required_fields.items():
            try:
                if record[field] is None:
                    raise AssertionError(f"{field} cannot be None")
                assert isinstance(record[field], expected_type), f"{error_msg}. Got {type(record[field])} for {field}"
            except AssertionError as e:
                logging.warning(f"[Required] {e}")
                raise

    def _gps_validator(self, record: Dict):
        lat = record['GPS_LATITUDE']
        lon = record['GPS_LONGITUDE']

        try:
            assert 45.2 <= lat <= 45.7, f"Latitude {lat} outside Portland area (45.2-45.7)"
        except AssertionError as e:
            logging.warning(f"[GPS] {e}")
            raise

        try:
            assert -124.0 <= lon <= -122.0, f"Longitude {lon} outside Portland area (-124.0 - -122.0)"
        except AssertionError as e:
            logging.warning(f"[GPS] {e}")
            raise

        try:
            assert abs(lat) >= 1 and abs(lon) >= 1, f"Suspicious coordinates: ({lat}, {lon})"
        except AssertionError as e:
            logging.warning(f"[GPS] {e}")
            raise

    def _validate_speed_plausibility(self, record: Dict):
        if 'SPEED' in record:
            try:
                assert record['SPEED'] >= 0, f"Negative speed detected: {record['SPEED']} m/s"
            except AssertionError as e:
                logging.warning(f"[Speed] {e}")
                raise

            try:
                assert record['SPEED'] <= 45, f"Implausible speed: {record['SPEED']} m/s"
            except AssertionError as e:
                logging.warning(f"[Speed] {e}")
                raise

    def _validate_vehicle_data(self, record: Dict):
        try:
            assert record['VEHICLE_ID'] > 0, f"Invalid Vehicle ID {record['VEHICLE_ID']} (must be positive)"
        except AssertionError as e:
            logging.warning(f"[Vehicle] {e}")
            raise

        try:
            assert record['METERS'] >= 0, f"Negative odometer reading: {record['METERS']} meters"
        except AssertionError as e:
            logging.warning(f"[Vehicle] {e}")
            raise

        try:
            assert record['METERS'] <= 1_000_000, f"Odometer value {record['METERS']} seems excessively high"
        except AssertionError as e:
            logging.warning(f"[Vehicle] {e}")
            raise

    def _check_geographic_jump(self, record: Dict, last_latlon: Optional[tuple] = None):
        from geopy.distance import geodesic
        if last_latlon:
            try:
                dist_km = geodesic(last_latlon, (record['GPS_LATITUDE'], record['GPS_LONGITUDE'])).km
                assert dist_km <= 5, f"Unrealistic location jump: {dist_km:.2f} km"
            except AssertionError as e:
                logging.warning(f"[Jump] {e}")
                raise

    def _validate_temporal_data(self, record: Dict):
        try:
            assert record['ACT_TIME'] >= 0, f"Negative ACT_TIME: {record['ACT_TIME']}"
        except AssertionError as e:
            logging.warning(f"[Time] {e}")
            raise

    def _duplicates_check(self, record: Dict):
        key = (
            record['VEHICLE_ID'], 
            record['EVENT_NO_TRIP'],
            record['EVENT_NO_STOP'],
            record['ACT_TIME'],
            record['METERS']
        )

        try:
            assert key not in self.processed_combinations, (
                f"Duplicate record detected: Vehicle {key[0]}, Trip {key[1]}, Stop {key[2]}, Time {key[3]}, Odometer {key[4]}"
            )
            self.processed_combinations.add(key)
        except AssertionError as e:
            logging.warning(f"[Duplicate] {e}")
            raise
