import logging
from datetime import datetime
from typing import Dict, Optional

class Validation:
    def __init__(self):
        self.processed_combinations = set()
        self.first_op_date: Optional[str] = None

    def validate_records(self, record: Dict) -> bool:
        """Validate a GPS record against all business rules with enhanced assertions"""
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
        
        missing_fields = [f for f in required_fields if f not in record]
        if missing_fields:
            raise AssertionError(f"Missing required fields: {', '.join(missing_fields)}")
            
        for field, (expected_type, error_msg) in required_fields.items():
            if record[field] is None:
                raise AssertionError(f"{field} cannot be None")
            if not isinstance(record[field], expected_type):
                raise AssertionError(f"{error_msg}. Got {type(record[field])} for {field}")

    def _gps_validator(self, record: Dict):
        """Enhanced GPS validation with precision checks"""
        lat = record['GPS_LATITUDE']
        lon = record['GPS_LONGITUDE']
        
        # Check value ranges
        if not 45.2 <= lat <= 45.7:
            raise AssertionError(f"Latitude {lat} outside Portland area (45.2-45.7)")
        if not -124.0 <= lon <= -122.0:
            raise AssertionError(f"Longitude {lon} outside Portland area (-124.0 - -122.0)")
            
        # Check for obviously wrong coordinates (like 0,0)
        if abs(lat) < 1 or abs(lon) < 1:
            raise AssertionError(f"Suspicious coordinates: ({lat}, {lon})")

    def _validate_speed_plausibility(self, record: Dict):
        """Check if calculated speed is physically possible"""
        if 'SPEED' in record:
            if record['SPEED'] < 0:
                raise AssertionError(f"Negative speed detected: {record['SPEED']} m/s")
            if record['SPEED'] > 45:  # ~100 mph (max plausible bus speed)
                raise AssertionError(f"Implausible speed: {record['SPEED']} m/s")
			
    def _validate_vehicle_data(self, record: Dict):
        # Vehicle ID validation
        if record['VEHICLE_ID'] <= 0:
            raise AssertionError(f"Invalid Vehicle ID {record['VEHICLE_ID']} (must be positive)")
            
        # Odometer validation
        if record['METERS'] < 0:
            raise AssertionError(f"Negative odometer reading: {record['METERS']} meters")
        if record['METERS'] > 1_000_000:  # 1,000 km upper limit
            raise AssertionError(f"Odometer value {record['METERS']} seems excessively high")
            
    def _check_geographic_jump(self, record: Dict, last_latlon: Optional[tuple] = None):
        """Rejects extreme spatial jumps suggesting GPS error or spoofing"""
        from geopy.distance import geodesic
        if last_latlon:
            dist_km = geodesic(last_latlon, (record['GPS_LATITUDE'], record['GPS_LONGITUDE'])).km
            if dist_km > 5:  # More than 5 km jump within one event = suspect
                raise AssertionError(f"Unrealistic location jump: {dist_km:.2f} km")

	
    def _validate_temporal_data(self, record: Dict):
        """Validate time-related fields"""
        # ACT_TIME validation
        act_time = record['ACT_TIME']
        if act_time < 0:
            raise AssertionError(f"Negative ACT_TIME: {act_time}")

    def _duplicates_check(self, record: Dict):
        """Enhanced duplicate detection with timestamp validation"""
        key = (
            record['VEHICLE_ID'], 
            record['EVENT_NO_TRIP'],
            record['EVENT_NO_STOP'],
            record['ACT_TIME'],
            record['METERS']
        )
        
        if key in self.processed_combinations:
            raise AssertionError(f"Duplicate record detected: Vehicle {key[0]}, Trip {key[1]}, "
                               f"Stop {key[2]}, Time {key[3]}, Odometer {key[4]}")
        self.processed_combinations.add(key)

    
