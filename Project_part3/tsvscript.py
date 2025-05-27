import csv
import json
from geojson import Feature, FeatureCollection, Point

features = []

# Use the correct TSV file name here
with open('fivec.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    
    for row in reader:
        if len(row) < 3:
            continue

        try:
            lon = float(row[0])
            lat = float(row[1])
            speed = float(row[2])

            features.append(
                Feature(
                    geometry=Point((lon, lat)),
                    properties={"speed": speed}
                )
            )
        except ValueError:
            # Skip lines with invalid floats
            continue

collection = FeatureCollection(features)
with open("fivec.geojson", "w") as f:
    json.dump(collection, f, indent=2)

print(f"GeoJSON written with {len(features)} points")