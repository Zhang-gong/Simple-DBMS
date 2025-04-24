import os
import json
import csv

# Create directory
os.makedirs("data/rel_i_100k", exist_ok=True)

# Step 1: Write metadata.json
metadata = {
    "name": "rel_i_100k",
    "columns": [
        {"name": "id", "type": "INT"},
        {"name": "val", "type": "INT"}
    ],
    "primary_key": "id"
}
with open("data/rel_i_100k/metadata.json", "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=4)

# Step 2: Write data.csv
with open("data/rel_i_100k/data.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "val"])
    for i in range(1, 100001):
        writer.writerow([i, i])

print("✅ rel_i_100k table written to disk.")
