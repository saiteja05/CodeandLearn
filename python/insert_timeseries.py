import json
from datetime import datetime, timedelta
import random
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Function to generate JSON document with increasing datetime field
def generate_json_with_datetimes(start_datetime, end_datetime, interval_minutes):
    data = []
    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        zone_id=random.randint(1, 50)
        temperature = random.randint(1, 100)
        pressure=random.randint(1, 100)
        if(temperature%2==0):
            data.append({"zone_id":zone_id,"datetime": current_datetime.strftime("%Y-%m-%d %H:%M:%S"),"manufacture":"ABC","temperature":temperature,"pressure":pressure})
        else:
            data.append({"zone_id":zone_id,"datetime": current_datetime.strftime("%Y-%m-%d %H:%M:%S"),"manufacture":"DGF","temperature":temperature,"pressure":pressure})

        current_datetime += timedelta(minutes=interval_minutes)
    return data

uri = "mongodb+srv://tejaboddapati:Bangalore123@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["timeseries_db"]
collection = db["sensors"]
# Example usage:
try:
    start_datetime = datetime(2024, 1, 1, 0, 0)  # Start datetime
    end_datetime = datetime(2024, 3, 31, 23, 59)  # End datetime
    interval_minutes = 1  # Interval in minutes

# Generate JSON document with increasing datetime field
    json_data = generate_json_with_datetimes(start_datetime, end_datetime, interval_minutes)
# Output JSON document
    output_json = json.dumps(json_data, indent=4)
# # print(output_json)
#     print(type(output_json))
    response = collection.insert_many(json.loads(output_json))
    print(len(response.inserted_ids))

except Exception as e:
    print(e)
