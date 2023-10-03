from tuya_connector import TuyaOpenAPI
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime

load_dotenv()

def fetch_data_from_api(start_time, end_time, openapi, device_id):
    # Convert datetime format to Unix timestamp
    start_time_unix = int(datetime.strptime(start_time, "%d/%m/%Y %H:%M").timestamp()) * 1000
    end_time_unix = int(datetime.strptime(end_time, "%d/%m/%Y %H:%M").timestamp()) * 1000
    
    params = {
        "type": 7,
        "query_type": 1,
        "start_time": start_time_unix,
        "end_time": end_time_unix,
        "size": 1000
    }
    
    logs = []
    
    while True:
        response = openapi.get(f"/v2.0/cloud/thing/{device_id}/logs", params)
        
        # Collect only power logs
        power_logs = [log for log in response["result"]["logs"] if log["code"] == "cur_power"]
        logs.extend(power_logs)
        
        if not response["result"]["has_next"]:
            break
            
        params["start_row_key"] = response["result"]["next_row_key"]
        
    return logs

def process_data(logs, description):
    csv_rows = []
    for log in logs:
        timestamp = pd.to_datetime(log["event_time"], unit="ms")
        measurement = "Power"
        value = int(log["value"]) / 100.0
        uom = "W"
        details = description
        csv_rows.append([timestamp, measurement, value, uom, details])
    
    # Load to DataFrame
    df = pd.DataFrame(csv_rows, columns=["timestamp", "measurement", "value", "uom", "details"])
    
    # Sort by timestamp
    df = df.sort_values(by="timestamp")
    
    # Calculate the time difference in seconds between each reading
    df["time_diff"] = df["timestamp"].diff().dt.total_seconds()
    
    # Calculate the energy (kWh) for each interval: (Watts * seconds) / (3600 * 1000)
    df["kWh"] = (df["value"].astype(float) * df["time_diff"]) / 3600000.0

    # Calculate the running sum of energy used
    df["cumulative_kWh"] = df["kWh"].cumsum()
    
    # Drop the first row as its time_diff and kWh will be NaN
    df = df.dropna()
    
    # Drop the time_diff column as it's not needed in the output
    df.drop(columns=["time_diff"], inplace=True)
    
    # Append data to existing CSV if exists, otherwise create new
    if os.path.exists("output.csv"):
        df.to_csv("output.csv", mode="a", header=False, index=False)
    else:
        df.to_csv("output.csv", index=False)

def main():
    # API and device setup from .env file
    ACCESS_ID = os.getenv("ACCESS_ID")
    ACCESS_KEY = os.getenv("ACCESS_KEY")
    API_ENDPOINT = os.getenv("API_ENDPOINT")
    DEVICE_ID = os.getenv("DEVICE_ID")
    
    openapi = TuyaOpenAPI(API_ENDPOINT, ACCESS_ID, ACCESS_KEY)
    openapi.connect()
    
    # What time period to fetch and what was the cycle
    start_time = "02/10/2023 21:00"
    end_time = "02/10/2023 23:55"
    description = "dishwasher-quickwash-and-dry-3"
        
    # Fetch and process data
    logs = fetch_data_from_api(start_time, end_time, openapi, DEVICE_ID)
    process_data(logs, description)

if __name__ == "__main__":
    main()
