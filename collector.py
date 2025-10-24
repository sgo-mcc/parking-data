import requests
import json
import csv
import time
from datetime import datetime
import os

import gzip
from pathlib import Path

# API endpoint
BASE_URL = "https://data.melbourne.vic.gov.au/api/v2/catalog/datasets/on-street-parking-bay-sensors/records"

# CSV file name
CSV_FILE = "parking_sensors_data.csv"

def get_all_parking_data():
    """
    Fetch ALL parking sensor data from the Melbourne API.
    Uses pagination to retrieve all records without limit.
    """
    all_data = []
    offset = 0
    limit = 100  # Fetch 100 records per request
    
    print("Fetching all parking sensor data...")
    
    while True:
        # Set up parameters for this batch
        params = {
            'limit': limit,
            'offset': offset
        }
        
        # Make the API request
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Get the records array from the response
            batch_data = data.get('records', [])
            
            # If no data returned, we've reached the end
            if not batch_data:
                break
            
            # DEBUG: Print full structure of first record on first batch
            if offset == 0 and len(batch_data) > 0:
                print("\n" + "=" * 80)
                print("DEBUG: FULL FIRST RECORD STRUCTURE")
                print("=" * 80)
                print(json.dumps(batch_data[0], indent=2))
                print("=" * 80)
                
                # Also print what keys are available
                print("\nDEBUG: Top-level keys in first record:")
                print(list(batch_data[0].keys()))
                
                if 'fields' in batch_data[0]:
                    print("\nDEBUG: Keys inside 'fields':")
                    print(list(batch_data[0]['fields'].keys()))
                    print("\nDEBUG: Sample field values:")
                    for key, value in list(batch_data[0]['fields'].items())[:5]:
                        print(f"  {key}: {value}")
                
                print("=" * 80 + "\n")
            
            # Add all records to our collection
            all_data.extend(batch_data)
            
            total_count = data.get('total_count', 'unknown')
            print(f"Retrieved {len(batch_data)} records (Total so far: {len(all_data)} / {total_count})")
            
            # If we got fewer records than the limit, we've reached the end
            if len(batch_data) < limit:
                break
            
            # Move to next batch
            offset += limit
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            break
    
    print(f"\nCompleted! Total records retrieved: {len(all_data)}")
    return all_data


def flatten_record(record, api_call_time):
    """
    Flatten a record to extract only the required fields.
    Handles nested 'location' field by extracting lat and lon.
    
    Args:
        record: Dictionary containing parking sensor data (full record structure)
        api_call_time: Timestamp when the API call was made
    
    Returns:
        Dictionary with flattened fields
    """
    # Extract the 'fields' dictionary from the record
    # The structure is: record -> record -> fields
    inner_record = record.get('record', {})
    fields = inner_record.get('fields', {})
    
    # DEBUG: Print what we're extracting (only for first record)
    if not hasattr(flatten_record, 'printed_debug'):
        print("\n" + "=" * 80)
        print("DEBUG: FLATTENING RECORD")
        print("=" * 80)
        print(f"Record keys: {list(record.keys())}")
        print(f"Fields keys: {list(fields.keys())}")
        print(f"Fields content: {json.dumps(fields, indent=2)}")
        print("=" * 80 + "\n")
        flatten_record.printed_debug = True
    
    flattened = {
        'api_call_time': api_call_time,
        'lastupdated': fields.get('lastupdated', ''),
        'status_timestamp': fields.get('status_timestamp', ''),
        'zone_number': fields.get('zone_number', ''),
        'status_description': fields.get('status_description', ''),
        'kerbsideid': fields.get('kerbsideid', ''),
    }
    
    # Handle nested location field
    location = fields.get('location', {})
    if isinstance(location, dict):
        flattened['location_lon'] = location.get('lon', '')
        flattened['location_lat'] = location.get('lat', '')
    else:
        flattened['location_lon'] = ''
        flattened['location_lat'] = ''
    
    # DEBUG: Print flattened result for first record
    if not hasattr(flatten_record, 'printed_flattened'):
        print("\n" + "=" * 80)
        print("DEBUG: FLATTENED RESULT")
        print("=" * 80)
        print(json.dumps(flattened, indent=2))
        print("=" * 80 + "\n")
        flatten_record.printed_flattened = True
    
    return flattened


def save_to_csv(data, api_call_time):
    """
    Save or append data to CSV file with API call timestamp.
    
    Args:
        data: List of parking sensor records
        api_call_time: Timestamp when the API call was made
    """
    if not data:
        print("No data to save.")
        return
    
    print(f"\nDEBUG: Processing {len(data)} records for CSV...")
    
    # Flatten all records to extract only required fields
    flattened_data = [flatten_record(record, api_call_time) for record in data]
    
    print(f"DEBUG: Flattened {len(flattened_data)} records")
    print(f"DEBUG: Sample flattened record: {flattened_data[0]}")
    
    # Define the column order
    fieldnames = [
        'api_call_time',
        'lastupdated',
        'status_timestamp',
        'zone_number',
        'status_description',
        'kerbsideid',
        'location_lon',
        'location_lat'
    ]
    
    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(CSV_FILE)
    
    # Open CSV file in append mode
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header only if file is new
        if not file_exists:
            writer.writeheader()
            print(f"Created new CSV file: {CSV_FILE}")
        else:
            print(f"Appending to existing CSV file: {CSV_FILE}")
        
        # Write all records
        writer.writerows(flattened_data)
    
    print(f"Saved {len(flattened_data)} records to {CSV_FILE}")


def run_once():
    """
    Run a single iteration: fetch data and save to CSV.
    """
    # Record the API call time
    api_call_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*60}")
    print(f"API Call Time: {api_call_time}")
    print(f"{'='*60}")
    
    # Fetch data
    data = get_all_parking_data()
    
    # Save to CSV
    if data:
        save_to_csv(data, api_call_time)
        
        # Display summary
        print(f"\n=== Summary ===")
        print(f"Total records: {len(data)}")
        
        # Count by status - check in nested 'fields' dictionary
        occupied = 0
        unoccupied = 0
        for record in data:
            inner_record = record.get('record', {})
            fields = inner_record.get('fields', {})
            status = fields.get('status_description', '')
            if status == 'Present':
                occupied += 1
            elif status == 'Unoccupied':
                unoccupied += 1
        
        print(f"Occupied: {occupied}")
        print(f"Unoccupied: {unoccupied}")
    else:
        print("No data retrieved.")


def run_continuously(interval_minutes=60):
    """
    Run the data collection continuously at specified intervals.
    
    Args:
        interval_minutes: Time interval in minutes between API calls (default: 60)
    """
    print(f"Starting continuous data collection...")
    print(f"Interval: Every {interval_minutes} minutes")
    print(f"CSV file: {CSV_FILE}")
    print(f"Press Ctrl+C to stop\n")
    
    try:
        iteration = 1
        while True:
            print(f"\n{'#'*60}")
            print(f"# Iteration {iteration}")
            print(f"{'#'*60}")
            
            # Run data collection
            run_once()
            
            # Calculate next run time
            next_run = datetime.now()
            next_run = next_run.replace(second=0, microsecond=0)
            next_run = next_run.replace(minute=next_run.minute + interval_minutes)
            
            print(f"\nNext run scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Waiting {interval_minutes} minutes...")
            
            # Wait for the specified interval
            time.sleep(interval_minutes * 60)
            
            iteration += 1
            
    except KeyboardInterrupt:
        print("\n\nData collection stopped by user.")
        print(f"Total iterations completed: {iteration - 1}")
        print(f"Data saved in: {CSV_FILE}")


# NEW: write each run to data/raw/YYYY-MM-DD/HHMM.jsonl.gz
def save_to_jsonl_gz(flattened_records, dt_utc):
    dt = datetime.strptime(dt_utc, '%Y-%m-%d %H:%M:%S')
    day = dt.strftime('%Y-%m-%d')
    hhmm = dt.strftime('%H%M')
    out_dir = Path("data/raw") / day
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{hhmm}.jsonl.gz"
    with gzip.open(out_path, "at", encoding="utf-8") as f:
        for row in flattened_records:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"Saved {len(flattened_records)} records to {out_path}")
    return str(out_path)

def run_once_write_jsonl():
    # UTC timestamps are best for automation
    api_call_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*60}")
    print(f"API Call Time (UTC): {api_call_time}")
    print(f"{'='*60}")

    data = get_all_parking_data()
    if not data:
        print("No data retrieved.")
        return

    # Reuse your flattening
    flattened_data = [flatten_record(record, api_call_time) for record in data]

    # Write compressed snapshot for this run
    out = save_to_jsonl_gz(flattened_data, api_call_time)

    # Optional quick summary (kept as-is)
    occupied = 0
    unoccupied = 0
    for record in data:
        inner_record = record.get('record', {})
        fields = inner_record.get('fields', {})
        status = fields.get('status_description', '')
        if status == 'Present':
            occupied += 1
        elif status == 'Unoccupied':
            unoccupied += 1
    print(f"\n=== Summary ===")
    print(f"Total records: {len(data)}")
    print(f"Occupied: {occupied}")
    print(f"Unoccupied: {unoccupied}")
    print(f"Wrote: {out}")

if __name__ == "__main__":
    # IMPORTANT FOR ACTIONS: run ONCE and exit
    # keep your continuous mode for local runs only
    RUN_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"

    if RUN_GITHUB:
        # Actions path: run once -> write compressed file -> exit
        run_once_write_jsonl()
    else:
        # Your local dev path (unchanged defaults)
        INTERVAL_MINUTES = 1
        # run_once()                  # if you want a single local run
        run_continuously(interval_minutes=INTERVAL_MINUTES)
