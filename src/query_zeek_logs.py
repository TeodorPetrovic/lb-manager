import requests
import json
import datetime
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Hardcoded properties
ELASTIC_URL = "https://10.100.0.7:9200"
USERNAME = "elastic"
PASSWORD = "-5HJtLOH+H3ma-S*9wAA"

# Host IP and time range
HOST_IP = "10.0.0.5"
START_TIME = "2025-03-06T00:00:00.000Z"
END_TIME = "2025-03-06T23:59:59.999Z"

def query_zeek_conn_logs():
    """Query Zeek connection logs for a specific host and save to JSON"""
    print("Fetching Zeek connection logs...")
    
    # Updated query based on the sample data structure
    query = {
        "size": 1000,
        "sort": [
            {"@timestamp": {"order": "desc"}}  # Most recent first
        ],
        "query": {
            "bool": {
                "must": [
                    # Either source or destination could be our host
                    {
                        "bool": {
                            "should": [
                                {"term": {"conn.id.orig_h": HOST_IP}},
                                {"term": {"conn.id.resp_h": HOST_IP}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    # Look for Zeek connection logs
                    {"term": {"log_type": "zeek"}},
                    {"term": {"filter_match_status": "conn"}},
                    {"range": {"@timestamp": {"gte": START_TIME, "lte": END_TIME}}}
                ]
            }
        }
    }
    
    # Send request to Elasticsearch
    print(f"Searching for Zeek conn logs with host IP {HOST_IP} from {START_TIME} to {END_TIME}")
    response = requests.post(
        f"{ELASTIC_URL}/_search",
        json=query,
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    
    print(f"Response status code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        with open("zeek_conn_logs.json", "w") as f:
            json.dump(result, f, indent=4)
        print(f"Response saved to zeek_conn_logs.json")
        
        # Print hit count
        hit_count = result.get('hits', {}).get('total', {}).get('value', 0)
        print(f"Total Zeek connection log hits: {hit_count}")
        
        # Print sample data from first hit if available
        hits = result.get('hits', {}).get('hits', [])
        if hits:
            print("\nSample data from first Zeek conn log:")
            source = hits[0].get('_source', {})
            
            # Display key connection information from the conn field
            conn = source.get('conn', {})
            timestamp = source.get('@timestamp', 'N/A')
            
            if conn:
                conn_id = conn.get('id', {})
                orig_h = conn_id.get('orig_h', 'N/A')
                orig_p = conn_id.get('orig_p', 'N/A')
                resp_h = conn_id.get('resp_h', 'N/A')
                resp_p = conn_id.get('resp_p', 'N/A')
                proto = conn.get('proto', 'N/A')
                duration = conn.get('duration', 'N/A')
                orig_bytes = conn.get('orig_bytes', 'N/A')
                resp_bytes = conn.get('resp_bytes', 'N/A')
                conn_state = conn.get('conn_state', 'N/A')
                
                print(f"Timestamp: {timestamp}")
                print(f"Connection: {orig_h}:{orig_p} -> {resp_h}:{resp_p} ({proto})")
                print(f"Duration: {duration} seconds")
                print(f"Bytes: orig={orig_bytes}, resp={resp_bytes}")
                print(f"State: {conn_state}")
            else:
                print("Warning: Conn field not found in the log")
                print("Raw source data:")
                print(json.dumps(source, indent=2))
        else:
            print("\nNo Zeek connection logs found. Possible reasons:")
            print("1. Check if the date range includes active times")
            print("2. Verify if the host IP has any logged connections")
            print("3. Check for possible index pattern issues")
            
            # Print the query for debugging
            print("\nQuery used:")
            print(json.dumps(query, indent=2))
    else:
        print(f"Error: {response.text}")
    
    return result if response.status_code == 200 else None

if __name__ == "__main__":
    print("Analyzing Zeek connection logs...")
    query_zeek_conn_logs()