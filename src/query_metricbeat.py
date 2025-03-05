import requests
import json
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Hardcoded properties
ELASTIC_URL = "https://10.100.0.7:9200"
USERNAME = "elastic"
PASSWORD = "-5HJtLOH+H3ma-S*9wAA"

# Host IP and time range
HOST_IP = "10.0.0.5"
START_TIME = "2025-02-23T18:00:00+00:00"
END_TIME = "2025-02-23T20:00:00+00:00"

# Query for metricbeat logs
def query_metricbeat():
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"range": {"@timestamp": {"gte": START_TIME, "lte": END_TIME}}}
                ]
            }
        }
    }
    
    # Send request to Elasticsearch
    response = requests.post(
        f"{ELASTIC_URL}/_search",
        json=query,
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    
    # Print status code
    print(f"Response status code: {response.status_code}")
    
    if response.status_code == 200:
        # Save response to JSON file
        result = response.json()
        with open("metricbeat_response.json", "w") as f:
            json.dump(result, f, indent=4)
        print(f"Response saved to metricbeat_response.json")
        
        # Print hit count
        hit_count = result.get('hits', {}).get('total', {}).get('value', 0)
        print(f"Total hits: {hit_count}")
        
        # Print first hit if available
        hits = result.get('hits', {}).get('hits', [])
        if hits:
            print("\nSample data from first hit:")
            source = hits[0].get('_source', {})
            host_ip = source.get('host', {}).get('ip', 'N/A')
            timestamp = source.get('@timestamp', 'N/A')
            metricset = source.get('metricset', {}).get('name', 'N/A')
            print(f"Host IP: {host_ip}")
            print(f"Timestamp: {timestamp}")
            print(f"Metricset: {metricset}")
    else:
        print(f"Error: {response.text}")

# Query for CPU metrics
def query_cpu_metrics():
    query = {
        "size": 100,  # Get more results (default is usually 10)
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"term": {"metricset.name": "cpu"}},  # Only get CPU metrics
                    {"range": {"@timestamp": {"gte": START_TIME, "lte": END_TIME}}}
                ]
            }
        }
    }
    
    # Send request to Elasticsearch
    print("Sending request to Elasticsearch...")
    response = requests.post(
        f"{ELASTIC_URL}/_search",
        json=query,
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    
    # Print status code
    print(f"Response status code: {response.status_code}")
    
    if response.status_code == 200:
        # Save response to JSON file
        result = response.json()
        with open("cpu_metrics.json", "w") as f:
            json.dump(result, f, indent=4)
        print(f"Response saved to cpu_metrics.json")
        
        # Print hit count
        hit_count = result.get('hits', {}).get('total', {}).get('value', 0)
        print(f"Total CPU metric hits: {hit_count}")
        
        # Print CPU utilization summary if available
        hits = result.get('hits', {}).get('hits', [])
        if hits:
            print("\nCPU Utilization Summary:")
            for i, hit in enumerate(hits[:5]):  # Show first 5 entries
                source = hit.get('_source', {})
                timestamp = source.get('@timestamp', 'N/A')
                
                # Get CPU metrics
                cpu_data = source.get('system', {}).get('cpu', {})
                total_pct = cpu_data.get('total', {}).get('pct', 'N/A')
                user_pct = cpu_data.get('user', {}).get('pct', 'N/A')
                system_pct = cpu_data.get('system', {}).get('pct', 'N/A')
                
                print(f"\nTimestamp: {timestamp}")
                print(f"  Total CPU: {total_pct}")
                print(f"  User CPU: {user_pct}")
                print(f"  System CPU: {system_pct}")
            
            if len(hits) > 5:
                print(f"\n... and {len(hits) - 5} more entries")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    print("Querying metricbeat logs...")
    query_metricbeat()
    print("Querying CPU metrics from metricbeat logs...")
    query_cpu_metrics()