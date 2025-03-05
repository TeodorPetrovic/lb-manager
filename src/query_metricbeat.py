import requests
import json
from urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import datetime
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Hardcoded properties
ELASTIC_URL = "https://10.100.0.7:9200"
USERNAME = "elastic"
PASSWORD = "-5HJtLOH+H3ma-S*9wAA"

# Host IP and time range
HOST_IP = "10.0.0.5"
START_TIME = "2025-02-23T18:00:00+00:00"
END_TIME = "2025-02-23T18:10:00+00:00"

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

def export_cpu_metrics_to_excel():
    print("Fetching CPU metrics for Excel export...")
    query = {
        "size": 1000,  # Get more results for better data set
        "sort": [
            {"@timestamp": {"order": "asc"}}  # Sort by timestamp ascending
        ],
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
    response = requests.post(
        f"{ELASTIC_URL}/_search",
        json=query,
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    
    if response.status_code == 200:
        result = response.json()
        hits = result.get('hits', {}).get('hits', [])
        
        if not hits:
            print("No CPU metrics found.")
            return
        
        # Extract data for Excel
        data = []
        for hit in hits:
            source = hit.get('_source', {})
            timestamp = source.get('@timestamp', 'N/A')
            
            # Convert UTC ISO timestamp to a more readable format
            try:
                dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                formatted_time = timestamp
            
            # Get CPU metrics
            cpu_data = source.get('system', {}).get('cpu', {})
            total_pct = cpu_data.get('total', {}).get('pct', None)
            user_pct = cpu_data.get('user', {}).get('pct', None)
            system_pct = cpu_data.get('system', {}).get('pct', None)
            
            # Convert to percentage for readability
            if total_pct is not None:
                total_pct = total_pct * 100
            if user_pct is not None:
                user_pct = user_pct * 100
            if system_pct is not None:
                system_pct = system_pct * 100
            
            # Add data row
            data.append({
                'Timestamp': formatted_time,
                'Total CPU (%)': total_pct,
                'User CPU (%)': user_pct,
                'System CPU (%)': system_pct
            })
        
        # Create DataFrame and save to Excel
        df = pd.DataFrame(data)
        
        # Calculate averages
        avg_total = df['Total CPU (%)'].mean()
        avg_user = df['User CPU (%)'].mean()
        avg_system = df['System CPU (%)'].mean()
        
        # Add summary row to the dataframe
        summary_df = pd.DataFrame([{
            'Timestamp': 'AVERAGE',
            'Total CPU (%)': avg_total,
            'User CPU (%)': avg_user,
            'System CPU (%)': avg_system
        }])
        
        # Append summary to the main dataframe
        df = pd.concat([df, summary_df])
        
        # Export to Excel
        excel_file = f"cpu_metrics_{HOST_IP.replace('.', '_')}.xlsx"
        writer = pd.ExcelWriter(excel_file, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='CPU Metrics')
        
        # Auto-adjust columns width
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['CPU Metrics'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
        
        writer.close()
        
        print(f"\nExported {len(hits)} CPU metrics to {excel_file}")
        print(f"\nAverage CPU Utilization:")
        print(f"  Total CPU: {avg_total:.2f}%")
        print(f"  User CPU: {avg_user:.2f}%")
        print(f"  System CPU: {avg_system:.2f}%")
    else:
        print(f"Error: {response.text}")

def export_ram_metrics_to_excel():
    print("Fetching RAM metrics for Excel export...")
    # First save raw JSON for inspection
    query_memory_metrics()
    
    query = {
        "size": 1000,
        "sort": [
            {"@timestamp": {"order": "asc"}}
        ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"term": {"metricset.name": "memory"}},
                    {"range": {"@timestamp": {"gte": START_TIME, "lte": END_TIME}}}
                ]
            }
        }
    }
    
    response = requests.post(
        f"{ELASTIC_URL}/_search",
        json=query,
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    
    if response.status_code == 200:
        result = response.json()
        hits = result.get('hits', {}).get('hits', [])
        
        if not hits:
            print("No RAM metrics found.")
            return
        
        # Extract data for Excel
        data = []
        for hit in hits:
            try:
                source = hit.get('_source', {})
                timestamp = source.get('@timestamp', 'N/A')
                
                # Convert UTC ISO timestamp to a more readable format
                try:
                    dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    formatted_time = timestamp
                
                # Get RAM metrics safely with robust error handling
                memory_data = source.get('system', {}).get('memory', {})
                
                # Initialize variables with default values
                ram_pct = None
                total_bytes = None
                used_bytes = None
                free_bytes = None
                
                # Extract data with safe navigation
                if isinstance(memory_data, dict):
                    # Get total bytes - could be at the top level
                    if 'total' in memory_data:
                        if isinstance(memory_data['total'], (int, float)):
                            total_bytes = memory_data['total']
                    
                    # Safe extraction of percentage
                    actual = memory_data.get('actual', {})
                    if isinstance(actual, dict):
                        used_data = actual.get('used', {})
                        if isinstance(used_data, dict):
                            ram_pct = used_data.get('pct')
                            used_bytes = used_data.get('bytes')
                        
                        free_data = actual.get('free', {})
                        if isinstance(free_data, dict):
                            free_bytes = free_data.get('bytes')
                
                # Convert percentage to readable format
                if ram_pct is not None:
                    ram_pct = ram_pct * 100
                
                # Convert bytes to MB for readability
                if total_bytes is not None:
                    total_mb = total_bytes / (1024 * 1024)
                else:
                    total_mb = None
                    
                if used_bytes is not None:
                    used_mb = used_bytes / (1024 * 1024)
                else:
                    used_mb = None
                    
                if free_bytes is not None:
                    free_mb = free_bytes / (1024 * 1024)
                else:
                    free_mb = None
                
                # Add data row
                data.append({
                    'Timestamp': formatted_time,
                    'RAM Used (%)': ram_pct,
                    'Total RAM (MB)': total_mb,
                    'Used RAM (MB)': used_mb,
                    'Free RAM (MB)': free_mb
                })
            except Exception as e:
                print(f"Error processing hit: {e}")
                # Optionally print the hit causing the error for debugging
                # print(json.dumps(hit, indent=2))
        
        if not data:
            print("Could not extract any usable RAM metrics.")
            return
        
        # Create DataFrame and save to Excel
        df = pd.DataFrame(data)
        
        # Calculate averages
        avg_ram_pct = df['RAM Used (%)'].mean() if df['RAM Used (%)'].count() > 0 else None
        avg_used_mb = df['Used RAM (MB)'].mean() if df['Used RAM (MB)'].count() > 0 else None
        avg_free_mb = df['Free RAM (MB)'].mean() if df['Free RAM (MB)'].count() > 0 else None
        
        # Add summary row to the dataframe
        summary_df = pd.DataFrame([{
            'Timestamp': 'AVERAGE',
            'RAM Used (%)': avg_ram_pct,
            'Total RAM (MB)': df['Total RAM (MB)'].dropna().iloc[0] if not df['Total RAM (MB)'].empty and df['Total RAM (MB)'].count() > 0 else None,
            'Used RAM (MB)': avg_used_mb,
            'Free RAM (MB)': avg_free_mb
        }])
        
        # Append summary to the main dataframe
        df = pd.concat([df, summary_df])
        
        # Export to Excel
        excel_file = f"ram_metrics_{HOST_IP.replace('.', '_')}.xlsx"
        writer = pd.ExcelWriter(excel_file, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='RAM Metrics')
        
        # Auto-adjust columns width
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['RAM Metrics'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
        
        writer.close()
        
        print(f"\nExported {len(hits)} RAM metrics to {excel_file}")
        print("\nAverage RAM Utilization:")
        if avg_ram_pct is not None:
            print(f"  RAM Used: {avg_ram_pct:.2f}%")
        if avg_used_mb is not None:
            print(f"  Used RAM: {avg_used_mb:.2f} MB")
        if avg_free_mb is not None:
            print(f"  Free RAM: {avg_free_mb:.2f} MB")
    else:
        print(f"Error: {response.text}")

def query_memory_metrics():
    print("Fetching memory metrics to examine JSON structure...")
    query = {
        "size": 100,
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"term": {"metricset.name": "memory"}},
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
    
    print(f"Response status code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        with open("memory_metrics.json", "w") as f:
            json.dump(result, f, indent=4)
        print(f"Response saved to memory_metrics.json")
        
        # Print hit count
        hit_count = result.get('hits', {}).get('total', {}).get('value', 0)
        print(f"Total memory metric hits: {hit_count}")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    print("Querying metricbeat logs...")
    query_metricbeat()
    print("Querying CPU metrics from metricbeat logs...")
    query_cpu_metrics()
    export_cpu_metrics_to_excel()
    print("\nExporting RAM metrics to Excel...")
    export_ram_metrics_to_excel()