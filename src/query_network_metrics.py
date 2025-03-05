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
END_TIME = "2025-02-23T20:00:00+00:00"

def query_network_metrics():
    """Query and save raw network metrics to a JSON file"""
    print("Fetching network metrics to examine JSON structure...")
    query = {
        "size": 100,
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"term": {"metricset.name": "network"}},
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
        with open("network_metrics.json", "w") as f:
            json.dump(result, f, indent=4)
        print(f"Response saved to network_metrics.json")
        
        # Print hit count
        hit_count = result.get('hits', {}).get('total', {}).get('value', 0)
        print(f"Total network metric hits: {hit_count}")
        
        # Print sample data from first hit
        hits = result.get('hits', {}).get('hits', [])
        if hits:
            print("\nSample data from first network metric:")
            source = hits[0].get('_source', {})
            network_data = source.get('system', {}).get('network', {})
            print(json.dumps(network_data, indent=2))
    else:
        print(f"Error: {response.text}")
    
    return result if response.status_code == 200 else None

def export_network_metrics_to_excel():
    """Process network metrics and export to Excel"""
    print("Fetching network metrics for Excel export...")
    
    # First query and save raw metrics
    query_network_metrics()
    
    query = {
        "size": 1000,
        "sort": [
            {"@timestamp": {"order": "asc"}}
        ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"term": {"metricset.name": "network"}},
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
            print("No network metrics found.")
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
                
                # Get network metrics - need to iterate through network interfaces
                network_data = source.get('system', {}).get('network', {})
                
                # Initialize counters for all interfaces
                bytes_in_total = 0
                bytes_out_total = 0
                packets_in_total = 0
                packets_out_total = 0
                
                # Get interface data
                for interface_name, interface_data in network_data.items():
                    # Skip non-interface entries (like "interface_name")
                    if not isinstance(interface_data, dict):
                        continue
                    
                    # Skip lo (loopback) interface as it's typically not relevant for external traffic
                    if interface_name == 'lo':
                        continue
                    
                    # Extract bytes in/out
                    bytes_in = interface_data.get('in', {}).get('bytes', 0)
                    bytes_out = interface_data.get('out', {}).get('bytes', 0)
                    packets_in = interface_data.get('in', {}).get('packets', 0)
                    packets_out = interface_data.get('out', {}).get('packets', 0)
                    
                    # Add to total
                    bytes_in_total += bytes_in if isinstance(bytes_in, (int, float)) else 0
                    bytes_out_total += bytes_out if isinstance(bytes_out, (int, float)) else 0
                    packets_in_total += packets_in if isinstance(packets_in, (int, float)) else 0
                    packets_out_total += packets_out if isinstance(packets_out, (int, float)) else 0
                
                # Convert bytes to MB for readability
                bytes_in_mb = bytes_in_total / (1024 * 1024) if bytes_in_total else 0
                bytes_out_mb = bytes_out_total / (1024 * 1024) if bytes_out_total else 0
                
                # Add data row
                data.append({
                    'Timestamp': formatted_time,
                    'Bytes In (MB)': bytes_in_mb,
                    'Bytes Out (MB)': bytes_out_mb,
                    'Packets In': packets_in_total,
                    'Packets Out': packets_out_total
                })
            except Exception as e:
                print(f"Error processing hit: {e}")
        
        if not data:
            print("Could not extract any usable network metrics.")
            return
        
        # Create DataFrame and save to Excel
        df = pd.DataFrame(data)
        
        # Calculate averages
        avg_bytes_in = df['Bytes In (MB)'].mean() if df['Bytes In (MB)'].count() > 0 else None
        avg_bytes_out = df['Bytes Out (MB)'].mean() if df['Bytes Out (MB)'].count() > 0 else None
        avg_packets_in = df['Packets In'].mean() if df['Packets In'].count() > 0 else None
        avg_packets_out = df['Packets Out'].mean() if df['Packets Out'].count() > 0 else None
        
        # Add summary row to the dataframe
        summary_df = pd.DataFrame([{
            'Timestamp': 'AVERAGE',
            'Bytes In (MB)': avg_bytes_in,
            'Bytes Out (MB)': avg_bytes_out,
            'Packets In': avg_packets_in,
            'Packets Out': avg_packets_out
        }])
        
        # Calculate totals
        total_bytes_in = df['Bytes In (MB)'].sum() if df['Bytes In (MB)'].count() > 0 else None
        total_bytes_out = df['Bytes Out (MB)'].sum() if df['Bytes Out (MB)'].count() > 0 else None
        total_packets_in = df['Packets In'].sum() if df['Packets In'].count() > 0 else None
        total_packets_out = df['Packets Out'].sum() if df['Packets Out'].count() > 0 else None
        
        # Add total row
        total_df = pd.DataFrame([{
            'Timestamp': 'TOTAL',
            'Bytes In (MB)': total_bytes_in,
            'Bytes Out (MB)': total_bytes_out,
            'Packets In': total_packets_in,
            'Packets Out': total_packets_out
        }])
        
        # Append summary rows to the main dataframe
        df = pd.concat([df, summary_df, total_df])
        
        # Export to Excel
        excel_file = f"network_metrics_{HOST_IP.replace('.', '_')}.xlsx"
        writer = pd.ExcelWriter(excel_file, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='Network Metrics')
        
        # Auto-adjust columns width
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['Network Metrics'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
        
        writer.close()
        
        print(f"\nExported {len(hits)} network metrics to {excel_file}")
        print("\nNetwork Traffic Summary:")
        if avg_bytes_in is not None:
            print(f"  Average Bytes In: {avg_bytes_in:.2f} MB")
        if avg_bytes_out is not None:
            print(f"  Average Bytes Out: {avg_bytes_out:.2f} MB")
        if total_bytes_in is not None:
            print(f"  Total Bytes In: {total_bytes_in:.2f} MB")
        if total_bytes_out is not None:
            print(f"  Total Bytes Out: {total_bytes_out:.2f} MB")
    else:
        print(f"Error: {response.text}")

def calculate_network_rates():
    """Calculate network transfer rates (MB/s) between points"""
    print("Calculating network transfer rates...")
    
    query = {
        "size": 1000,
        "sort": [
            {"@timestamp": {"order": "asc"}}
        ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"host.ip": HOST_IP}},
                    {"term": {"metricset.name": "network"}},
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
        
        if len(hits) < 2:
            print("Not enough data points to calculate rates.")
            return
        
        # Process data to calculate rates
        timestamps = []
        bytes_in_values = []
        bytes_out_values = []
        
        for hit in hits:
            try:
                source = hit.get('_source', {})
                timestamp_str = source.get('@timestamp', None)
                if not timestamp_str:
                    continue
                
                # Parse timestamp
                timestamp = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                
                # Get network metrics - sum across all interfaces
                network_data = source.get('system', {}).get('network', {})
                
                bytes_in_total = 0
                bytes_out_total = 0
                
                for interface_name, interface_data in network_data.items():
                    if not isinstance(interface_data, dict) or interface_name == 'lo':
                        continue
                    
                    bytes_in = interface_data.get('in', {}).get('bytes', 0)
                    bytes_out = interface_data.get('out', {}).get('bytes', 0)
                    
                    bytes_in_total += bytes_in if isinstance(bytes_in, (int, float)) else 0
                    bytes_out_total += bytes_out if isinstance(bytes_out, (int, float)) else 0
                
                timestamps.append(timestamp)
                bytes_in_values.append(bytes_in_total)
                bytes_out_values.append(bytes_out_total)
                
            except Exception as e:
                print(f"Error processing hit for rate calculation: {e}")
        
        # Calculate rates between consecutive measurements
        rates_data = []
        for i in range(1, len(timestamps)):
            time_diff = (timestamps[i] - timestamps[i-1]).total_seconds()
            
            if time_diff > 0:
                # Calculate bytes/second
                bytes_in_rate = (bytes_in_values[i] - bytes_in_values[i-1]) / time_diff
                bytes_out_rate = (bytes_out_values[i] - bytes_out_values[i-1]) / time_diff
                
                # Convert to MB/s
                mb_in_rate = bytes_in_rate / (1024 * 1024)
                mb_out_rate = bytes_out_rate / (1024 * 1024)
                
                rates_data.append({
                    'Timestamp': timestamps[i].strftime('%Y-%m-%d %H:%M:%S'),
                    'In Rate (MB/s)': mb_in_rate if mb_in_rate > 0 else 0,
                    'Out Rate (MB/s)': mb_out_rate if mb_out_rate > 0 else 0
                })
        
        if rates_data:
            # Create DataFrame
            rates_df = pd.DataFrame(rates_data)
            
            # Calculate average rates
            avg_in_rate = rates_df['In Rate (MB/s)'].mean()
            avg_out_rate = rates_df['Out Rate (MB/s)'].mean()
            max_in_rate = rates_df['In Rate (MB/s)'].max()
            max_out_rate = rates_df['Out Rate (MB/s)'].max()
            
            # Add summary row
            summary = pd.DataFrame([{
                'Timestamp': 'AVERAGE',
                'In Rate (MB/s)': avg_in_rate,
                'Out Rate (MB/s)': avg_out_rate
            }, {
                'Timestamp': 'MAX',
                'In Rate (MB/s)': max_in_rate,
                'Out Rate (MB/s)': max_out_rate
            }])
            
            rates_df = pd.concat([rates_df, summary])
            
            # Save to Excel
            excel_file = f"network_rates_{HOST_IP.replace('.', '_')}.xlsx"
            writer = pd.ExcelWriter(excel_file, engine='openpyxl')
            rates_df.to_excel(writer, index=False, sheet_name='Network Rates')
            
            # Auto-adjust columns
            for column in rates_df:
                column_length = max(rates_df[column].astype(str).map(len).max(), len(column))
                col_idx = rates_df.columns.get_loc(column)
                writer.sheets['Network Rates'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
            
            writer.close()
            
            print(f"\nExported {len(rates_data)} network rate calculations to {excel_file}")
            print("\nNetwork Rate Summary:")
            print(f"  Average In Rate: {avg_in_rate:.4f} MB/s")
            print(f"  Average Out Rate: {avg_out_rate:.4f} MB/s")
            print(f"  Maximum In Rate: {max_in_rate:.4f} MB/s")
            print(f"  Maximum Out Rate: {max_out_rate:.4f} MB/s")
        else:
            print("Could not calculate any valid network rates.")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    print("Analyzing network traffic metrics...")
    export_network_metrics_to_excel()
    calculate_network_rates()