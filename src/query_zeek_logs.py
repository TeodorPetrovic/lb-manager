import requests
import json
import datetime
import pandas as pd
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

def analyze_connection_durations():
    """Analyze connection durations from Zeek logs and export to Excel"""
    print("Analyzing connection durations...")
    
    # First, get the Zeek connection logs
    result = query_zeek_conn_logs()
    
    if not result:
        print("No connection logs to analyze.")
        return
    
    # Create a dictionary to store connection data grouped by remote IP
    connection_data = {}
    
    # Process each connection log
    hits = result.get('hits', {}).get('hits', [])
    for hit in hits:
        source = hit.get('_source', {})
        conn = source.get('conn', {})
        
        if conn:
            # Get connection details
            conn_id = conn.get('id', {})
            orig_h = conn_id.get('orig_h')
            resp_h = conn_id.get('resp_h')
            duration = conn.get('duration')
            
            # Skip entries without duration information
            if duration is None:
                continue
            
            # Determine which IP is remote (not our host IP)
            if orig_h == HOST_IP:
                remote_ip = resp_h
                direction = "outgoing"
            elif resp_h == HOST_IP:
                remote_ip = orig_h
                direction = "incoming"
            else:
                # Should not happen with our query, but just in case
                continue
            
            # Initialize entry for this remote IP if it doesn't exist
            if remote_ip not in connection_data:
                connection_data[remote_ip] = {
                    'ip': remote_ip,
                    'total_duration': 0,
                    'count': 0,
                    'min_duration': float('inf'),
                    'max_duration': 0,
                    'incoming_count': 0,
                    'outgoing_count': 0,
                    'orig_bytes_total': 0,
                    'resp_bytes_total': 0
                }
            
            # Update connection statistics
            connection_data[remote_ip]['total_duration'] += duration
            connection_data[remote_ip]['count'] += 1
            connection_data[remote_ip]['min_duration'] = min(connection_data[remote_ip]['min_duration'], duration)
            connection_data[remote_ip]['max_duration'] = max(connection_data[remote_ip]['max_duration'], duration)
            
            # Add bytes transferred when available
            orig_bytes = conn.get('orig_bytes', 0) or 0
            resp_bytes = conn.get('resp_bytes', 0) or 0
            connection_data[remote_ip]['orig_bytes_total'] += orig_bytes
            connection_data[remote_ip]['resp_bytes_total'] += resp_bytes
            
            # Update direction counts
            if direction == "incoming":
                connection_data[remote_ip]['incoming_count'] += 1
            else:
                connection_data[remote_ip]['outgoing_count'] += 1
    
    # Calculate averages and prepare data for Excel
    excel_data = []
    for remote_ip, data in connection_data.items():
        if data['count'] > 0:
            avg_duration = data['total_duration'] / data['count']
            # Convert to milliseconds
            avg_duration_ms = avg_duration * 1000
            min_duration_ms = data['min_duration'] * 1000 if data['min_duration'] != float('inf') else 0
            max_duration_ms = data['max_duration'] * 1000
            
            excel_data.append({
                'Remote IP': remote_ip,
                'Connection Count': data['count'],
                'Avg Duration (ms)': round(avg_duration_ms, 2),
                'Min Duration (ms)': round(min_duration_ms, 2),
                'Max Duration (ms)': round(max_duration_ms, 2),
                'Incoming Connections': data['incoming_count'],
                'Outgoing Connections': data['outgoing_count'],
                'Bytes Sent': data['orig_bytes_total'],
                'Bytes Received': data['resp_bytes_total']
            })
    
    # Sort by average duration (descending)
    excel_data.sort(key=lambda x: x['Avg Duration (ms)'], reverse=True)
    
    # Create DataFrame and export to Excel
    if excel_data:
        df = pd.DataFrame(excel_data)
        
        # Calculate overall average across all connections
        overall_avg = df['Avg Duration (ms)'].mean()
        total_connections = df['Connection Count'].sum()
        
        # Create summary row
        summary = pd.DataFrame([{
            'Remote IP': 'AVERAGE/TOTAL',
            'Connection Count': total_connections,
            'Avg Duration (ms)': round(overall_avg, 2),
            'Min Duration (ms)': df['Min Duration (ms)'].min(),
            'Max Duration (ms)': df['Max Duration (ms)'].max(),
            'Incoming Connections': df['Incoming Connections'].sum(),
            'Outgoing Connections': df['Outgoing Connections'].sum(),
            'Bytes Sent': df['Bytes Sent'].sum(),
            'Bytes Received': df['Bytes Received'].sum()
        }])
        
        # Add summary to the dataframe
        df = pd.concat([df, summary])
        
        # Export to Excel
        excel_file = f"zeek_connection_durations_{HOST_IP.replace('.', '_')}.xlsx"
        writer = pd.ExcelWriter(excel_file, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='Connection Durations')
        
        # Auto-adjust columns width
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['Connection Durations'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
        
        writer.close()
        
        print(f"\nExported connection duration analysis to {excel_file}")
        print(f"\nConnection Summary for {HOST_IP}:")
        print(f"  Total Connections: {total_connections}")
        print(f"  Overall Average Duration: {overall_avg:.2f} ms")
        print(f"  Total Incoming Connections: {df['Incoming Connections'].sum()}")
        print(f"  Total Outgoing Connections: {df['Outgoing Connections'].sum()}")
    else:
        print("No valid connection data found for analysis.")

# Update the main execution part
if __name__ == "__main__":
    print("Analyzing Zeek connection logs...")
    # query_zeek_conn_logs()
    print("\nCalculating connection durations...")
    analyze_connection_durations()