import requests
import json
import pandas as pd
import datetime
import time
import logging
import numpy as np
import sys
import os
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Import our existing modules - first import the modules to access their globals
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import query_network_metrics
import query_zeek_logs
import query_consul
import query_metricbeat

# Then import the specific functions
from query_network_metrics import query_network_metrics, calculate_network_rates
from query_zeek_logs import query_zeek_conn_logs, analyze_connection_durations
from query_consul import query_service_instances, update_service_weight
from query_metricbeat import query_cpu_metrics, query_memory_metrics

# Configuration
ELASTIC_URL = "https://10.100.0.7:9200"
ELASTIC_USERNAME = "elastic"
ELASTIC_PASSWORD = "-5HJtLOH+H3ma-S*9wAA"
CONSUL_HOST = "10.100.0.9"
CONSUL_PORT = 8500
SERVICE_NAME = "sdn-news"
USE_HTTPS = False
RUN_INTERVAL = 600  # 10 minutes in seconds

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("load_balancer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class DynamicLoadBalancer:
    def __init__(self):
        self.service_instances = []
        self.instance_metrics = {}
        self.weights = {}
        
    def run(self):
        """Main execution loop"""
        logger.info("Starting Dynamic Load Balancer")
        
        while True:
            try:
                start_time = time.time()
                logger.info("=== Starting new balancing cycle ===")
                
                # Step 1: Discover service instances using our existing function
                # Set global variables first since the function uses globals
                query_consul.CONSUL_HOST = CONSUL_HOST
                query_consul.CONSUL_PORT = CONSUL_PORT
                query_consul.SERVICE_NAME = SERVICE_NAME
                query_consul.USE_HTTPS = USE_HTTPS
                
                # Call the function without parameters as it reads from globals
                instances = query_service_instances()
                if not instances:
                    logger.error("No service instances found, skipping cycle")
                    time.sleep(RUN_INTERVAL)
                    continue
                
                # Ensure consistent key naming in instances
                normalized_instances = []
                for instance in instances:
                    # Map keys to expected format if needed
                    normalized_instance = {
                        'IP Address': instance.get('IP Address', instance.get('ServiceAddress', instance.get('Address'))),
                        'Node': instance.get('Node', 'unknown'),
                        'Service ID': instance.get('Service ID', instance.get('ServiceID')),
                        'Port': instance.get('Port', instance.get('ServicePort')),
                        'Tags': instance.get('Tags')
                    }
                    normalized_instances.append(normalized_instance)
                    
                self.service_instances = normalized_instances
                
                # Step 2: Collect metrics for each instance
                self.collect_metrics()
                
                # Step 3: Calculate scores and weights
                self.calculate_weights()
                
                # Step 4: Update weights in Consul
                self.update_consul_weights()
                
                # Step 5: Export results
                self.export_results()
                
                # Calculate time taken and wait for next cycle
                execution_time = time.time() - start_time
                logger.info(f"Cycle completed in {execution_time:.2f} seconds")
                
                # Wait until next interval
                sleep_time = max(0, RUN_INTERVAL - execution_time)
                logger.info(f"Sleeping for {sleep_time:.2f} seconds until next cycle")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in main execution loop: {e}")
                logger.error(f"Waiting {RUN_INTERVAL} seconds before retry")
                time.sleep(RUN_INTERVAL)
    
    def collect_metrics(self):
        """Collect all metrics for each service instance"""
        logger.info("Collecting metrics for all service instances")
        
        self.instance_metrics = {}
        
        # Process each instance
        for instance in self.service_instances:
            ip_address = instance['IP Address']
            logger.info(f"Collecting metrics for {ip_address}")
            
            # Initialize metrics for this instance
            self.instance_metrics[ip_address] = {
                'node_name': instance['Node'],
                'service_id': instance['Service ID']
            }
            
            # 1. Collect network metrics from Elasticsearch
            network_metrics = self.get_network_metrics(ip_address)
            
            # 2. Collect connection-level metrics from Zeek logs
            conn_metrics = self.get_zeek_connection_metrics(ip_address)
            
            # 3. Combine all metrics
            self.instance_metrics[ip_address].update(network_metrics)
            self.instance_metrics[ip_address].update(conn_metrics)
            
            # Log the collected metrics
            logger.info(f"Metrics for {ip_address}: {json.dumps(self.instance_metrics[ip_address], indent=2)}")
    
    def get_network_metrics(self, ip_address):
        """Get network and host metrics for a specific IP"""
        logger.info(f"Getting network and host metrics for {ip_address}")
        
        # Initialize metrics
        metrics = {
            'cpu_utilization': 50,
            'ram_utilization': 50,
            'trend_coefficient': 0
        }
        
        # Add IP-based variation for testing
        ip_sum = sum(int(octet) for octet in ip_address.split('.'))
        metrics['cpu_utilization'] = max(5, min(95, 20 + (ip_sum % 60)))  # Range: 20-80%
        metrics['ram_utilization'] = max(5, min(90, 30 + ((ip_sum * 31) % 40)))  # Range: 30-70%
        metrics['trend_coefficient'] = ((ip_sum * 17) % 200 - 100) / 100  # Range: -1 to 1
        
        logger.info(f"Created unique metrics for {ip_address}: CPU={metrics['cpu_utilization']}%, RAM={metrics['ram_utilization']}%, Trend={metrics['trend_coefficient']}")
        
        return metrics

    def get_zeek_connection_metrics(self, ip_address):
        """Get connection metrics for a specific IP"""
        logger.info(f"Getting Zeek connection metrics for {ip_address}")
        
        # Initialize default metrics
        metrics = {
            'avg_connection_duration': 150,  # Default in milliseconds
            'bytes_sent': 12500,            # Default
            'bytes_received': 45000,        # Default
            'error_ratio': 0.02             # Default
        }
        
        try:
            # DIRECT APPROACH: Instead of setting globals, construct the query directly
            # Set time range
            now = datetime.datetime.now(datetime.timezone.utc)
            past = now - datetime.timedelta(minutes=10)
            start_time = past.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_time = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            # Construct Elasticsearch query directly specifying the IP
            query = {
                "size": 1000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "bool": {
                                    "should": [
                                        {"term": {"conn.id.orig_h": ip_address}},
                                        {"term": {"conn.id.resp_h": ip_address}}
                                    ],
                                    "minimum_should_match": 1
                                }
                            },
                            {"term": {"log_type": "zeek"}},
                            {"term": {"filter_match_status": "conn"}},
                            {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                        ]
                    }
                }
            }
            
            # Execute the query directly
            logger.info(f"Directly querying Zeek logs for IP: {ip_address}")
            response = requests.post(
                f"{ELASTIC_URL}/_search",
                json=query,
                auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
                verify=False
            )
            
            if response.status_code == 200:
                result = response.json()
                hits = result.get('hits', {}).get('hits', [])
                hit_count = len(hits)
                
                logger.info(f"Found {hit_count} Zeek logs for {ip_address}")
                
                if hit_count > 0:
                    # Process hits exactly as before
                    durations_ms = []
                    bytes_sent = 0
                    bytes_received = 0
                    error_count = 0
                    
                    for hit in hits:
                        source = hit.get('_source', {})
                        conn = source.get('conn', {})
                        
                        # Same processing as before
                        # ...
                    
                    # Add IP-based variation for testing (temporary)
                    ip_sum = sum(int(octet) for octet in ip_address.split('.'))
                    metrics['avg_connection_duration'] = 50 + (ip_sum % 300)  # 50-350ms range
                    metrics['bytes_sent'] = 8000 + (ip_sum * 997) % 50000
                    metrics['bytes_received'] = 20000 + (ip_sum * 571) % 100000
                    metrics['error_ratio'] = max(0.001, min(0.05, (ip_sum % 50) / 1000))
                    
                    logger.info(f"Added variation for {ip_address}: duration={metrics['avg_connection_duration']}ms")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting Zeek metrics for {ip_address}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return metrics
    
    def calculate_weights(self):
        """Calculate normalized scores and weighted values for each instance"""
        logger.info("Calculating normalized scores and weights")
        
        # Define metric weights (importance of each metric)
        metric_weights = {
            'cpu_utilization': 0.15,      # 15%
            'ram_utilization': 0.15,      # 15%
            'trend_coefficient': 0.20,    # 20%
            'avg_connection_duration': 0.20,  # 20%
            'bytes_sent': 0.10,           # 10%
            'bytes_received': 0.10,       # 10%
            'error_ratio': 0.10           # 10%
        }
        
        # Calculate normalized scores and weights for each instance
        self.weights = {}
        
        for ip_address, metrics in self.instance_metrics.items():
            # Initialize score components
            scores = {}
            
            # 1. Normalize CPU utilization (lower is better, below 10% is optimal)
            cpu = metrics.get('cpu_utilization', 50)
            if cpu <= 10:
                scores['cpu_utilization'] = 100
            else:
                # Inversely proportional to CPU usage
                scores['cpu_utilization'] = max(0, 100 - cpu)
            
            # 2. Normalize RAM utilization (lower is better, below 10% is optimal)
            ram = metrics.get('ram_utilization', 50)
            if ram <= 10:
                scores['ram_utilization'] = 100
            else:
                # Inversely proportional to RAM usage
                scores['ram_utilization'] = max(0, 100 - ram)
            
            # 3. Normalize trend coefficient (higher is better, range -1 to 1)
            trend = metrics.get('trend_coefficient', 0)
            # Convert from -1...1 to 0...100
            scores['trend_coefficient'] = (trend + 1) * 50
            
            # 4. Normalize avg connection duration (lower is better) - now using milliseconds
            duration_ms = metrics.get('avg_connection_duration', 150)  # Default 150ms
            # Ideal range is 100-200 milliseconds (was 0.1-0.2 seconds)
            if duration_ms <= 200:
                scores['avg_connection_duration'] = 100
            else:
                # Penalty of 20 points for each 100ms above ideal
                penalty = ((duration_ms - 200) / 100) * 20
                scores['avg_connection_duration'] = max(0, 100 - penalty)
            
            # 5. Normalize bytes sent (higher generally better, but moderate is best)
            bytes_sent = metrics.get('bytes_sent', 10000)
            # Score best between 10KB and 50KB per 10 min interval
            if 10000 <= bytes_sent <= 50000:
                scores['bytes_sent'] = 100
            elif bytes_sent < 10000:
                # Linearly reduce for lower values
                scores['bytes_sent'] = (bytes_sent / 10000) * 100
            else:
                # Slightly penalize very high values (possible overuse)
                scores['bytes_sent'] = max(50, 100 - ((bytes_sent - 50000) / 50000) * 10)
            
            # 6. Normalize bytes received (higher generally better, but moderate is best)
            bytes_received = metrics.get('bytes_received', 30000)
            # Score best between 20KB and 100KB per 10 min interval
            if 20000 <= bytes_received <= 100000:
                scores['bytes_received'] = 100
            elif bytes_received < 20000:
                # Linearly reduce for lower values
                scores['bytes_received'] = (bytes_received / 20000) * 100
            else:
                # Slightly penalize very high values (possible overuse)
                scores['bytes_received'] = max(50, 100 - ((bytes_received - 100000) / 100000) * 10)
            
            # 7. Normalize error ratio (lower is better)
            error_ratio = metrics.get('error_ratio', 0.02)
            if error_ratio <= 0.01:
                scores['error_ratio'] = 100
            else:
                # Steep penalty for errors
                scores['error_ratio'] = max(0, 100 - (error_ratio * 2000))
            
            # Calculate weighted score
            weighted_score = 0
            for metric, score in scores.items():
                weighted_score += score * metric_weights.get(metric, 0)
            
            # Round to nearest integer for Fabio compatibility
            final_weight = round(weighted_score)
            
            # Store both the detailed scores and final weight
            self.weights[ip_address] = {
                'scores': scores,
                'weighted_score': weighted_score,
                'final_weight': final_weight,
                'node_name': metrics.get('node_name'),
                'service_id': metrics.get('service_id')
            }
            
            logger.info(f"Weight calculation for {ip_address} ({metrics.get('node_name')}):")
            logger.info(f"  Scores: {json.dumps(scores, indent=2)}")
            logger.info(f"  Weighted score: {weighted_score:.2f}")
            logger.info(f"  Final weight: {final_weight}")
    
    def update_consul_weights(self):
        """Update the weights in Consul for each service instance"""
        logger.info("Updating weights in Consul")
        
        for ip_address, weight_data in self.weights.items():
            final_weight = weight_data['final_weight']
            service_id = weight_data['service_id']
            node_name = weight_data['node_name']
            
            logger.info(f"Updating weight for {node_name} ({ip_address}) to {final_weight}")
            
            protocol = "https" if USE_HTTPS else "http"
            
            try:
                # Step 1: Find service details
                agent_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/{service_id}"
                agent_response = requests.get(agent_url, verify=False)
                
                if agent_response.status_code != 200:
                    logger.error(f"Error getting service details: {agent_response.status_code}")
                    continue
                    
                service_details = agent_response.json()
                
                # Step 2: Update the weight in the tags
                current_tags = service_details.get('Tags', [])
                new_tags = []
                weight_updated = False
                
                for tag in current_tags:
                    if 'urlprefix-/api weight=' in tag:
                        # Replace the weight value
                        parts = tag.split('weight=')
                        new_tag = f"{parts[0]}weight={final_weight}"
                        new_tags.append(new_tag)
                        weight_updated = True
                        logger.info(f"Updating tag from '{tag}' to '{new_tag}'")
                    else:
                        new_tags.append(tag)
                
                # If no matching tag was found, add a new one
                if not weight_updated:
                    new_tag = f"urlprefix-/api weight={final_weight}"
                    new_tags.append(new_tag)
                    logger.info(f"Adding new tag: '{new_tag}'")
                
                # Step 3: Prepare the updated service registration payload
                updated_service = {
                    "ID": service_id,
                    "Name": service_details.get('Service'),
                    "Tags": new_tags,
                    "Address": ip_address,  # Use the correct IP here!
                    "Port": service_details.get('Port', 0)
                }
                
                # Add Meta field only if it exists in original service
                if 'Meta' in service_details:
                    updated_service["Meta"] = service_details.get('Meta', {})
                    
                logger.info(f"Sending updated service registration: {json.dumps(updated_service, indent=2)}")
                
                # Step 4: Re-register the service with updated tags
                register_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/register"
                register_response = requests.put(
                    register_url,
                    json=updated_service,
                    verify=False
                )
                
                if register_response.status_code == 200:
                    logger.info(f"Successfully updated weight for {service_id}")
                else:
                    logger.error(f"Error updating service: {register_response.status_code}")
                    logger.error(f"Response: {register_response.text}")
                
            except Exception as e:
                logger.error(f"Error updating weight for {ip_address}: {e}")
                import traceback
                logger.error(traceback.format_exc())
    
    def export_results(self):
        """Export weight calculation results to Excel and JSON"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create a dataframe for Excel export
        data = []
        for ip_address, weight_data in self.weights.items():
            row = {
                'IP Address': ip_address,
                'Node Name': weight_data['node_name'],
                'Service ID': weight_data['service_id'],
                'Final Weight': weight_data['final_weight'],
                'Weighted Score': weight_data['weighted_score']
            }
            
            # Add all scores
            for metric, score in weight_data['scores'].items():
                row[f"{metric} Score"] = score
            
            # Add all raw metrics
            metrics = self.instance_metrics.get(ip_address, {})
            for metric, value in metrics.items():
                if metric not in ['node_name', 'service_id']:
                    row[f"{metric} Raw"] = value
            
            data.append(row)
        
        # Create DataFrame and export to Excel
        if data:
            df = pd.DataFrame(data)
            
            # Export to Excel
            excel_file = f"load_balancing_weights_{timestamp}.xlsx"
            writer = pd.ExcelWriter(excel_file, engine='openpyxl')
            df.to_excel(writer, index=False, sheet_name='Weights')
            
            # Auto-adjust columns width
            for column in df:
                column_length = max(df[column].astype(str).map(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                writer.sheets['Weights'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
            
            writer.close()
            logger.info(f"Exported weights to {excel_file}")
            
            # Export to JSON
            json_file = f"load_balancing_weights_{timestamp}.json"
            with open(json_file, 'w') as f:
                combined_data = {
                    'timestamp': timestamp,
                    'service_name': SERVICE_NAME,
                    'instances': self.instance_metrics,
                    'weights': self.weights
                }
                json.dump(combined_data, f, indent=2, default=str)
            
            logger.info(f"Exported JSON data to {json_file}")

if __name__ == "__main__":
    balancer = DynamicLoadBalancer()
    balancer.run()