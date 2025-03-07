import requests
import json
import pandas as pd
import datetime
import time
import logging
import numpy as np
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

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
                
                # Step 1: Discover service instances
                self.discover_service_instances()
                
                if not self.service_instances:
                    logger.error("No service instances found, skipping cycle")
                    time.sleep(RUN_INTERVAL)
                    continue
                
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
    
    def discover_service_instances(self):
        """Query Consul for all instances of the service"""
        logger.info("Discovering service instances from Consul")
        
        protocol = "https" if USE_HTTPS else "http"
        consul_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/catalog/service/{SERVICE_NAME}"
        
        try:
            response = requests.get(consul_url, verify=False)
            
            if response.status_code == 200:
                services = response.json()
                logger.info(f"Found {len(services)} service instances")
                
                # Process service instances
                self.service_instances = []
                for instance in services:
                    service_address = instance.get('ServiceAddress') or instance.get('Address')
                    service_id = instance.get('ServiceID')
                    node_name = instance.get('Node')
                    
                    self.service_instances.append({
                        'node_name': node_name,
                        'ip_address': service_address,
                        'service_id': service_id
                    })
                    logger.info(f"Instance: {service_id} at {service_address}")
            else:
                logger.error(f"Error getting service instances: {response.status_code}")
        except Exception as e:
            logger.error(f"Error discovering service instances: {e}")
    
    def collect_metrics(self):
        """Collect all metrics for each service instance"""
        logger.info("Collecting metrics for all service instances")
        
        self.instance_metrics = {}
        
        # Process each instance
        for instance in self.service_instances:
            ip_address = instance['ip_address']
            logger.info(f"Collecting metrics for {ip_address}")
            
            # Initialize metrics for this instance
            self.instance_metrics[ip_address] = {
                'node_name': instance['node_name'],
                'service_id': instance['service_id']
            }
            
            # 1. Collect host-level metrics (CPU, RAM)
            host_metrics = self.get_host_metrics(ip_address)
            
            # 2. Collect connection-level metrics from Zeek logs
            conn_metrics = self.get_connection_metrics(ip_address)
            
            # 3. Combine all metrics
            self.instance_metrics[ip_address].update(host_metrics)
            self.instance_metrics[ip_address].update(conn_metrics)
            
            # Log the collected metrics
            logger.info(f"Metrics for {ip_address}: {json.dumps(self.instance_metrics[ip_address], indent=2)}")
    
    def get_host_metrics(self, ip_address):
        """Get host-level metrics (CPU, RAM utilization, trend coefficient)"""
        logger.info(f"Getting host metrics for {ip_address}")
        
        # In a real implementation, this would query a monitoring system like Zabbix
        # For now, we'll simulate with sample metrics
        # TODO: Replace with actual API calls to your monitoring system
        
        # Sample metrics (replace with actual data)
        metrics = {
            'cpu_utilization': 35.2,  # Percentage
            'ram_utilization': 48.7,  # Percentage
            'trend_coefficient': 0.8   # 0-1 range, higher is better (decreasing trend)
        }
        
        # Simulate metric collection from Elasticsearch or other metrics source
        try:
            # This would be a real metrics API call in production
            # Here we're just simulating different values for different hosts
            
            # Generate some variation based on IP (just for simulation)
            ip_sum = sum(int(octet) for octet in ip_address.split('.'))
            seed = ip_sum % 100
            
            # Vary metrics slightly based on seed
            metrics['cpu_utilization'] = max(5, min(95, 30 + (seed % 40)))
            metrics['ram_utilization'] = max(10, min(90, 40 + (seed % 30)))
            
            # Trend: -1 (increasing rapidly) to 1 (decreasing rapidly)
            metrics['trend_coefficient'] = (seed % 20 - 10) / 10
            
            logger.info(f"Host metrics for {ip_address}: CPU={metrics['cpu_utilization']}%, RAM={metrics['ram_utilization']}%, Trend={metrics['trend_coefficient']}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting host metrics for {ip_address}: {e}")
            return metrics  # Return default values as fallback
    
    def get_connection_metrics(self, ip_address):
        """Get connection-level metrics from Zeek logs"""
        logger.info(f"Getting connection metrics for {ip_address}")
        
        # Default metrics in case of error
        metrics = {
            'avg_connection_duration': 0.15,  # seconds
            'bytes_sent': 12500,              # bytes
            'bytes_received': 45000,          # bytes
            'error_ratio': 0.02               # ratio 0-1
        }
        
        # Query Zeek logs from Elasticsearch
        try:
            # Construct the query for the past 10 minutes
            now = datetime.datetime.utcnow()
            past = now - datetime.timedelta(minutes=10)
            
            start_time = past.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_time = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
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
            
            response = requests.post(
                f"{ELASTIC_URL}/_search",
                json=query,
                auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
                verify=False
            )
            
            if response.status_code == 200:
                result = response.json()
                hits = result.get('hits', {}).get('hits', [])
                logger.info(f"Found {len(hits)} Zeek connection logs for {ip_address}")
                
                if hits:
                    # Process connection logs to extract metrics
                    durations = []
                    bytes_sent_total = 0
                    bytes_received_total = 0
                    error_count = 0
                    total_connections = len(hits)
                    
                    for hit in hits:
                        source = hit.get('_source', {})
                        conn = source.get('conn', {})
                        
                        # Get connection duration
                        duration = conn.get('duration')
                        if duration:
                            durations.append(duration)
                        
                        # Get bytes transferred
                        if ip_address == conn.get('id', {}).get('orig_h'):
                            # This host is the originator
                            bytes_sent_total += conn.get('orig_bytes', 0) or 0
                            bytes_received_total += conn.get('resp_bytes', 0) or 0
                        else:
                            # This host is the responder
                            bytes_sent_total += conn.get('resp_bytes', 0) or 0
                            bytes_received_total += conn.get('orig_bytes', 0) or 0
                        
                        # Check for connection errors
                        conn_state = conn.get('conn_state', '')
                        if conn_state in ['S0', 'S1', 'REJ', 'RSTO', 'RSTOS0', 'RSTRH']:
                            error_count += 1
                    
                    # Calculate metrics
                    if durations:
                        metrics['avg_connection_duration'] = sum(durations) / len(durations)
                    
                    metrics['bytes_sent'] = bytes_sent_total
                    metrics['bytes_received'] = bytes_received_total
                    
                    if total_connections > 0:
                        metrics['error_ratio'] = error_count / total_connections
                    
                    logger.info(f"Connection metrics for {ip_address}: "
                                f"avg_duration={metrics['avg_connection_duration']:.3f}s, "
                                f"sent={metrics['bytes_sent']} bytes, "
                                f"received={metrics['bytes_received']} bytes, "
                                f"error_ratio={metrics['error_ratio']:.3f}")
            else:
                logger.error(f"Error querying Zeek logs: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error processing connection metrics for {ip_address}: {e}")
        
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
            
            # 4. Normalize avg connection duration (lower is better)
            duration = metrics.get('avg_connection_duration', 0.15)
            # Ideal range is 0.1 - 0.2 seconds
            if duration <= 0.2:
                scores['avg_connection_duration'] = 100
            else:
                # Penalty of 20 points for each 0.1s above ideal
                penalty = ((duration - 0.2) / 0.1) * 20
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
            
            # Use the existing update_service_weight function
            success = self.update_service_weight(ip_address, final_weight)
            
            if success:
                logger.info(f"Successfully updated weight for {service_id}")
            else:
                logger.error(f"Failed to update weight for {service_id}")
    
    def update_service_weight(self, ip_address, new_weight):
        """Update the weight tag for a specific service instance in Consul"""
        protocol = "https" if USE_HTTPS else "http"
        
        try:
            # Step 1: Find the service instance with the specified IP
            consul_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/catalog/service/{SERVICE_NAME}"
            response = requests.get(consul_url, verify=False)
            
            if response.status_code != 200:
                logger.error(f"Error finding service: {response.status_code}")
                return False
            
            services = response.json()
            
            # Find the instance with the target IP
            target_instance = None
            for instance in services:
                service_address = instance.get('ServiceAddress') or instance.get('Address')
                if service_address == ip_address:
                    target_instance = instance
                    break
            
            if not target_instance:
                logger.error(f"No service instance found with IP: {ip_address}")
                return False
            
            # Extract service details
            service_id = target_instance.get('ServiceID')
            
            # Step 2: Get detailed service information
            agent_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/{service_id}"
            agent_response = requests.get(agent_url, verify=False)
            
            if agent_response.status_code != 200:
                logger.error(f"Error getting service details: {agent_response.status_code}")
                return False
                
            service_details = agent_response.json()
            
            # Step 3: Update the weight in the tags
            current_tags = service_details.get('Tags', [])
            new_tags = []
            weight_updated = False
            
            for tag in current_tags:
                if 'urlprefix-/api weight=' in tag:
                    # Replace the weight value
                    parts = tag.split('weight=')
                    new_tag = f"{parts[0]}weight={new_weight}"
                    new_tags.append(new_tag)
                    weight_updated = True
                    logger.info(f"Updating tag from '{tag}' to '{new_tag}'")
                else:
                    new_tags.append(tag)
            
            # If no matching tag was found, add a new one
            if not weight_updated:
                new_tag = f"urlprefix-/api weight={new_weight}"
                new_tags.append(new_tag)
                logger.info(f"Adding new tag: '{new_tag}'")
            
            # Step 4: Prepare the updated service registration payload
            updated_service = {
                "ID": service_id,
                "Name": service_details.get('Service'),
                "Tags": new_tags,
                "Address": service_details.get('Address', ip_address),
                "Port": service_details.get('Port', 0)
            }
            
            # Add Meta field only if it exists in original service
            if 'Meta' in service_details:
                updated_service["Meta"] = service_details.get('Meta', {})
            
            # Step 5: Re-register the service with updated tags
            register_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/register"
            register_response = requests.put(
                register_url,
                json=updated_service,
                verify=False
            )
            
            if register_response.status_code == 200:
                logger.info(f"Successfully updated weight for service {service_id} to {new_weight}")
                return True
            else:
                logger.error(f"Error updating service: {register_response.status_code}")
                logger.error(f"Response: {register_response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating service weight: {e}")
            return False
    
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