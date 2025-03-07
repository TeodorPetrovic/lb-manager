import requests
import json
import pandas as pd
import datetime
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Consul configuration
CONSUL_HOST = "10.100.0.9"
CONSUL_PORT = 8500  # Default Consul HTTP API port
SERVICE_NAME = "sdn-news"
USE_HTTPS = False  # Set to True if Consul API is using HTTPS

def query_service_instances():
    """Query Consul for all instances of a specific service"""
    protocol = "https" if USE_HTTPS else "http"
    consul_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/catalog/service/{SERVICE_NAME}"
    
    print(f"Querying Consul for service: {SERVICE_NAME}")
    print(f"URL: {consul_url}")
    
    try:
        # Get service instances from Consul
        response = requests.get(consul_url, verify=False)
        
        if response.status_code == 200:
            services = response.json()
            
            # Save raw response to file
            with open(f"{SERVICE_NAME}_instances.json", "w") as f:
                json.dump(services, f, indent=4)
            
            print(f"\nFound {len(services)} instances of {SERVICE_NAME}")
            
            if not services:
                print(f"No instances found for service: {SERVICE_NAME}")
                return
            
            # Extract and display service information
            instance_data = []
            for instance in services:
                # Extract core data
                node_name = instance.get('Node', 'N/A')
                address = instance.get('Address', 'N/A')
                service_id = instance.get('ServiceID', 'N/A')
                service_address = instance.get('ServiceAddress', address)  # If ServiceAddress is empty, use Node Address
                service_port = instance.get('ServicePort', 'N/A')
                
                # Extract health checks if available
                health_status = "Unknown"
                if 'Checks' in instance:
                    checks = instance['Checks']
                    statuses = [check.get('Status') for check in checks]
                    if all(status == 'passing' for status in statuses):
                        health_status = "Healthy"
                    elif any(status == 'critical' for status in statuses):
                        health_status = "Critical"
                    elif any(status == 'warning' for status in statuses):
                        health_status = "Warning"
                
                # Extract tags if available
                tags = instance.get('ServiceTags', [])
                tags_str = ", ".join(tags) if tags else "None"
                
                # Extract metadata if available
                meta = instance.get('ServiceMeta', {})
                
                # Add to our collection
                instance_data.append({
                    'Node': node_name,
                    'IP Address': service_address,
                    'Port': service_port,
                    'Service ID': service_id,
                    'Health': health_status,
                    'Tags': tags_str
                })
                
                # Print summary to console
                print(f"\nInstance: {node_name}")
                print(f"  IP Address: {service_address}")
                print(f"  Port: {service_port}")
                print(f"  Service ID: {service_id}")
                print(f"  Health: {health_status}")
                print(f"  Tags: {tags_str}")
            
            # Create Excel report
            export_to_excel(instance_data)
            
            return instance_data
            
        else:
            print(f"Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.ConnectionError:
        print(f"Connection Error: Could not connect to Consul at {consul_url}")
        print("Please check that Consul is running and the address is correct.")
        return None
    except Exception as e:
        print(f"Error querying Consul: {e}")
        return None

def export_to_excel(instance_data):
    """Export service instance data to Excel"""
    if not instance_data:
        print("No data to export to Excel")
        return
    
    # Create DataFrame
    df = pd.DataFrame(instance_data)
    
    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_file = f"{SERVICE_NAME}_instances_{timestamp}.xlsx"
    
    # Export to Excel
    writer = pd.ExcelWriter(excel_file, engine='openpyxl')
    df.to_excel(writer, index=False, sheet_name=f'{SERVICE_NAME} Instances')
    
    # Auto-adjust columns width
    for column in df:
        column_length = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets[f'{SERVICE_NAME} Instances'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
    
    writer.close()
    
    print(f"\nExported instance data to {excel_file}")

def update_service_weight(ip_address="10.0.0.5", new_weight=60):
    """Update the weight tag for a specific service instance in Consul"""
    protocol = "https" if USE_HTTPS else "http"
    
    print(f"Updating weight for service instance with IP: {ip_address}")
    print(f"New weight will be: {new_weight}")
    
    # Step 1: Find the service instance with the specified IP
    consul_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/catalog/service/{SERVICE_NAME}"
    
    try:
        # Get all service instances
        response = requests.get(consul_url, verify=False)
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
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
            print(f"No service instance found with IP: {ip_address}")
            return False
        
        # Extract service details
        service_id = target_instance.get('ServiceID')
        print(f"Found service instance: {service_id}")
        
        # Step 2: Get detailed service information
        agent_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/{service_id}"
        agent_response = requests.get(agent_url, verify=False)
        
        if agent_response.status_code != 200:
            print(f"Error getting service details: {agent_response.status_code}")
            print(f"Response: {agent_response.text}")
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
                print(f"Updating tag from '{tag}' to '{new_tag}'")
            else:
                new_tags.append(tag)
        
        # If no matching tag was found, add a new one
        if not weight_updated:
            new_tag = f"urlprefix-/api weight={new_weight}"
            new_tags.append(new_tag)
            print(f"Adding new tag: '{new_tag}'")
        
        # Step 4: Prepare the updated service registration payload
        # Using correct field names as expected by Consul API
        updated_service = {
            "ID": service_id,
            "Name": service_details.get('Service'),  # Changed 'Service' to 'Name'
            "Tags": new_tags,
            "Address": service_details.get('Address', ip_address),
            "Port": service_details.get('Port', 0)
        }
        
        # Add Meta field only if it exists in original service
        if 'Meta' in service_details:
            updated_service["Meta"] = service_details.get('Meta', {})
            
        print(f"Sending updated service registration: {json.dumps(updated_service, indent=2)}")
        
        # Step 5: Re-register the service with updated tags
        register_url = f"{protocol}://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/register"
        register_response = requests.put(
            register_url,
            json=updated_service,
            verify=False
        )
        
        if register_response.status_code == 200:
            print(f"Successfully updated weight for service {service_id} to {new_weight}")
            return True
        else:
            print(f"Error updating service: {register_response.status_code}")
            print(f"Response: {register_response.text}")
            return False
            
    except Exception as e:
        print(f"Error updating service weight: {e}")
        return False

if __name__ == "__main__":
    print("Querying Consul Service Registry...")
    query_service_instances()
    
    print("\nUpdating service weight...")
    update_service_weight("10.0.0.5", 60)