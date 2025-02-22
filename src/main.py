import time
import json
from datetime import datetime, timedelta
from consul_client import ConsulClient
from elastic_client import ElasticClient
from zeek_processor import ZeekProcessor

def main():
    consul_url = "http://10.100.0.9:8500/"
    elastic_url = "http://10.100.0.7:5601/"
    load_balancer_url = "http://10.100.0.9:9998/"
    service_name = "sdn-news"

    consul_client = ConsulClient(consul_url)
    # Use appropriate credentials for Elastic.
    elastic_client = ElasticClient(elastic_url, "elastic_user", "elastic_password")
    zeek_processor = ZeekProcessor()

    # Extract load balancer IP from its URL.
    lb_ip = "10.100.0.9"

    while True:
        now = datetime.utcnow()
        start_time = (now - timedelta(minutes=5)).isoformat() + "Z"
        end_time = now.isoformat() + "Z"

        # Fetch services from Consul using the existing fetch_services method.
        services = consul_client.fetch_services(service_name)

        results = {}
        for service in services:
            # Get CPU/RAM metrics from Elastic (Metricbeat logs).
            cpu_ram_metrics = elastic_client.get_cpu_ram_metrics(service['Service'], start_time, end_time)
            # Get average connection duration from Zeek conn logs.
            avg_conn_duration = zeek_processor.get_avg_conn_duration(lb_ip, service['Address'], start_time, end_time)
            
            results[service['Service']] = {
                "cpu_average": cpu_ram_metrics['cpu_average'],
                "ram_average": cpu_ram_metrics['ram_average'],
                "cpu_trend": cpu_ram_metrics['cpu_trend'],
                "ram_trend": cpu_ram_metrics['ram_trend'],
                "avg_connection_duration": avg_conn_duration
            }

        with open('results.json', 'w') as json_file:
            json.dump(results, json_file, indent=4)

        # Wait for 5 minutes before the next iteration.
        time.sleep(300)

if __name__ == "__main__":
    main()