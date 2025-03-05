import time
import json
import logging
from datetime import datetime, timezone
from consul_client import ConsulClient
from elastic_client import ElasticClient

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    consul_url = "http://10.100.0.9:8500/"
    elastic_url = "https://10.100.0.7:9200"
    service_name = "sdn-news"

    consul_client = ConsulClient(consul_url)
    elastic_client = ElasticClient(elastic_url, "elastic", "-5HJtLOH+H3ma-S*9wAA")

    lb_ip = "10.100.0.9"
    
    # Fixed time window: 23.02.2025 from 19:30 to 20:00 (UTC)
    start_time = "2025-02-23T19:30:00+00:00"
    end_time   = "2025-02-23T20:00:00+00:00"

    logging.info("Fetching services for service name '%s'", service_name)
    services = consul_client.fetch_services(service_name)
    logging.info("Found %d service(s)", len(services))
    
    results = {}

    for service in services:
        svc_name = service.get("ServiceName")
        svc_ip = service.get("ServiceAddress")
        if svc_name and svc_ip:
            logging.info("Processing service '%s' with IP %s", svc_name, svc_ip)
            cpu_ram_metrics = elastic_client.get_cpu_ram_metrics(svc_ip, start_time, end_time)
            avg_conn_duration = elastic_client.get_zeek_avg_conn_duration(lb_ip, svc_ip, start_time, end_time)
            
            results[svc_name + "_" + svc_ip] = {
                "cpu_average": cpu_ram_metrics['cpu_average'],
                "ram_average": cpu_ram_metrics['ram_average'],
                "cpu_trend": cpu_ram_metrics['cpu_trend'],
                "ram_trend": cpu_ram_metrics['ram_trend'],
                "avg_connection_duration": avg_conn_duration
            }
            logging.info("Updated results for '%s' with IP %s", svc_name, svc_ip)
        else:
            logging.warning("Skipping service due to missing keys: %s", service)

    with open('results.json', 'w') as json_file:
        json.dump(results, json_file, indent=4)
    logging.info("Results written to results.json")

    # Sleep is optional when using a fixed time window; remove if not needed.
    logging.info("Sleeping for 300 seconds...")
    time.sleep(300)

if __name__ == "__main__":
    main()
