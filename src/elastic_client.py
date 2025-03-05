import requests
import logging

class ElasticClient:
    def __init__(self, elastic_url, username, password):
        # Set the elastic_url to point to HTTPS and port 9200.
        self.elastic_url = elastic_url  # e.g., "https://10.100.0.7:9200"
        self.username = username
        self.password = password

    def fetch_metricbeat_logs(self, field, host_ip, start_time, end_time):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"host.ip": host_ip}},
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                    ]
                }
            }
        }
        logging.debug("Elastic Query for metricbeat logs: %s", query)
        response = requests.post(
            f"{self.elastic_url}/_search",
            json=query,
            auth=(self.username, self.password),
            verify=False
        )
        logging.debug("Elastic response status (metricbeat): %s", response.status_code)
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            logging.error("Error in fetch_metricbeat_logs: %s", err)
            logging.error("Response content: %s", response.text)
            raise
        result = response.json()
        #logging.debug("Elastic response data (metricbeat): %s", result)
        return result

    def calculate_average(self, values):
        return sum(values) / len(values) if values else 0

    def calculate_trend(self, usage_data):
        if len(usage_data) < 2:
            return 0
        start_value = usage_data[0]
        end_value = usage_data[-1]
        if start_value == 0:
            return 0
        raw_trend = (end_value - start_value) / start_value
        return max(-1, min(1, raw_trend))

    def get_cpu_ram_metrics(self, host_ip, start_time, end_time):
        logs = self.fetch_metricbeat_logs("cpu", host_ip, start_time, end_time)
        hits = logs.get('hits', {}).get('hits', [])
        logging.debug("Number of Metricbeat hits returned: %d", len(hits))
        cpu_values = []
        ram_values = []
        for hit in hits:
            source = hit.get('_source', {})
            metricset = source.get("metricset", {}).get("name", "")
            if metricset == "cpu":
                cpu_val = source.get("system", {}) \
                                .get("cpu", {}) \
                                .get("total", {}) \
                                .get("pct")
                if cpu_val is not None:
                    cpu_values.append(cpu_val)
            elif metricset == "memory":
                ram_val = source.get("system", {}) \
                                .get("memory", {}) \
                                .get("actual", {}) \
                                .get("used", {}) \
                                .get("pct")
                if ram_val is not None:
                    ram_values.append(ram_val)
        logging.debug("Collected CPU values: %s", cpu_values)
        logging.debug("Collected RAM values: %s", ram_values)
        return {
            "cpu_average": self.calculate_average(cpu_values),
            "ram_average": self.calculate_average(ram_values),
            "cpu_trend": self.calculate_trend(cpu_values),
            "ram_trend": self.calculate_trend(ram_values)
        }
    
    def get_zeek_avg_conn_duration(self, lb_ip, service_ip, start_time, end_time):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"filter_match_status": "conn"}},
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                        {"bool": {
                            "should": [
                                {
                                    "bool": {
                                        "must": [
                                            {"match": {"conn.id.orig_h": lb_ip}},
                                            {"match": {"conn.id.resp_h": service_ip}}
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {"match": {"conn.id.orig_h": service_ip}},
                                            {"match": {"conn.id.resp_h": lb_ip}}
                                        ]
                                    }
                                }
                            ]
                        }}
                    ]
                }
            }
        }
        logging.debug("Elastic Query for Zeek logs: %s", query)
        response = requests.post(
            f"{self.elastic_url}/_search",
            json=query,
            auth=(self.username, self.password),
            verify=False
        )
        logging.debug("Elastic response status (Zeek): %s", response.status_code)
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            logging.error("Error in get_zeek_avg_conn_duration: %s", err)
            logging.error("Response content (Zeek): %s", response.text)
            raise
        logs = response.json()
        # Log the number of Zeek hits
        zeek_hits = logs.get('hits', {}).get('hits', [])
        logging.debug("Number of Zeek hits returned: %d", len(zeek_hits))
        #logging.debug("Elastic Zeek logs: %s", logs)
        durations = []
        for hit in zeek_hits:
            duration = hit['_source'].get("conn", {}).get("duration")
            if duration is not None:
                durations.append(duration)
        return sum(durations) / len(durations) if durations else 0