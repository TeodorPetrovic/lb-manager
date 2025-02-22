import requests

class ElasticClient:
    def __init__(self, elastic_url, username, password):
        self.elastic_url = elastic_url
        self.username = username
        self.password = password

    def fetch_metricbeat_logs(self, service_name, start_time, end_time):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"service.name": service_name}},
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                    ]
                }
            }
        }
        response = requests.post(
            f"{self.elastic_url}/_search",
            json=query,
            auth=(self.username, self.password)
        )
        return response.json()

    def calculate_average_cpu_ram(self, logs):
        cpu_usage = []
        ram_usage = []
        for hit in logs.get('hits', {}).get('hits', []):
            cpu_usage.append(hit['_source'].get('system.cpu.total.pct', 0))
            ram_usage.append(hit['_source'].get('system.memory.actual.used.pct', 0))
        average_cpu = sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0
        average_ram = sum(ram_usage) / len(ram_usage) if ram_usage else 0
        return average_cpu, average_ram

    def calculate_trend(self, usage_data):
        if len(usage_data) < 2:
            return 0
        start_value = usage_data[0]
        end_value = usage_data[-1]
        if start_value == 0:
            return 0
        raw_trend = (end_value - start_value) / start_value
        # Clamp normalized trend to the range [-1, 1]
        normalized_trend = max(-1, min(1, raw_trend))
        return normalized_trend

    def get_cpu_ram_metrics(self, service_name, start_time, end_time):
        # Fetch Metricbeat logs from Elastic.
        logs = self.fetch_metricbeat_logs(service_name, start_time, end_time)
        cpu_avg, ram_avg = self.calculate_average_cpu_ram(logs)
        # Recreate lists for calculating trends.
        cpu_data = [hit['_source'].get('system.cpu.total.pct', 0) 
                     for hit in logs.get('hits', {}).get('hits', [])]
        ram_data = [hit['_source'].get('system.memory.actual.used.pct', 0) 
                     for hit in logs.get('hits', {}).get('hits', [])]
        cpu_trend = self.calculate_trend(cpu_data)
        ram_trend = self.calculate_trend(ram_data)
        return {
            "cpu_average": cpu_avg,
            "ram_average": ram_avg,
            "cpu_trend": cpu_trend,
            "ram_trend": ram_trend
        }