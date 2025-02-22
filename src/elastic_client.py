import requests

class ElasticClient:
    def __init__(self, elastic_url, username, password):
        self.elastic_url = elastic_url
        self.username = username
        self.password = password

    def fetch_metricbeat_logs(self, field, service_name, start_time, end_time):
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

    def get_cpu_ram_metrics(self, service_name, start_time, end_time):
        logs = self.fetch_metricbeat_logs("cpu", service_name, start_time, end_time)
        cpu_values = []
        ram_values = []
        for hit in logs.get('hits', {}).get('hits', []):
            source = hit['_source']
            cpu_values.append(source.get('system.cpu.total.pct', 0))
            ram_values.append(source.get('system.memory.actual.used.pct', 0))
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
        response = requests.post(
            f"{self.elastic_url}/_search",
            json=query,
            auth=(self.username, self.password)
        )
        logs = response.json()
        durations = []
        for hit in logs.get('hits', {}).get('hits', []):
            duration = hit['_source'].get("conn", {}).get("duration")
            if duration is not None:
                durations.append(duration)
        return sum(durations) / len(durations) if durations else 0