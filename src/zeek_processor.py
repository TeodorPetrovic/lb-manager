import requests

class ZeekProcessor:
    def __init__(self, zeek_url):
        self.zeek_url = zeek_url

    def get_avg_conn_duration(self, lb_ip, service_ip, start_time, end_time):
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
        # Query Zeek logs endpoint.
        response = requests.post(f"{self.zeek_url}/_search", json=query)
        logs = response.json()
        durations = []
        for hit in logs.get('hits', {}).get('hits', []):
            # Extract duration from the "conn" section, if present.
            duration = hit['_source'].get("conn", {}).get("duration")
            if duration is not None:
                durations.append(duration)
        avg_duration = sum(durations) / len(durations) if durations else 0
        return avg_duration

    def process_logs(self, lb_ip, services, start_time, end_time):
        result = {}
        for service in services:
            avg_duration = self.get_avg_conn_duration(lb_ip, service['Address'], start_time, end_time)
            result[service['Service']] = avg_duration
        return result