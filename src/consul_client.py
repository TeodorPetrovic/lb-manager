class ConsulClient:
    def __init__(self, consul_url):
        self.consul_url = consul_url

    def fetch_services(self, service_name):
        import requests
        response = requests.get(f"{self.consul_url}/v1/catalog/service/{service_name}")
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_service_ips(self, service_name):
        services = self.fetch_services(service_name)
        return [service['Address'] for service in services] if services else []