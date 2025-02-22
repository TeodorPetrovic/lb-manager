# My Python App

This project is a Python application that interacts with a Consul registry to monitor services and fetch metrics from an Elastic Kibana server. It also processes Zeek logs to analyze network connections between a load balancer and registered applications.

## Project Structure

```
my-python-app
├── src
│   ├── main.py            # Entry point of the application
│   ├── consul_client.py   # Handles interactions with the Consul registry
│   ├── elastic_client.py   # Interacts with the Elastic Kibana server
│   ├── zeek_processor.py   # Processes Zeek logs for connection metrics
│   └── utils.py           # Utility functions for calculations
├── requirements.txt       # Lists project dependencies
└── README.md              # Project documentation
```

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd my-python-app
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

To run the application, execute the following command:
```
python src/main.py
```

The application will read services from the Consul registry every 5 minutes, fetch CPU and RAM metrics from the Elastic Kibana server, and process Zeek logs for connection metrics.

## Main Components

- **ConsulClient**: This class is responsible for fetching services from the Consul registry and retrieving their IP addresses.

- **ElasticClient**: This class queries the Elastic Kibana server for metricbeat logs and calculates average CPU and RAM usage over a specified time period.

- **ZeekProcessor**: This class processes Zeek logs to calculate average connection duration, count errors, and sum bytes sent and received between the load balancer and registered applications.

- **Utils**: Contains utility functions, including one to quantify trends in CPU and RAM usage.

## License

This project is licensed under the MIT License.