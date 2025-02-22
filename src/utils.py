def calculate_trend(data):
    if len(data) < 2:
        return 0  # Not enough data to determine a trend

    initial_value = data[0]
    final_value = data[-1]
    trend = (final_value - initial_value) / initial_value * 100  # Percentage change
    return trend

def average(values):
    if not values:
        return 0
    return sum(values) / len(values)

def extract_metrics_from_logs(logs):
    cpu_usage = []
    ram_usage = []

    for log in logs:
        cpu_usage.append(log.get('cpu', 0))
        ram_usage.append(log.get('ram', 0))

    return average(cpu_usage), average(ram_usage)

def extract_connection_metrics(connections):
    total_duration = 0
    error_count = 0
    total_bytes_sent = 0
    total_bytes_received = 0

    for conn in connections:
        total_duration += conn.get('duration', 0)
        error_count += conn.get('error', 0)
        total_bytes_sent += conn.get('bytes_sent', 0)
        total_bytes_received += conn.get('bytes_received', 0)

    average_duration = total_duration / len(connections) if connections else 0

    return average_duration, error_count, total_bytes_sent, total_bytes_received