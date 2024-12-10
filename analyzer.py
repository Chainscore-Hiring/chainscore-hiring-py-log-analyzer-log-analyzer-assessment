from typing import Dict

class Analyzer:
    """Calculates real-time metrics from results."""
    
    def __init__(self):
        self.metrics = {
            'total_requests': 0,
            'total_errors': 0,
            'total_response_time': 0,
            'worker_count': 0,
            'error_rate': 0,
            'avg_response_time': 0,
            'request_count_per_second': {}
        }

    def update_metrics(self, new_data: Dict) -> None:
        """Update metrics with new data from workers."""
        # Update total requests and errors
        self.metrics['total_requests'] += new_data.get('request_count', 0)
        self.metrics['total_errors'] += new_data.get('error_count', 0)
        self.metrics['total_response_time'] += new_data.get('avg_response_time', 0) * new_data.get('request_count', 0)

        # Update request count per second
        for second, count in new_data.get('request_count_per_second', {}).items():
            if second in self.metrics['request_count_per_second']:
                self.metrics['request_count_per_second'][second] += count
            else:
                self.metrics['request_count_per_second'][second] = count

        # Update worker count
        self.metrics['worker_count'] += 1

        # Recalculate error rate and average response time
        if self.metrics['total_requests'] > 0:
            self.metrics['error_rate'] = self.metrics['total_errors'] / self.metrics['total_requests']
            self.metrics['avg_response_time'] = self.metrics['total_response_time'] / self.metrics['total_requests']

    def get_current_metrics(self) -> Dict:
        """Return current calculated metrics."""
        return {
            'total_requests': self.metrics['total_requests'],
            'total_errors': self.metrics['total_errors'],
            'error_rate': self.metrics['error_rate'],
            'avg_response_time': self.metrics['avg_response_time'],
            'request_count_per_second': self.metrics['request_count_per_second']
        }