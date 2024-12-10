from datetime import datetime
from typing import Dict, Optional

class LogEntry:
    def __init__(self, timestamp: datetime, level: str, message: str, metrics: Optional[Dict[str, float]] = None):
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.metrics = metrics or {}

    @classmethod
    def from_line(cls, line: str) -> 'LogEntry':
        """
        Parse a log line and return a LogEntry object.
        Example log format: "2024-01-24 10:15:32.123 INFO Request processed in 127ms"
        """
        parts = line.split(' ', 3)
        if len(parts) < 4:
            print(f"Malformed log line: {line}")  # Log the malformed line
            return None  # Return None if the line does not have enough parts
        
        try:
            timestamp = datetime.strptime(parts[0] + ' ' + parts[1], "%Y-%m-%d %H:%M:%S.%f")
        except ValueError as e:
            print(f"Error parsing timestamp from line: {line}. Error: {e}")
            return None  # Return None if timestamp parsing fails

        level = parts[2]
        message = parts[3]

        metrics = {}
        if "ms" in message:
           try:
                response_time = int(message.split(' ')[3][:-2])  # "127ms" -> 127
                metrics["response_time"] = response_time
           except (IndexError, ValueError) as e:
               print(f"Error extracting metrics from message: {message}. Error: {e}")
            
        return cls(timestamp, level, message, metrics)