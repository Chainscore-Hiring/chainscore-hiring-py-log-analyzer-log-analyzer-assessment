import argparse
import asyncio
import json
import random
import re
import os
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from datetime import datetime
from collections import defaultdict
from log_entry import LogEntry
import time
import psutil
from typing import Dict

class Worker:
    """Processes log chunks and reports results to the coordinator."""
    
    def __init__(self, worker_id: str, coordinator_url: str, port: int):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port
        self.session = None


    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of log file and return metrics"""
        try:
            # Start measuring time and memory
            start_time = time.time()
            process = psutil.Process()  # Get the current process
            initial_memory = process.memory_info().rss  # Get initial memory usage
            
            # Simulate log processing
            await self.simulate_log_processing(filepath, start, size)
            
            # Measure time taken and memory used
            duration = time.time() - start_time
            peak_memory = process.memory_info().rss
            
            # Calculate metrics
            memory_used = peak_memory - initial_memory
            processing_speed = size / duration if duration > 0 else 0  # Bytes per second
            
            print(f"Worker {self.worker_id} processed {size} bytes in {duration:.2f} seconds.")
            print(f"Memory used: {memory_used / (1024 * 1024):.2f} MB")
            print(f"Processing speed: {processing_speed / (1024 * 1024):.2f} MB/s")
            
            return {
                'duration': duration,
                'memory_used': memory_used,
                'processing_speed': processing_speed
            }
        except Exception as e:
            print(f"Error processing chunk: {e}")

    async def simulate_log_processing(self, filepath: str, start: int, size: int):
        """Simulate log processing """
        await asyncio.sleep(1)  # Simulate processing time

    async def start(self):
        """Start the worker and connect to the coordinator."""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        self.session = ClientSession(connector=TCPConnector(ssl=False))
        
        try:
            # Check health periodically
            await self.report_health()
            
            # Fetch work from the coordinator
            await self.assign_work()
        finally:
            await self.close()
    
    async def close(self):
        """Close the client session."""
        if self.session:
            await self.session.close()
        
     

    async def report_health(self):
        """Send heartbeat to coordinator"""
        url = f"{self.coordinator_url}/health/{self.worker_id}"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    print(f"Worker {self.worker_id} health check passed.")
                else:
                    print(f"Worker {self.worker_id} health check failed.")
        except Exception as e:
            print(f"Worker {self.worker_id} failed to connect to coordinator: {e}")

    async def assign_work(self):
        """Fetch work assignment from coordinator."""
        url = f"{self.coordinator_url}/assign_work/{self.worker_id}"
        try:
            async with self.session.post(url) as response:
                if response.status == 200:
                    work = await response.json()
                    print(f"Worker {self.worker_id} received work assignment.")
                    await self.process_work(work)
                else:
                    print(f"Worker {self.worker_id} failed to get work.")
        except Exception as e:
            print(f"Worker {self.worker_id} failed to connect to coordinator: {e}")

    async def process_work(self, work):
        """Process the assigned chunk of logs."""
        file_path = work['file_path']
        start = work['start']
        size = work['size']
        
        # Initialize metrics
        request_times = []
        error_count = 0
        total_response_time = 0
        total_requests = 0
        
        # Read the chunk of the log file (simulated with random reading)
        with open(file_path, 'r') as file:
            file.seek(start)
            chunk = file.read(size)
        
        print(f"Worker {self.worker_id} processed a chunk from {file_path} starting at {start}.")
        
        # Split the chunk into lines and process each line
        lines = chunk.splitlines()
        for line in lines:
            log_entry = LogEntry.from_line(line)  # Create LogEntry instance
            if log_entry:
                total_requests += 1  # Count total requests
                request_times.append(log_entry.timestamp)  # Store request timestamp
                
                if log_entry.level == "ERROR":
                    error_count += 1  # Increment error count if level is ERROR
                
                if "response_time" in log_entry.metrics:
                    total_response_time += log_entry.metrics["response_time"]  # Accumulate response time

        # Calculate request count per second
        request_count_per_second = defaultdict(int)
        for request_time in request_times:
            second = request_time.replace(microsecond=0)  # Normalize to the second
            request_count_per_second[second] += 1
        
        # Printing metrics before reporting
        print(f"Total Requests: {total_requests}, Error Count: {error_count}, Total Response Time: {total_response_time}")
        
        # Calculate error rate
        error_rate = error_count / total_requests if total_requests > 0 else 0
        
        # Calculate average response time
        avg_response_time = total_response_time / total_requests if total_requests > 0 else 0

        results = {
            'request_count': total_requests,  # Include total requests
            'error_count': error_count,        # Include total errors
            'error_rate': error_rate,
            'avg_response_time': avg_response_time,
            'request_count_per_second': dict(request_count_per_second)  # Store the counts
        }
    
        # Report the results to the coordinator
        await self.report_results(results)

    async def report_results(self, results):
        """Send processing results to coordinator."""
        url = f"{self.coordinator_url}/report_results/{self.worker_id}"
        try:
            # Convert datetime keys to string in request_count_per_second
            results['request_count_per_second'] = {
                str(key): value for key, value in results['request_count_per_second'].items()
            }
            
            async with self.session.post(url, json=results) as response:
                if response.status == 200:
                    print(f"Worker {self.worker_id} results successfully reported.")
                else:
                    print(f"Worker {self.worker_id} failed to report results")
        except Exception as e:
            print(f"Worker {self.worker_id} failed to report results: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Processing Worker")
    parser.add_argument("--id", required=True, help="Worker ID")
    parser.add_argument("--port", type=int, required=True, help="Port to run the worker")
    parser.add_argument("--coordinator", required=True, help="URL of the coordinator")
    args = parser.parse_args()

    worker = Worker(args.id, args.coordinator, args.port)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.start())
