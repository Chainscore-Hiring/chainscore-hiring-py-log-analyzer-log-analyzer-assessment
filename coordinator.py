import argparse
import asyncio
from aiohttp import web
import json
import random
import os
from analyzer import Analyzer

class Coordinator:
    """Manages work distribution and aggregates results from workers."""
    
    def __init__(self, port: int):
        self.workers = {}
        self.results = {}
        self.port = port
        self.analyzer = Analyzer()
        self.app = web.Application()
        self.app.add_routes([web.get('/health/{worker_id}', self.health_check),
                             web.post('/assign_work/{worker_id}', self.assign_work),
                             web.post('/report_results/{worker_id}', self.report_results)])

    async def start(self):
        """Start the coordinator's web server."""
        print(f"Coordinator starting on port {self.port}")
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        print(f"Coordinator started on port {self.port}")

        # Keep the coordinator running until manually stopped
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour, can be adjusted

    async def health_check(self, request):
        """Health check for workers"""
        worker_id = request.match_info.get('worker_id', None)
        print(f"Received health check from worker {worker_id}")
        return web.Response(text=f"Worker {worker_id} is alive")

    async def assign_work(self, request):
        """Assign work to workers"""
        worker_id = request.match_info.get('worker_id', None)
        print(f"Assigning work to worker {worker_id}")

        # Log file path
        log_file_paths = [
            'test_vectors/logs/error_spike.log',
            'test_vectors/logs/malformed.log',
            'test_vectors/logs/mixed_format.log',
            'test_vectors/logs/normal.log'
            ]
        
        # Prepare work assignments for each log file
        work_assignments = []
        for log_file_path in log_file_paths:
            file_size = os.path.getsize(log_file_path)
        
        # For simplicity, split the log file into chunks. Here we assign the whole file
        # start = random.randint(0, file_size // 2)
        # size = file_size // 2
        
        work = {
            'file_path': log_file_path,
            'start': 0,
            'size': file_size
        }
        work_assignments.append(work)

        # Return the work assignment as JSON
        return web.Response(text=json.dumps(work), content_type='application/json', status=200)

    async def report_results(self, request):
        """Receive and aggregate results from workers"""
        worker_id = request.match_info.get('worker_id', None)
        data = await request.json()
        
        print(f"Results received from worker {worker_id}: {data}")
        self.results[worker_id] = data
        
        # Update the analyzer with the new data
        self.analyzer.update_metrics(data)
        
        # Logging the current metrics
        current_metrics = self.analyzer.get_current_metrics()
        print(f"Current Metrics: {current_metrics}")
        
        # Optionally, you could aggregate the results here
        return web.Response(text="Results received", status=200)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Processing Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the coordinator")
    args = parser.parse_args()

    coordinator = Coordinator(args.port)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coordinator.start())