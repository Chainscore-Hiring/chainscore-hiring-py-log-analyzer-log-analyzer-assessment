import asyncio
import aiohttp
import os
from fastapi import FastAPI, Request
import uvicorn
import argparse

class Analyzer:
    """Calculates real-time metrics from results."""
    def __init__(self):
        self.metrics = {
            "error_count": 0,
            "total_response_time": 0,
            "request_count": 0,
        }

    def update_metrics(self, new_data: dict):
        """Update metrics with new data from a worker."""
        self.metrics["error_count"] += new_data["error_count"]
        self.metrics["total_response_time"] += new_data["total_response_time"]
        self.metrics["request_count"] += new_data["request_count"]

    def get_current_metrics(self) -> dict:
        """Calculate and return current metrics."""
        avg_response_time = (
            self.metrics["total_response_time"] / self.metrics["request_count"]
            if self.metrics["request_count"] > 0
            else 0
        )
        error_rate = (
            self.metrics["error_count"] / self.metrics["request_count"]
            if self.metrics["request_count"] > 0
            else 0
        )
        return {
            "error_count": self.metrics["error_count"],
            "request_count": self.metrics["request_count"],
            "average_response_time_ms": avg_response_time,
            "error_rate": error_rate,
        }
    
class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int):
        print(f"Starting coordinator on port {port}")
        self.workers = {}
        self.results = {}
        self.port = port
        self.analyzer = Analyzer() 
        self.app = FastAPI()
        self.register_routes()

    def start(self) -> None:
        """Start the coordinator server."""
        print(f"Starting server on port {self.port}...")
        uvicorn.run(self.app, host="127.0.0.1", port=self.port)

    def register_routes(self):
        """Register API routes."""
        self.app.post("/register")(self.register_worker)
        self.app.post("/health")(self.health_handler)
        self.app.post("/metrics")(self.metrics_handler)
        self.app.get("/metrics")(self.get_metrics_handler)

    async def register_worker(self, request: Request):
        """Register a new worker."""
        data = await request.json()
        worker_id = data["worker_id"]
        worker_port = data["port"]
        self.workers[worker_id] = {"port": worker_port, "healthy": True}
        print(f"Worker {worker_id} registered on port {worker_port}.")
        return {"message": "Worker registered"}
    
    async def health_handler(self, request: Request):
        """Receive heartbeat updates from workers."""
        data = await request.json()
        worker_id = data["worker_id"]

        if worker_id in self.workers:
            self.workers[worker_id]["healthy"] = True
            print(f"Received health update from worker {worker_id}.")
        else:
            print(f"Unknown worker {worker_id} sent heartbeat.")
        return {"message": "Health received"}

    async def metrics_handler(self, request: Request):
        """Receive and process metrics from workers."""
        data = await request.json()
        worker_id = data["worker_id"]
        worker_metrics = data["metrics"]

        self.results[worker_id] = worker_metrics
        print(f"Metrics received from {worker_id}: {worker_metrics}")
        self.analyzer.update_metrics(worker_metrics)
        return {"message": "Metrics received"}

    async def get_metrics_handler(self):
        """Serve real-time metrics."""
        current_metrics = self.analyzer.get_current_metrics()
        return current_metrics


    async def distribute_work(self, filepath: str) -> None:
        """Split file and assign chunks to workers"""
        available_workers = [worker_id for worker_id, worker in self.workers.items() if worker["healthy"]]
        
        if not available_workers:
            print("No available workers to distribute work to.")
            return
        

        file_size = os.path.getsize(filepath)
        chunk_size = file_size // len(available_workers)  

        tasks = []
        for idx, worker_id in enumerate(available_workers):
            start = idx * chunk_size 
            size = chunk_size if idx < len(available_workers) - 1 else file_size - start  
            worker_port = self.workers[worker_id]["port"] 
            task = self.send_chunk_to_worker(worker_id, filepath, start, size, worker_port)
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def send_chunk_to_worker(self, worker_id: str, filepath: str, start: int, size: int, port: int):
        """Send file chunk to worker for processing."""
        url = f"http://127.0.0.1:{port}/process_chunk"
        payload = {"filepath": filepath, "start": start, "size": size}
        
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(url, json=payload)
                if response.status == 200:
                    print(f"Sent chunk to worker {worker_id} for processing.")
                else:
                    print(f"Failed to send chunk to worker {worker_id}. Status code: {response.status}")
        except Exception as e:
            print(f"Error sending chunk to worker {worker_id}: {e}")
            await self.handle_worker_failure(worker_id)

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        print(f"Handling failure of worker {worker_id}")
        
        if worker_id in self.workers:
            self.workers[worker_id]["healthy"] = False 
            print(f"Worker {worker_id} marked as failed.")
        
        failed_worker_tasks = self.results.pop(worker_id, None) 
        await self.redistribute_failed_tasks(failed_worker_tasks)  

    async def redistribute_failed_tasks(self, failed_worker_tasks):
        """Redistribute failed worker's tasks to available workers."""
        if not failed_worker_tasks:
            print("No tasks to redistribute.")
            return
        
        available_workers = [worker_id for worker_id, worker in self.workers.items() if worker["healthy"]] 
        
        if not available_workers:
            print("No available workers to redistribute tasks to.")
            return
        
        for worker_id in available_workers: 
            worker_port = self.workers[worker_id]["port"]
            await self.send_chunk_to_worker(worker_id, failed_worker_tasks['filepath'], failed_worker_tasks['start'], failed_worker_tasks['size'], worker_port)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    coordinator.start()