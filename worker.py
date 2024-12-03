import asyncio
import aiohttp
import re
from typing import Dict
import argparse
import uvicorn
from fastapi import FastAPI, Request, HTTPException


class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, worker_id: str, port: int, coordinator_url: str):
        self.worker_id = worker_id
        self.port = port
        self.coordinator_url = coordinator_url
        self.app = FastAPI()
        self.app.post("/process_chunk")(self.process_chunk_handler)
        self.session = None

    async def initialize(self):
        """Initialize resources such as aiohttp session."""
        self.session = aiohttp.ClientSession()

    async def cleanup(self):
        """Cleanup resources such as aiohttp session."""
        if self.session:
            await self.session.close()
    
    async def start(self) -> None:
        """Start the worker server."""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        await self.initialize()
        asyncio.create_task(self.register_with_coordinator())
        asyncio.create_task(self.send_heartbeat())
        config = uvicorn.Config(self.app, host="localhost", port=self.port)
        server = uvicorn.Server(config)
        await server.serve()

    async def register_with_coordinator(self):
        """Register the worker with the coordinator."""
        try:
            async with self.session.post(
                f"{self.coordinator_url}/register",
                json={"worker_id": self.worker_id, "port": self.port},
            ) as response:
                if response.status == 200:
                    print(f"Worker {self.worker_id} registered successfully.")
                else:
                    print(f"Failed to register worker {self.worker_id}. Response: {response.status}")
        except Exception as e:
            print(f"Error registering worker {self.worker_id}: {e}")

    async def send_heartbeat(self):
        """Send periodic heartbeat to the coordinator."""
        while True:
            try:
                async with self.session.post(
                    f"{self.coordinator_url}/health",
                    json={"worker_id": self.worker_id},
                ) as response:
                    if response.status == 200:
                        print(f"Heartbeat sent successfully for worker {self.worker_id}.")
                    else:
                        print(f"Heartbeat failed for worker {self.worker_id}. Response: {response.status}")
            except Exception as e:
                print(f"Error sending heartbeat for worker {self.worker_id}: {e}")
            await asyncio.sleep(5)


    async def process_chunk_handler(self, request: Request):
        """Handle file chunk processing request."""
        try:
            data = await request.json()
            filepath = data.get("filepath") or "test_vectors/logs/normal.log"
            start = data.get("start") or 0
            size = data.get("size") or 5

            if not all([filepath, start, size]):
                raise HTTPException(status_code=400, detail="Invalid input data")
            metrics = await self.process_chunk(filepath, start, size)
            await self.send_metrics(metrics)

            return {"message": "Chunk processed successfully"}
        except Exception as e:
            print(f"Error processing chunk request: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of the log file and return metrics."""
        metrics = {
            "error_count": 0,
            "total_response_time": 0,
            "request_count": 0,
        }

        try:
            with open(filepath, 'r') as file:
                file.seek(start)
                lines = file.read(size).splitlines()

            for line in lines:
                match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\w+) (.+)", line)
                if match:
                    _, level, message = match.groups()
                    if "Request processed in" in message:
                        response_time = int(message.split("processed in ")[-1].replace("ms", ""))
                        metrics["total_response_time"] += response_time
                        metrics["request_count"] += 1

                    if level == "ERROR":
                        metrics["error_count"] += 1

            metrics["avg_response_time"] = (
                metrics["total_response_time"] / metrics["request_count"]
                if metrics["request_count"] > 0
                else 0
            )
        except Exception as e:
            print(f"Error processing chunk from {filepath}: {e}")

        return metrics

    async def send_metrics(self, metrics: Dict):
        """Send processed metrics back to the coordinator."""
        try:
            async with self.session.post(
                f"{self.coordinator_url}/metrics",
                json={"worker_id": self.worker_id, "metrics": metrics},
            ) as response:
                if response.status == 200:
                    print(f"Metrics sent successfully for worker {self.worker_id}.")
                else:
                    print(f"Failed to send metrics for worker {self.worker_id}. Response: {response.status}")
        except Exception as e:
            print(f"Error sending metrics for worker {self.worker_id}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Worker")
    parser.add_argument("--port", type=int, default=8001, help="Worker port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(worker_id=args.id, port=args.port, coordinator_url=args.coordinator)

    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        print("Shutting down worker...")
        asyncio.run(worker.cleanup())
