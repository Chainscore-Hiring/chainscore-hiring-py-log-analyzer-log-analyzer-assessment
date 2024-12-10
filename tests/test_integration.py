import pytest
import asyncio
from coordinator import Coordinator
from worker import Worker

@pytest.mark.asyncio
async def test_worker_assignment():
    coordinator = Coordinator(port=8000)
    worker = Worker(worker_id='test_worker', coordinator_url='http://localhost:8000')

    # Simulate starting the coordinator and assigning work
    await coordinator.distribute_work('test_vectors/logs/error_spike.log')