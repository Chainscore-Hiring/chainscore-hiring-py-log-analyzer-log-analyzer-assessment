import pytest
import asyncio
from worker import Worker

@pytest.mark.asyncio
async def test_process_chunk():
    worker = Worker(worker_id='test_worker', coordinator_url='http://localhost:8000')
    
    # Simulate a log file path and chunk size
    filepath = 'test_vectors/logs/error_spike.log'
    start = 0
    size = 1024 * 1024  # 1 MB chunk

    # Call the process_chunk method
    metrics = await worker.process_chunk(filepath, start, size)

    # Assertions to verify the metrics
    assert 'duration' in metrics
    assert 'memory_used' in metrics
    assert 'processing_speed' in metrics
    assert metrics['duration'] >= 0  # Duration should be non-negative
    assert metrics['memory_used'] >= 0  # Memory used should be non-negative
    assert metrics['processing_speed'] >= 0  # Processing speed should be non-negative