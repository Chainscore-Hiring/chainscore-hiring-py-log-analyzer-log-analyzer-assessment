import pytest
import asyncio


async def test_normal_processing():
    """Test normal log processing with all workers"""
    coordinator = Coordinator(port=8000)
    workers = [
        Worker("worker1", "localhost:8000"),
        Worker("worker2", "localhost:8000"),
        Worker("worker3", "localhost:8000")
    ]
    
    # Start system
    await coordinator.start()
    for w in workers:
        await w.start()
    
    # Process normal logs
    results = await coordinator.process_file("test_vectors/logs/normal.log")
    
    # Verify results
    assert results["avg_response_time"] == pytest.approx(109.0, rel=1e-2)
    assert results["error_rate"] == 0.0
    assert results["requests_per_second"] == pytest.approx(50.0, rel=1e-2)

async def test_worker_failure():
    """Test recovery from worker failure"""
    coordinator = Coordinator(port=8000)
    workers = [
        Worker("worker1", "localhost:8000"),
        Worker("worker2", "localhost:8000"),
        Worker("worker3", "localhost:8000")
    ]
    
    # Start with failing worker scenario
    await NetworkScenarios.worker_failure()
    
    # Process should complete despite failure
    results = await coordinator.process_file("test_vectors/logs/normal.log")
    
    # Verify results still accurate
    assert results["total_requests"] == 3000

async def test_malformed_logs():
    """Test handling of malformed logs"""
    coordinator = Coordinator(port=8000)
    workers = [Worker("worker1", "localhost:8000")]
    
    results = await coordinator.process_file("test_vectors/logs/malformed.log")
    
    assert results["malformed_lines"] == 30
    assert results["total_requests"] == 200