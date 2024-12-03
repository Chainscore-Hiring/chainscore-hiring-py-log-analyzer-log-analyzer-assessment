
async def test_processing_speed():
    """Test processing speed meets requirements"""
    coordinator = Coordinator(port=8000)
    workers = [
        Worker("worker1", "localhost:8000"),
        Worker("worker2", "localhost:8000"),
        Worker("worker3", "localhost:8000")
    ]
    
    # Generate 1GB test file
    generate_test_data(size_mb=1024, path="test_vectors/logs/large.log")
    
    start_time = time.time()
    await coordinator.process_file("test_vectors/logs/large.log")
    duration = time.time() - start_time
    
    # Should process at least 100MB/sec
    assert (1024 / duration) >= 100

async def test_memory_usage():
    """Test memory stays within limits"""
    import psutil
    
    worker = Worker("worker1", "localhost:8000")
    process = psutil.Process()
    
    initial_memory = process.memory_info().rss
    await worker.process_chunk("test_vectors/logs/large.log", 0, 1024*1024*100)
    peak_memory = process.memory_info().rss
    
    # Should stay under 500MB
    assert (peak_memory - initial_memory) < 500 * 1024 * 1024