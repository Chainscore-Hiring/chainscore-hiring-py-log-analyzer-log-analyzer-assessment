# Distributed Log Analyzer Documentation

## Table of Contents
- [Project Overview](#project-overview)
- [Setup Instructions](#setup-instructions)
- [Design Decisions](#design-decisions)
- [Performance Results](#performance-results)
- [Testing Instructions](#testing-instructions)

## Project Overview
The Distributed Log Analyzer is a system designed to process large log files efficiently using a coordinator and multiple worker nodes. The system calculates real-time metrics such as error rates, average response times, and resource usage patterns while handling worker failures gracefully.

## Setup Instructions
To set up the project, follow these steps:

1. **Clone the Repository**
   ```bash
   git clone https://github.com/chainscore-hiring/log-analyzer-assessment
   cd log-analyzer-assessment

2. **Create a Virtual Environment**
    python -m venv venv
    source venv/bin/activate  # On Windowsuse `venv\Scripts\activate`

3. **Install Dependencies**
    pip install -r requirements.txt

4. **Start the System Open separate terminal windows for the coordinator and workers:**
    # Start the coordinator
    python coordinator.py --port 8000

    # Start worker nodes
    python worker.py --id alice --port 8001 --coordinator http://localhost:8000
    python worker.py --id bob --port 8002 --coordinator http://localhost:8000
    python worker.py --id charlie --port 8003 --coordinator http://localhost:8000

# Design Decisions
    -Architecture: The system is designed with a coordinator that manages multiple worker nodes. This allows for distributed processing of log files, improving efficiency and scalability.
    -Error Handling: The system includes mechanisms to handle -worker failures. If a worker fails, the coordinator can reassign the work to another available worker.
    -Real-time Metrics: The system calculates metrics in real-time, allowing for immediate insights into log data, which is crucial for monitoring and debugging.

# Testing Instructions
To run the tests for the project, follow these steps:
1. Install Testing Dependencies Ensure you have pytest and pytest-asyncio installed:
pip install pytest pytest-asyncio pytest-cov

2. Run the Tests Execute the following command to run all tests:
pytest tests/

3. Check Test Coverage To check the coverage of your tests, run:
pytest --cov=your_module_name tests/

# Conclusion
This documentation provides an overview of the Distributed Log Analyzer project, including setup instructions, design decisions, performance results, and testing instructions. For further information or contributions, please refer to the project's GitHub repository.

