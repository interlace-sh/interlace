#!/usr/bin/env python3
"""
Example script to run Interlace models.

This demonstrates:
- Model discovery from Python files
- Dependency extraction (implicit from function parameters)
- Dynamic execution of DAG
- CSV ingestion and transformations
"""

"""
Example script to run Interlace models using the programmatic API.
"""

from pathlib import Path
from interlace import run


if __name__ == "__main__":
    project_dir = Path(__file__).parent
    
    # Run all models using the programmatic API
    # Heading is now displayed by the executor using project name from config
    results = run(project_dir=project_dir, verbose=True)

