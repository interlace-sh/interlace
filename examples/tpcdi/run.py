#!/usr/bin/env python3
"""
TPC-DI Benchmark Runner

This script runs the TPC-DI benchmark using Interlace.
TPC-DI focuses on data integration workflows: Extract, Transform, Load (ETL).
"""

from pathlib import Path
from interlace import run


if __name__ == "__main__":
    project_dir = Path(__file__).parent
    
    # Run all TPC-DI models using the programmatic API
    results = run(project_dir=project_dir, env="dev", verbose=True)


