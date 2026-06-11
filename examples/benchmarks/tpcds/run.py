#!/usr/bin/env python3
"""
TPC-DS Benchmark Runner

This script runs the TPC-DS benchmark using Interlace.
It generates TPC-DS data and executes all 99 standard queries.
"""

from pathlib import Path
from interlace import run


if __name__ == "__main__":
    project_dir = Path(__file__).parent
    
    # Run all TPC-DS models using the programmatic API
    results = run(project_dir=project_dir, env="dev", verbose=True)


