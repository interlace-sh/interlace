#!/usr/bin/env python3
"""
TPC-H Benchmark Runner

This script runs the TPC-H benchmark using Interlace.
It generates TPC-H data and executes all 22 standard queries.
"""

from pathlib import Path
from interlace import run


if __name__ == "__main__":
    project_dir = Path(__file__).parent
    
    # Run all TPC-H models using the programmatic API
    results = run(project_dir=project_dir, env="dev", verbose=True)


