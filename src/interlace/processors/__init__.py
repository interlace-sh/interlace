"""
File processor plugins for ingestion jobs (sync, streams, etc.).

Phase A: Processor interfaces are defined in `interlace.sync.types` and concrete
processors will be added here (e.g., pgp_decrypt) in the next step.
"""

from interlace.processors.pgp import PGPDecryptProcessor
from interlace.processors.registry import build_default_processor_registry

__all__ = [
    "PGPDecryptProcessor",
    "build_default_processor_registry",
]
