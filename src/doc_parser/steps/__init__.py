"""Decoupled 2-step pipeline functions."""

from doc_parser.steps.step2_parse import run_parse
from doc_parser.steps.step3_extract import run_extraction

__all__ = ["run_parse", "run_extraction"]
