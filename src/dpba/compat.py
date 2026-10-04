"""Temporary aliases for the pre-src-layout public module names."""
import sys

from dpba import component2_data_generator, component3_pipeline_builder, mapping_parser

sys.modules.setdefault("mapping_parser", mapping_parser)
sys.modules.setdefault("component2_data_generator", component2_data_generator)
sys.modules.setdefault("component3_pipeline_builder", component3_pipeline_builder)
