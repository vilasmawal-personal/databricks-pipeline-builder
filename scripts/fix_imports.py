import os, glob, re

def fix_file(path):
    with open(path) as f:
        content = f.read()
    
    # Simple strategy: just find "from dpba.models.pipeline_spec import ..."
    # and split it into two: one for mapping_parser and one for dpba.models.pipeline_spec
    # Actually, easiest is just to replace all imports of parse_mapping_json and T_*
    # from dpba.models.pipeline_spec to mapping_parser.
    
    # We will just replace specific strings.
    content = content.replace(
        "from dpba.models.pipeline_spec import parse_mapping_json", 
        "from mapping_parser import parse_mapping_json"
    )
    content = content.replace(
        "from dpba.models.pipeline_spec import FieldMapping, T_CAST, T_CONCAT, T_CONDITIONAL, T_LOOKUP",
        "from dpba.models.pipeline_spec import FieldMapping\nfrom mapping_parser import T_CAST, T_CONCAT, T_CONDITIONAL, T_LOOKUP"
    )
    content = content.replace(
        "from dpba.models.pipeline_spec import FieldMapping, T_CONDITIONAL, T_CONCAT, T_LOOKUP, T_DATE_FORMAT",
        "from dpba.models.pipeline_spec import FieldMapping\nfrom mapping_parser import T_CONDITIONAL, T_CONCAT, T_LOOKUP, T_DATE_FORMAT"
    )
    
    # tests/test_mapping_parser.py
    if "test_mapping_parser.py" in path:
        content = content.replace(
            "from dpba.models.pipeline_spec import (",
            "from dpba.models.pipeline_spec import PipelineSpec, FieldMapping, PipelineStage, DataExpectation\nfrom mapping_parser import ("
        )
        content = content.replace("PipelineSpec,\n", "")
        content = content.replace("FieldMapping,\n", "")
        content = content.replace("PipelineStage,\n", "")
        content = content.replace("DataExpectation,\n", "")
        
    with open(path, "w") as f:
        f.write(content)

for p in glob.glob("tests/*.py"):
    fix_file(p)
