import re

path = 'tests/test_component2_data_generator.py'
with open(path) as f:
    content = f.read()

replacement = """def _fm(**overrides) -> FieldMapping:
    defaults = dict(
        target_field="x", source_fields=["y"], target_data_type="string",
        is_nullable=True, default_value=None, transformation_type=T_CAST,
        transformation_logic="", transformation_params={},
    )
    defaults.update(overrides)
    from dpba.models.pipeline_spec import TransformationDef
    return FieldMapping(
        target_field=defaults["target_field"],
        source_fields=defaults["source_fields"],
        target_type=defaults["target_data_type"],
        is_nullable=defaults["is_nullable"],
        default_value=defaults["default_value"],
        transformation=TransformationDef(
            type=defaults["transformation_type"],
            params=defaults["transformation_params"]
        )
    )"""

old_fm = """def _fm(**overrides) -> FieldMapping:
    defaults = dict(
        target_field="x", source_fields=["y"], target_data_type="string",
        is_nullable=True, default_value=None, transformation_type="cast",
        transformation_logic="", transformation_params={},
    )
    defaults.update(overrides)
    return FieldMapping(**defaults)"""

content = content.replace(old_fm, replacement)
with open(path, "w") as f:
    f.write(content)
