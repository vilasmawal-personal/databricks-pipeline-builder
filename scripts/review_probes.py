"""Read-only behavior probes for the October 2026 repository review.

Run from the repository root: python -X utf8 review/review_probes.py
These do not run generated notebooks or contact Databricks/Ollama.
"""
import ast
import copy
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mapping_parser import parse_mapping_dict
from dpba.component1 import MappingAgent
from dpba.config import Settings
from dpba.llm import FakeLLMClient
from dpba.models.pipeline_spec import FieldMapping, TransformationDef
from component2_data_generator import DataGenerator, _compute_raw_expected, _rule_satisfied
from component3_pipeline_builder import (
    SnippetLibrary, generate_ingest_notebook, generate_transform_notebook,
    generate_integration_test_notebook, generate_databricks_yml, generate_job_yml,
)
import yaml

ROOT = Path(__file__).resolve().parents[1]
base = json.loads((ROOT / 'sample_specs/customer_pipeline.json').read_text(encoding='utf-8'))
results = {}

def probe(name, fn):
    try:
        results[name] = fn()
    except Exception as exc:
        results[name] = {'exception': type(exc).__name__, 'message': str(exc)}

probe('empty_object_passes_component1', lambda: MappingAgent(FakeLLMClient({}), Settings(llm_max_retries=0)).generate('requirements'))
probe('empty_object_then_component2', lambda: len(DataGenerator(parse_mapping_dict({})).generate()))
probe('schema_shaped_empty_stages_pass_component1', lambda: MappingAgent(FakeLLMClient({'pipeline_metadata':{},'source_configuration':{},'pipeline_stages':[{},{}]}), Settings(llm_max_retries=0)).generate('requirements'))

spec = parse_mapping_dict(base)
canonical = spec.model_dump()

def invalid_graph():
    raw = copy.deepcopy(canonical)
    raw['stages'][1]['inputs'] = ['nonexistent_stage']
    validated = parse_mapping_dict(raw)
    return {'accepted_inputs': validated.stages[1].inputs,
            'actually_reads_previous_stage': 'main.bronze.customers_raw' in generate_transform_notebook(validated, 1)}
probe('invalid_graph_accepted', invalid_graph)

def raw_rule():
    raw = copy.deepcopy(base)
    raw['data_expectations'] = [{'expectation_name':'required_rule', 'rule':'account_balance BETWEEN 0 AND 100', 'action_on_failure':'fail_pipeline'}]
    content = generate_transform_notebook(parse_mapping_dict(raw), 1)
    return {'accepted': True, 'emitted_skip_comment': 'not auto-enforced' in content}
probe('unsupported_rule_silently_skipped', raw_rule)

def formats():
    return {'declared_environments':base['bundle_metadata']['target_environments'],
            'generated_environments':list(yaml.safe_load(generate_databricks_yml(spec))['targets']),
            'declared_bundle':base['bundle_metadata']['bundle_name'], 'generated_bundle':spec.bundle_name,
            'declared_source':spec.source_location,
            'generated_dev_source':yaml.safe_load(generate_databricks_yml(spec))['targets']['dev']['variables']['source_path']}
probe('legacy_deployment_metadata_lost', formats)

def fm(kind, dtype='string', params=None):
    return FieldMapping(target_field='out', target_type=dtype, source_fields=['raw'], transformation=TransformationDef(type=kind,params=params or {}))
probe('integer_cast_oracle', lambda: {'expected':_compute_raw_expected(fm('cast','integer'),{'raw':'001'}), 'snippet':SnippetLibrary.cast(fm('cast','integer'))})
probe('decimal_input_oracle', lambda: {'expected':_compute_raw_expected(fm('conditional','decimal(18,2)',{'divisor':100}),{'raw':'1250.50'})})
probe('null_numeric_rule_oracle', lambda: _rule_satisfied(None, '>=', 0))

def bad_literal():
    field = FieldMapping(target_field='out',target_type='string',source_fields=['a','b'],transformation=TransformationDef(type='concat',params={'separator':'"'}))
    ast.parse(SnippetLibrary.concat(field))
probe('quote_separator_breaks_python', bad_literal)

def bad_description():
    raw = copy.deepcopy(base)
    raw['pipeline_metadata']['description'] = 'A "quoted" business description'
    yaml.safe_load(generate_job_yml(parse_mapping_dict(raw)))
probe('quote_description_breaks_yaml', bad_description)

def same_stage_shadowing():
    raw = copy.deepcopy(canonical)
    raw['stages'][1]['operations']['mappings'] = [
        {'target_field':'a','target_type':'string','source_fields':['b'],'transformation':{'type':'direct'}},
        {'target_field':'b','target_type':'string','source_fields':['a'],'transformation':{'type':'direct'}}]
    raw['stages'][1]['operations']['expectations'] = []
    raw['stages'][1]['output']['merge_keys'] = []
    s = parse_mapping_dict(raw)
    return {'oracle':DataGenerator(s)._compute_chain_and_disposition({'a':'A','b':'B'})[0],
            'emitted_sequential_snippets':[SnippetLibrary.direct(f) for f in s.final_stage.fields]}
probe('source_column_shadowing', same_stage_shadowing)

def conflicting_actions():
    raw = copy.deepcopy(canonical)
    raw['stages'][1]['operations']['expectations'] = [
        {'rule_id':action, 'name':action,'condition':{'operator':'IS_NOT_NULL','left_field':'customer_id'},'action':action}
        for action in ['drop_row','fail_pipeline']]
    s = parse_mapping_dict(raw)
    return {'oracle_disposition':DataGenerator(s)._resolve_stage_disposition(s.final_stage,{'customer_id':None}),
            'emits_fail_before_drop':generate_transform_notebook(s,1).index('_violations =') < generate_transform_notebook(s,1).index('_dropped =')}
probe('expectation_precedence_disagrees', conflicting_actions)

def merge_ignored():
    raw = copy.deepcopy(canonical)
    raw['stages'][1]['output']['write_mode'] = 'overwrite'
    s = parse_mapping_dict(raw)
    return {'declared_mode':s.final_stage.write_mode,'emits_merge':'.merge(df_out' in generate_transform_notebook(s,1)}
probe('overwrite_with_keys_emits_merge', merge_ignored)

def table_source():
    raw = copy.deepcopy(canonical)
    raw['sources'][0].update(type='table',format='delta',location_ref='main.raw.source')
    s = parse_mapping_dict(raw)
    return {'accepted_type':s.sources[0].type,'uses_file_load':'.load(SOURCE_PATH)' in generate_ingest_notebook(s)}
probe('table_source_emits_file_read', table_source)

def ambiguity_lost():
    raw = copy.deepcopy(base)
    raw['mappings'][0]['provenance']={'confidence':'human_required','assumptions':'Unknown field'}
    raw['issues']=[{'severity':'error','message':'unresolved'}]
    s = parse_mapping_dict(raw)
    return {'manual_json_accepted':True,'normalized_confidence':s.final_stage.fields[0].provenance.confidence}
probe('manual_ambiguity_bypasses_gate', ambiguity_lost)

def unsupported_type():
    raw = copy.deepcopy(canonical)
    raw['stages'][1]['operations']['mappings'][0]['target_type']='float'
    f = parse_mapping_dict(raw).final_stage.fields[0]
    return {'declared':f.target_type,'generated_type':f.spark_type_literal}
probe('unknown_target_type_silently_string', unsupported_type)

def generation_stable():
    a = [generate_databricks_yml(spec),generate_job_yml(spec),generate_ingest_notebook(spec),generate_transform_notebook(spec,1),generate_integration_test_notebook(spec)]
    b = [generate_databricks_yml(spec),generate_job_yml(spec),generate_ingest_notebook(spec),generate_transform_notebook(spec,1),generate_integration_test_notebook(spec)]
    return a == b
probe('same_spec_same_artifact_text', generation_stable)

def retries():
    class CountingLLM:
        calls=0
        def generate_structured(self, prompt, schema):
            self.calls += 1
            raw=copy.deepcopy(base)
            raw['mappings'][0]['transformation']['type']='aggregate'
            return raw
    llm=CountingLLM()
    try:
        MappingAgent(llm,Settings(llm_max_retries=3)).generate('requirements')
    except Exception as exc:
        return {'calls':llm.calls,'error':type(exc).__name__}
probe('unsupported_transform_does_not_retry', retries)

def packaging():
    import os
    import shutil
    import subprocess
    import tempfile
    with tempfile.TemporaryDirectory(prefix='dpba-review-package-') as temp:
        root = Path(temp)
        for name in ['pyproject.toml','README.md','mapping_parser.py','component2_data_generator.py','component3_pipeline_builder.py']:
            shutil.copy2(ROOT/name,root/name)
        shutil.copytree(ROOT/'dpba',root/'dpba',ignore=shutil.ignore_patterns('__pycache__'))
        shutil.copytree(ROOT/'prompts',root/'prompts')
        built = root/'built'
        env = dict(os.environ, PYTHONUTF8='1')
        env.pop('PYTHONPATH',None)
        process = subprocess.run([sys.executable,'-c','from setuptools import setup; setup()','build_py','--build-lib',str(built)],cwd=root,env=env,capture_output=True,text=True)
        if process.returncode:
            return {'build_exit':process.returncode,'error':process.stderr[-2000:]}
        imported = subprocess.run([sys.executable,'-c','import dpba.__main__'],cwd=built,env=env,capture_output=True,text=True)
        return {'built_files':sorted(str(p.relative_to(built)) for p in built.rglob('*.py')),
                'entrypoint_import_exit':imported.returncode,'error':imported.stderr.strip()}
probe('isolated_package_build', packaging)

rendered=json.dumps(results,indent=2,ensure_ascii=True)
Path(__file__).with_name('probe_results.json').write_text(rendered+'\n',encoding='utf-8')
print(rendered)
