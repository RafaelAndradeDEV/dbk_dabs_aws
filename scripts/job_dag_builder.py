"""Build dependency DAG from Model() usage."""

import argparse
import ast
import json
import os
from os.path import relpath
from pathlib import Path

import yaml
from ruamel.yaml import YAML


def load_base_task_dict(value: str):
    try:
        p = Path(value)

        # File input (YAML or JSON)
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                return yaml.safe_load(p.read_text())
        else:
            return json.loads(value)
    except TypeError:
        return json.loads(value)


def update_job_inplace(job_path: Path, job_name: str, tasks: list):
    yaml_loader = YAML()
    yaml_loader.preserve_quotes = True  # optional, preserves quotes
    yaml_loader.indent(mapping=2, sequence=4, offset=2)

    # Load YAML
    with job_path.open("r") as f:
        doc = yaml_loader.load(f)

    # Ensure structure exists
    resources = doc.setdefault("resources", {})
    jobs = resources.setdefault("jobs", {})

    if job_name not in jobs:
        raise KeyError(f"Job '{job_name}' not found under resources.jobs")

    jobs[job_name]["tasks"] = tasks

    # Write back YAML
    with job_path.open("w") as f:
        yaml_loader.dump(doc, f)


def extract_literal_or_dict_call(node):
    out = {}
    if isinstance(node, ast.Dict):
        for k, v in zip(node.keys, node.values, strict=True):
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                if isinstance(v, ast.Constant):
                    out[k.value] = v.value
                else:
                    out[k.value] = None

    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict":
        for kw in node.keywords:
            if isinstance(kw.value, ast.Constant):
                out[kw.arg] = kw.value.value
            else:
                out[kw.arg] = None
    return out


def scan_file(path: Path):
    with path.open() as f:
        tree = ast.parse(f.read(), filename=str(path))

    params = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Model":
            for kw in node.keywords:
                if kw.arg == "model_name" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    try:
                        params["depends_on"].append(kw.value.value)
                    except KeyError:
                        params["depends_on"] = [kw.value.value]
                if kw.arg == "task_params":
                    params.update(extract_literal_or_dict_call(kw.value))

    return params


def build_dag(root: Path, job_file_path: Path, base_task_dict: dict):
    dag = []

    for file in root.rglob("*.py"):
        task = base_task_dict.copy()

        os.chdir(job_file_path.parent)
        rel = relpath(file)
        meta = scan_file(file)

        task.update({"task_key": file.stem, "notebook_task": {"notebook_path": rel}, **meta})
        dag.append(task)

    return dag


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build dependency DAG from Model() usage.")
    parser.add_argument("--project-root", required=True, help="Path to project root")
    parser.add_argument("--job-file", required=True, help="Path to job base directory")

    parser.add_argument("--base-task", help="YAML/JSON file or inline JSON", default="{}")
    parser.add_argument("--inplace", action="store_true", help="YAML file containing job definition to update")
    parser.add_argument("--job-name", help="Job name under resources.jobs.<job_name>")

    args = parser.parse_args()

    project_root = Path(args.project_root).absolute()
    job_file_path = Path(args.job_file).absolute()
    base_task_dict = load_base_task_dict(args.base_task)

    dag = build_dag(project_root, job_file_path, base_task_dict)

    # Update-in-place mode
    if args.inplace and not args.job_name:
        raise SystemExit("--update-inplace requires --job-name")
    elif args.inplace:
        update_job_inplace(job_file_path, args.job_name, dag)
    else:
        print(yaml.dump(dag, sort_keys=False))
