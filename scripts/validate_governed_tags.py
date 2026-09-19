"""Deploy-time validator for per-bundle governed_tags.yml specs.

Finds all governed_tags.yml files under bundles/, validates YAML structure,
and checks that all tag keys and values are in the allowed policy defined in
bundles/shared/governed_tags_allowed.yml.

Usage:
    python scripts/validate_governed_tags.py
    python scripts/validate_governed_tags.py --allowed bundles/shared/governed_tags_allowed.yml

Exit codes:
    0 — all specs are valid
    1 — one or more violations found
"""

import argparse
import fnmatch
import sys
from pathlib import Path

import yaml

REQUIRED_RULE_FIELDS = ("catalog_var", "schema_var", "include")


def load_yaml(path: Path) -> object:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_allowed(allowed_path: Path) -> dict[str, list[str]]:
    """Return {key: [allowed_values]} from governed_tags_allowed.yml."""
    data = load_yaml(allowed_path)
    result: dict[str, list[str]] = {}
    for key, cfg in (data.get("governed_tags") or {}).items():
        result[key] = cfg.get("values", [])
    return result


def validate_glob(pattern: str) -> str | None:
    """Return an error message if pattern is not a valid glob, else None."""
    try:
        fnmatch.translate(pattern)
        return None
    except Exception as exc:
        return str(exc)


def validate_spec(spec_path: Path, allowed: dict[str, list[str]]) -> list[str]:
    """Return a list of violation messages for a single governed_tags.yml."""
    violations: list[str] = []

    try:
        data = load_yaml(spec_path)
    except yaml.YAMLError as exc:
        return [f"Invalid YAML: {exc}"]

    if not isinstance(data, dict):
        return ["Top-level structure must be a mapping."]

    defaults = data.get("defaults") or {}
    rules = data.get("rules") or []

    if not isinstance(defaults, dict):
        violations.append("'defaults' must be a mapping.")
        defaults = {}

    if not isinstance(rules, list):
        violations.append("'rules' must be a list.")
        rules = []

    # Validate defaults
    for key, value in defaults.items():
        if key not in allowed:
            violations.append(f"defaults: unknown tag key '{key}'")
        elif value not in allowed[key]:
            violations.append(f"defaults: invalid value '{value}' for key '{key}' (allowed: {allowed[key]})")

    # Validate rules
    for idx, rule in enumerate(rules):
        prefix = f"rules[{idx}]"

        if not isinstance(rule, dict):
            violations.append(f"{prefix}: must be a mapping.")
            continue

        for field in REQUIRED_RULE_FIELDS:
            if field not in rule:
                violations.append(f"{prefix}: missing required field '{field}'")

        for pattern in rule.get("include") or []:
            err = validate_glob(pattern)
            if err:
                violations.append(f"{prefix}: invalid include glob '{pattern}': {err}")

        for pattern in rule.get("exclude") or []:
            err = validate_glob(pattern)
            if err:
                violations.append(f"{prefix}: invalid exclude glob '{pattern}': {err}")

        for key, value in (rule.get("tags") or {}).items():
            if key not in allowed:
                violations.append(f"{prefix}: unknown tag key '{key}'")
            elif value not in allowed[key]:
                violations.append(f"{prefix}: invalid value '{value}' for key '{key}' (allowed: {allowed[key]})")

    return violations


def main() -> None:
    repo_root = Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Validate per-bundle governed_tags.yml files.")
    parser.add_argument(
        "--allowed",
        type=Path,
        default=repo_root / "bundles" / "shared" / "governed_tags_allowed.yml",
        help="Path to governed_tags_allowed.yml (default: bundles/shared/governed_tags_allowed.yml)",
    )
    args = parser.parse_args()

    if not args.allowed.exists():
        print(f"ERROR: allowed values file not found: {args.allowed}", file=sys.stderr)
        sys.exit(1)

    allowed = load_allowed(args.allowed)

    spec_files = sorted((repo_root / "bundles").glob("*/governed_tags.yml"))
    if not spec_files:
        print("No governed_tags.yml files found. Nothing to validate.")
        sys.exit(0)

    total_violations = 0
    for spec_path in spec_files:
        rel = spec_path.relative_to(repo_root)
        violations = validate_spec(spec_path, allowed)
        if violations:
            print(f"FAIL  {rel}")
            for v in violations:
                print(f"      {v}")
            total_violations += len(violations)
        else:
            print(f"OK    {rel}")

    if total_violations:
        print(f"\n{total_violations} violation(s) found.", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll governed_tags.yml files are valid.")


if __name__ == "__main__":
    main()
