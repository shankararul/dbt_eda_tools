"""
Estimate BigQuery build cost by dry-running all compiled dbt models.

Usage:
    python scripts/estimate_build_cost.py [--select model1 model2 ...]

Requires:
    - dbt compile to have been run first (populates target/compiled/)
    - google-cloud-bigquery installed
    - authenticated via gcloud or service account (reads from profiles.yml)
"""

import argparse
import json
import os
import sys
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account


BYTES_PER_TB = 1_099_511_627_776
COST_PER_TB = 6.25


def load_profile(project_dir: Path):
    """Read project + profiles.yml and return the active BQ target config."""
    import yaml

    dbt_project = yaml.safe_load((project_dir / "dbt_project.yml").read_text())
    profile_name = dbt_project["profile"]

    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    profiles = yaml.safe_load(profiles_path.read_text())

    profile = profiles[profile_name]
    target_name = profile["target"]
    return profile["outputs"][target_name]


def get_bq_client(target: dict) -> bigquery.Client:
    project = target.get("project") or target.get("database")
    if target.get("method") == "service-account":
        creds = service_account.Credentials.from_service_account_file(
            target["keyfile"],
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project, credentials=creds)
    return bigquery.Client(project=project)


def get_compiled_models(project_dir: Path, select: list[str] | None):
    manifest_path = project_dir / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())

    models = []
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model":
            continue
        if select and node["name"] not in select:
            continue
        compiled_path = (
            project_dir
            / "target"
            / "compiled"
            / node["package_name"]
            / node["original_file_path"]
        )
        if compiled_path.exists():
            models.append((node["name"], compiled_path.read_text()))
        else:
            print(f"  [skip] {node['name']} — compiled file not found, run dbt compile first")
    return models


def dry_run_model(client: bigquery.Client, sql: str):
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    return job.total_bytes_processed or 0


def print_table(headers, rows):
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    sep = "-" * (sum(col_widths) + len(headers) * 3 + 1)
    fmt = " | ".join(f"{{:<{w}}}" for w in col_widths)

    print(sep)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))
    print(sep)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--select", nargs="*", help="Model names to estimate")
    args = parser.parse_args()

    project_dir = Path(__file__).parent.parent
    target = load_profile(project_dir)

    if target["type"] != "bigquery":
        print("Error: estimate_build_cost only supports BigQuery targets.")
        sys.exit(1)

    client = get_bq_client(target)
    models = get_compiled_models(project_dir, args.select)

    if not models:
        print("No compiled models found. Run `dbt compile` first.")
        sys.exit(1)

    rows = []
    total_bytes = 0

    for name, sql in models:
        try:
            model_bytes = dry_run_model(client, sql)
            total_bytes += model_bytes
            gb = round(model_bytes / 1_073_741_824, 4)
            cost = round((model_bytes / BYTES_PER_TB) * COST_PER_TB, 6)
            rows.append([name, f"{gb} GB", f"${cost}"])
        except Exception as e:
            rows.append([name, "ERROR", str(e)[:40]])

    print_table(["Model", "GB Scanned", "Est. Cost (USD)"], rows)

    total_gb = round(total_bytes / 1_073_741_824, 4)
    total_cost = round((total_bytes / BYTES_PER_TB) * COST_PER_TB, 6)
    print(f"Total: {total_gb} GB  |  Estimated cost: ${total_cost} USD")


if __name__ == "__main__":
    main()
