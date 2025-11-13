import argparse
import json
from helpers.adapter_loader import run_php_adapter
from helpers.shared_logic import get_bearer_token
from dispatcher import dispatch
from reports.report_writer import generate_report_files

def main():
    parser = argparse.ArgumentParser(description="Run migration from CLI")
    parser.add_argument("--adapter", required=True)
    parser.add_argument("--csv", required=True)
    parser.add_argument("--base_url", required=True)
    parser.add_argument("--entity", required=True)
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--migration_type", default="insert")
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args()

    adapter_path = f"adapters/{args.adapter}.php"
    raw_output = run_php_adapter(adapter_path, args.csv, args.migration_type)
    token = get_bearer_token(args.email, args.password, args.base_url)
    api_url = f"{args.base_url}/entities/{args.entity}"

    summary, stats = dispatch(
        adapter_key=args.adapter,
        payload=raw_output,
        migration_type=args.migration_type,
        api_url=api_url,
        auth_token=token,
        entity=args.entity
    )

    print(json.dumps(summary, indent=2))

    report_files = generate_report_files(summary, args.adapter, args.entity, args.migration_type)
    print("Reports generated:", report_files)

if __name__ == "__main__":
    main()