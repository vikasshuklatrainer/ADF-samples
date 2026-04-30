# ============================================================
# Exercise 12 — CLI Pipeline Runner                  [MEDIUM]
# Topic: Automation
# ============================================================
#
# TASK:
# Build a command-line script that accepts:
#   --source   path to a CSV file (required)
#   --env      environment: "dev" or "prod" (default: "dev")
#   --dry-run  boolean flag (no value needed)
#
# Behaviour:
#   - Set logging level to DEBUG in dev, INFO in prod
#   - If --dry-run is set, log what WOULD happen but don't process
#   - Otherwise, read the CSV and log how many rows it found
#
# Run it like:
#   python ex12_cli_runner.py --source data.csv --env dev
#   python ex12_cli_runner.py --source data.csv --env prod --dry-run
#
# HINT:
# Use argparse.ArgumentParser.
# add_argument("--dry-run", action="store_true") creates a boolean flag.
# Access it as args.dry_run (argparse converts hyphens to underscores).
# ============================================================

import argparse, logging, csv, io

# Simulate file content (replace with open(args.source) in real use)
MOCK_FILE = "id,name,amount\n1,Alice,100\n2,Bob,200\n3,Carol,150"

# --- YOUR CODE HERE ---
def process_file(source: str) -> int:
    pass

def main():
    pass

if __name__ == "__main__":
    main()


# ============================================================
# SOLUTION
# ============================================================

def solution():
    import sys
    sys.argv = ["ex12", "--source", "data.csv", "--env", "dev"]  # simulate CLI

    def process_file(source: str) -> int:
        """Return row count from a CSV file."""
        reader = csv.DictReader(io.StringIO(MOCK_FILE))
        rows = list(reader)
        logging.debug(f"Loaded {len(rows)} rows from {source}")
        return len(rows)

    def main():
        parser = argparse.ArgumentParser(description="Pipeline runner")
        parser.add_argument("--source",  required=True)
        parser.add_argument("--env",    choices=["dev","prod"], default="dev")
        parser.add_argument("--dry-run", action="store_true")
        args = parser.parse_args()

        level = logging.DEBUG if args.env == "dev" else logging.INFO
        logging.basicConfig(level=level, format="%(levelname)s %(message)s")

        logging.info(f"Starting pipeline [env={args.env}]")

        if args.dry_run:
            logging.info(f"DRY RUN — would process: {args.source}")
        else:
            count = process_file(args.source)
            logging.info(f"Done — processed {count} rows")

    main()

if __name__ == "__main__":
    solution()
