# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests",
# ]
# ///

import csv
import os
import subprocess
import sys
from datetime import datetime, timedelta

import requests

REPO = "conda/conda"
WORKFLOW_NAME = "tests.yml"


def check_gh_cli():
    try:
        result = subprocess.run(
            ["gh", "--version"], capture_output=True, text=True, check=True
        )
        print(f"Found gh CLI: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError:
        print(
            "ERROR: either the GitHub CLI is not found or not working.",
        )
        print(
            "Please install the GitHub CLI from https://cli.github.com/",
            file=sys.stderr,
        )
        print("After installation, authenticate with: gh auth login")
        sys.exit(1)


def get_gh_token():
    """Get GitHub token from gh CLI or environment variable"""
    token = os.getenv("GITHUB_TOKEN")
    if token:
        print("Using GITHUB_TOKEN from environment variable")
        return token

    check_gh_cli()

    try:
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, check=True
        )
        token = result.stdout.strip()
        if token:
            print("Successfully retrieved a token from the GitHub CLI")
            return token
        else:
            print("ERROR: gh auth token returned empty result")
            print("Please authenticate with: gh auth login")
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        print("ERROR: Failed to get token from gh CLI")
        print(f"Error: {e.stderr}")
        print("Please authenticate with: gh auth login")
        sys.exit(1)


token = get_gh_token()
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json",
}

three_months_ago = datetime.now() - timedelta(days=90)

print("Fetching workflow runs...")

# some pagination needed here..
runs = []
page = 1
should_continue = True

while should_continue:
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_NAME}/runs"
    params = {
        "status": "completed",
        "per_page": 100,
        "page": page,
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    if not data["workflow_runs"]:
        break

    for run in data["workflow_runs"]:
        run_date = datetime.strptime(run["created_at"], "%Y-%m-%dT%H:%M:%SZ")
        if run_date < three_months_ago:
            should_continue = False
            break
        if run["conclusion"] == "success":
            runs.append(run)

    page += 1
    print(f"    Fetched page {page - 1} ({len(runs)} successful runs so far)...")

print(f"\nFound {len(runs)} successful runs in the last 3 months")

print("Fetching job details for Windows tests...")
results = []
for i, run in enumerate(runs):
    print(f"  Processing run {i + 1}/{len(runs)}...", end="\r")

    jobs_url = run["jobs_url"]
    response = requests.get(jobs_url, headers=headers)
    response.raise_for_status()
    jobs_data = response.json()

    for job in jobs_data["jobs"]:
        if "windows" in job["name"].lower() and job["conclusion"] == "success":
            duration_seconds = (
                datetime.strptime(job["completed_at"], "%Y-%m-%dT%H:%M:%SZ")
                - datetime.strptime(job["started_at"], "%Y-%m-%dT%H:%M:%SZ")
            ).total_seconds()

            results.append(
                {
                    "run_id": run["id"],
                    "run_date": run["created_at"],
                    "job_name": job["name"],
                    "duration_seconds": duration_seconds,
                    "duration_minutes": duration_seconds / 60,
                }
            )

print(
    f"\nFound {len(results)} Windows job results in total between start {three_months_ago} and {three_months_ago + timedelta(days=90)}\n"
)

output_file = "windows_ci_times.csv"
with open(output_file, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "run_id",
            "run_date",
            "job_name",
            "duration_seconds",
            "duration_minutes",
        ],
    )
    writer.writeheader()
    writer.writerows(results)

print(f"Results written to {output_file}")
