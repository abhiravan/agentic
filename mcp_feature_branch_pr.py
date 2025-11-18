import os
import subprocess
import requests
from dotenv import load_dotenv

def get_issue(jira_key):
    JIRA_URL = 'https://mercagent.atlassian.net'
    JIRA_USER = os.getenv('JIRA_USER')
    JIRA_TOKEN = os.getenv('JIRA_TOKEN')
    url = f"{JIRA_URL}/rest/api/3/issue/{jira_key}"
    auth = (JIRA_USER, JIRA_TOKEN)
    headers = {"Accept": "application/json"}
    resp = requests.get(url, auth=auth, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Jira fetch failed: {resp.status_code} {resp.text}")
    issue = resp.json()
    summary = issue.get('fields', {}).get('summary', '')
    description = issue.get('fields', {}).get('description', '')
    return summary, description

def run_git(cmd, check=True):
    print(f"$ git {' '.join(cmd)}")
    result = subprocess.run(['git'] + cmd, check=check, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result

def main():
    load_dotenv()
    jira_key = input("Enter Jira key (e.g., AG-3): ").strip()
    summary, description = get_issue(jira_key)
    branch = f"fb_{jira_key}"
    print(f"\n--- MCP Automated Bug Fix for {jira_key} ---\nSummary: {summary}\nDescription: {description}\n")

    # 1. Ensure main is up to date
    run_git(["checkout", "main"])
    run_git(["pull"])

    # 2. Create feature branch
    run_git(["checkout", "-B", branch])

    # 3. (Manual/AI) Apply fix here - for demo, just touch a file
    fix_file = f"fix_{jira_key}.txt"
    with open(fix_file, "w") as f:
        f.write(f"Fix for {jira_key}: {summary}\n{description}\n")
    print(f"Created {fix_file} as a placeholder for the fix.")

    # 4. Stage and commit
    run_git(["add", fix_file])
    commit_msg = f"fix({jira_key}): {summary}"
    run_git(["commit", "-m", commit_msg])

    # 5. Push branch
    run_git(["push", "--set-upstream", "origin", branch])

    # 6. Create PR using GitHub CLI (gh)
    pr_title = commit_msg
    pr_body = f"Jira: {jira_key}\n\n{description}"
    print("Creating PR via GitHub CLI...")
    subprocess.run([
        "gh", "pr", "create",
        "--title", pr_title,
        "--body", pr_body,
        "--base", "main",
        "--head", branch
    ], check=False)
    print("PR creation attempted. Check your GitHub repo for the PR.")

if __name__ == "__main__":
    main()
