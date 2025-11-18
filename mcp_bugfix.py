import os
import tempfile
import subprocess

def run_mcp_bugfix(jira_number, summary, description):
    """
    Implements the MCP bug fix workflow using git and GitHub CLI tools, following Agents.md.
    """
    status_steps = [f"Received request to fix {jira_number}"]
    branch = f"fb_{jira_number}"
    commit_msg = f"fix({jira_number}): {summary}"
    pr_body = f"Jira: {jira_number}\n\n{description}"
    fix_file = f"fix_{jira_number}.txt"
    try:
        # 1. Ensure main is up to date
        status_steps.append("Checking out main and pulling latest changes...")
        subprocess.run(["git", "checkout", "main"], check=True)
        subprocess.run(["git", "pull"], check=True)
        # 2. Create feature branch
        status_steps.append(f"Creating/updating feature branch {branch}...")
        subprocess.run(["git", "checkout", "-B", branch], check=True)
        # 3. Apply the fix (placeholder: create a file)
        with open(fix_file, "w") as f:
            f.write(f"Fix for {jira_number}: {summary}\n{description}\n")
        status_steps.append(f"Created {fix_file} as a placeholder for the fix.")
        # 4. Stage and commit
        subprocess.run(["git", "add", fix_file], check=True)
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        status_steps.append(f"Committed fix with message: {commit_msg}")
        # 5. Push branch
        subprocess.run(["git", "push", "--set-upstream", "origin", branch], check=True)
        status_steps.append(f"Pushed branch {branch} to origin.")
        # 6. Create PR using GitHub CLI (gh)
        status_steps.append("Creating PR via GitHub CLI...")
        pr_result = subprocess.run([
            "gh", "pr", "create",
            "--title", commit_msg,
            "--body", pr_body,
            "--base", "main",
            "--head", branch
        ], capture_output=True, text=True, check=False)
        if pr_result.returncode == 0:
            status_steps.append("PR created successfully.")
            message = pr_result.stdout.strip()
        else:
            status_steps.append(f"PR creation failed: {pr_result.stderr.strip()}")
            message = f"PR creation failed: {pr_result.stderr.strip()}"
    except subprocess.CalledProcessError as e:
        status_steps.append(f"Git or CLI error: {e}")
        message = f"Failed to trigger fix workflow: {e}"
    except Exception as e:
        status_steps.append(f"Error: {str(e)}")
        message = f"Failed to trigger fix workflow: {str(e)}"
    return status_steps, message
