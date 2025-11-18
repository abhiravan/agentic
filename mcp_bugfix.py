import os
import subprocess
import re
import difflib

def ai_select_file(jira_summary, jira_description, repo_files):
    """
    Select the most relevant file for the fix based on Jira summary/description.
    """
    keywords = set((jira_summary + ' ' + jira_description).lower().split())
    best_file = None
    best_score = 0
    for fname in repo_files:
        score = sum(1 for k in keywords if k in fname.lower())
        if score > best_score:
            best_score = score
            best_file = fname
    return best_file or (repo_files[0] if repo_files else None)

def ai_generate_patch(jira_summary, jira_description, old_code):
    """
    Generate the new code based on Jira description. STRICT: Only apply the described change.
    For demo, if AG-3, add NEXT_PRICE and NEXT_PRICE_EFFECTIVE_DATE columns to extract and downstream logic.
    """
    # This is a hardcoded patch for AG-3, but in a real system, call an LLM here.
    if 'NEXT_PRICE' in jira_description and 'NEXT_PRICE_EFFECTIVE_DATE' in jira_description:
        # Patch for nt_pchg_audit.py
        lines = old_code.splitlines()
        new_lines = []
        for line in lines:
            if "'NEW_PRICE_EFFECTIVE_DATE'," in line and 'selected_columns' in ''.join(lines):
                new_lines.append(line)
                new_lines.append("    'NEXT_PRICE', 'NEXT_PRICE_EFFECTIVE_DATE',")
            elif 'F.col(\'NEW_PRICE_EFFECTIVE_DATE\').alias(\'newPriceEffectiveDate\'),' in line:
                new_lines.append(line)
                new_lines.append("                    F.col('NEXT_PRICE').alias('nextPrice'),")
                new_lines.append("                    F.col('NEXT_PRICE_EFFECTIVE_DATE').alias('nextPriceEffectiveDate'),")
            elif "F.col('MULTI_FACTOR_QUANTITY').alias('multiFactorQuantity')," in line and 'select(' in ''.join(lines):
                new_lines.append(line)
                new_lines.append("                F.col('NEXT_PRICE').alias('nextPrice'),")
                new_lines.append("                F.col('NEXT_PRICE_EFFECTIVE_DATE').alias('nextPriceEffectiveDate'),")
            else:
                new_lines.append(line)
        return '\n'.join(new_lines)
    return old_code

def run_mcp_bugfix(jira_number, summary, description):
    """
    Implements the MCP bug fix workflow using git and GitHub CLI tools, following Agents.md.
    Finds the most relevant file to fix based on the Jira description/summary.
    """
    status_steps = [f"Received request to fix {jira_number}"]
    branch = f"fb_{jira_number}"
    commit_msg = f"fix({jira_number}): {summary}"
    pr_body = f"Jira: {jira_number}\n\n{description}"
    try:
        # 1. Ensure main is up to date
        status_steps.append("Checking out main and pulling latest changes...")
        subprocess.run(["git", "checkout", "main"], check=True)
        subprocess.run(["git", "pull"], check=True)
        # 2. Create feature branch
        status_steps.append(f"Creating/updating feature branch {branch}...")
        subprocess.run(["git", "checkout", "-B", branch], check=True)

        # 3. Find the most relevant file to fix (AI)
        status_steps.append("AI: Selecting the most relevant file to fix...")
        candidate_files = [f for f in os.listdir('.') if f.endswith('.py') or f.endswith('.sql')]
        best_file = ai_select_file(summary, description, candidate_files)
        if not best_file:
            raise Exception("No candidate file found to apply the fix.")
        status_steps.append(f"Selected file for fix: {best_file}")

        # 4. AI: Generate the code patch
        with open(best_file, 'r', encoding='utf-8') as f:
            old_code = f.read()
        new_code = ai_generate_patch(summary, description, old_code)
        if new_code == old_code:
            status_steps.append("AI did not generate any changes. No fix applied.")
            return status_steps, "No code changes were made."
        with open(best_file, 'w', encoding='utf-8') as f:
            f.write(new_code)
        status_steps.append(f"Applied AI-generated fix to {best_file}.")

        # 5. Stage and commit
        subprocess.run(["git", "add", best_file], check=True)
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        status_steps.append(f"Committed fix with message: {commit_msg}")
        # 6. Push branch
        subprocess.run(["git", "push", "--set-upstream", "origin", branch], check=True)
        status_steps.append(f"Pushed branch {branch} to origin.")
        # 7. Create PR using GitHub CLI (gh)
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
