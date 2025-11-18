import os
import subprocess
import re

def ai_select_file(jira_summary, jira_description, repo_files):
    """
    Use Copilot Instructions and repo file context to select the most relevant file for the fix.
    Prioritize files whose names or docstrings/comments match Jira keywords, and prefer .py for code, .sql for SQL.
    """
    # Combine Copilot instructions and Jira context
    copilot_keywords = set()
    # Try to extract file hints from COPILOT_INSTRUCTIONS.md
    try:
        with open('COPILOT_INSTRUCTIONS.md', 'r', encoding='utf-8') as f:
            copilot_text = f.read().lower()
            for fname in repo_files:
                if fname.lower() in copilot_text:
                    copilot_keywords.add(fname.lower())
    except Exception:
        pass
    # Build keyword set from Jira
    jira_keywords = set(re.findall(r'\w+', jira_summary + ' ' + jira_description))
    jira_keywords = {k.lower() for k in jira_keywords if len(k) > 2}
    # Score files
    best_file = None
    best_score = 0
    for fname in repo_files:
        score = 0
        fname_lower = fname.lower()
        # +2 if mentioned in Copilot instructions
        if fname_lower in copilot_keywords:
            score += 2
        # +1 for each Jira keyword in filename
        score += sum(1 for k in jira_keywords if k in fname_lower)
        # +1 if .py and summary/desc mention 'python' or 'code'
        if fname_lower.endswith('.py') and ('python' in jira_keywords or 'code' in jira_keywords):
            score += 1
        # +1 if .sql and summary/desc mention 'sql' or 'query'
        if fname_lower.endswith('.sql') and ('sql' in jira_keywords or 'query' in jira_keywords):
            score += 1
        if score > best_score:
            best_score = score
            best_file = fname
    return best_file or (repo_files[0] if repo_files else None)

def ai_generate_patch(jira_summary, jira_description, old_code):
    """
    Use simple pattern matching to add a column to a DataFrame or SQL SELECT if the Jira description requests it.
    This is a generic AI-inspired patcher for Python and SQL files.
    """
    # Extract column names to add from the Jira description
    add_column_pattern = re.compile(r"add(?:\s+column)?\s+([A-Za-z0-9_']+)", re.IGNORECASE)
    columns_to_add = add_column_pattern.findall(jira_description)
    if not columns_to_add:
        # Try to find 'missing column' or 'include column' patterns
        missing_pattern = re.compile(r"(?:missing|include)\s+column[s]?\s*([A-Za-z0-9_', ]+)", re.IGNORECASE)
        match = missing_pattern.search(jira_description)
        if match:
            columns_to_add = [c.strip(" '") for c in match.group(1).split(',') if c.strip()]
    if not columns_to_add:
        return old_code
    # Patch Python DataFrame column lists
    if 'selected_columns' in old_code:
        lines = old_code.splitlines()
        new_lines = []
        in_columns = False
        for line in lines:
            if 'selected_columns' in line and '[[' in line:
                in_columns = True
                new_lines.append(line)
                continue
            if in_columns and ']]' in line:
                # Insert new columns before closing
                for col in columns_to_add:
                    col_str = col.strip(" '")
                    if f"'{col_str}'" not in ''.join(new_lines):
                        new_lines.append(f"    '{col_str}',")
                in_columns = False
            new_lines.append(line)
        return '\n'.join(new_lines)
    # Patch SQL SELECT lists
    if re.search(r'select\s', old_code, re.IGNORECASE):
        # Try to add columns to the SELECT clause
        select_match = re.search(r'(select\s+)([\s\S]+?)(from\s)', old_code, re.IGNORECASE)
        if select_match:
            select_cols = select_match.group(2)
            for col in columns_to_add:
                col_str = col.strip(" '")
                if col_str.lower() not in select_cols.lower():
                    select_cols = select_cols + f', {col_str}'
            new_code = old_code[:select_match.start(2)] + select_cols + old_code[select_match.end(2):]
            return new_code
    return old_code

def run_mcp_bugfix(jira_number, summary, description):
    """
    Implements the MCP bug fix workflow using git and GitHub CLI tools, following Agents.md.
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
        # 2. AI: Select the most relevant file to fix
        status_steps.append("AI: Selecting the most relevant file to fix...")
        candidate_files = [f for f in os.listdir('.') if f.endswith('.py') or f.endswith('.sql')]
        best_file = ai_select_file(summary, description, candidate_files)
        if not best_file:
            raise Exception("No candidate file found to apply the fix.")
        status_steps.append(f"Selected file for fix: {best_file}")

        # 3. AI: Generate the code patch
        with open(best_file, 'r', encoding='utf-8') as f:
            old_code = f.read()
        new_code = ai_generate_patch(summary, description, old_code)
        if new_code == old_code:
            status_steps.append("AI did not generate any changes. No fix applied.")
            return status_steps, "No code changes were made."
        status_steps.append(f"Creating/updating feature branch {branch}...")
        subprocess.run(["git", "checkout", "-B", branch], check=True)
        with open(best_file, 'w', encoding='utf-8') as f:
            f.write(new_code)
        status_steps.append(f"Applied AI-generated fix to {best_file}.")

        # 4. Stage and commit
        subprocess.run(["git", "add", best_file], check=True)
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
