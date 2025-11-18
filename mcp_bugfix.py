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
    Enhanced: Handles DataFrame columns, SQL SELECT, and .withColumn chains for column addition.
    """
    import re
    # Try to extract column names to add from the Jira description or AI suggestion in comments
    columns_to_add = []
    # 1. Look for explicit column addition in Jira description
    add_column_pattern = re.compile(r"(?:add|missing|include)\s+column[s]?\s*([A-Za-z0-9_', ]+)", re.IGNORECASE)
    match = add_column_pattern.search(jira_description)
    if match:
        columns_to_add = [c.strip(" '") for c in match.group(1).split(',') if c.strip()]
    # 2. Look for AI suggestion in code comments
    ai_suggestion_pattern = re.compile(r"AI Suggestion.*?:.*?(add|include|missing)[^\n]*column[s]?[^:]*: ([A-Za-z0-9_', ]+)", re.IGNORECASE)
    ai_match = ai_suggestion_pattern.search(old_code)
    if ai_match:
        ai_cols = [c.strip(" '") for c in ai_match.group(2).split(',') if c.strip()]
        for col in ai_cols:
            if col not in columns_to_add:
                columns_to_add.append(col)
    # 3. If still nothing, look for missing columns in DataFrame assignments
    if not columns_to_add:
        # Try to find selected_columns assignment and see if any columns are referenced elsewhere but missing
        import ast
        try:
            tree = ast.parse(old_code)
            all_names = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    all_names.add(node.id)
            # Look for selected_columns assignment
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    if hasattr(node.targets[0], 'id') and node.targets[0].id == 'selected_columns':
                        if isinstance(node.value, (ast.List, ast.Tuple)):
                            present = set()
                            for elt in node.value.elts:
                                if isinstance(elt, ast.Str):
                                    present.add(elt.s)
                            # If any names are referenced elsewhere but not present, add them
                            missing = all_names - present
                            for col in missing:
                                if col not in columns_to_add:
                                    columns_to_add.append(col)
        except Exception:
            pass
    if not columns_to_add:
        return old_code
    code = old_code
    # 1. DataFrame column list (Python)
    if 'selected_columns' in code:
        lines = code.splitlines()
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
        code = '\n'.join(new_lines)
    # 2. SQL SELECT
    if re.search(r'select\s', code, re.IGNORECASE):
        select_match = re.search(r'(select\s+)([\s\S]+?)(from\s)', code, re.IGNORECASE)
        if select_match:
            select_cols = select_match.group(2)
            for col in columns_to_add:
                col_str = col.strip(" '")
                if col_str.lower() not in select_cols.lower():
                    select_cols = select_cols + f', {col_str}'
            code = code[:select_match.start(2)] + select_cols + code[select_match.end(2):]
    # 3. .withColumn chains (PySpark)
    # Add a .withColumn for each new column if not present
    for col in columns_to_add:
        col_str = col.strip(" '")
        if f'.withColumn("{col_str}"' not in code:
            # Add after the last .withColumn in the file
            last_withcol = code.rfind('.withColumn(')
            if last_withcol != -1:
                insert_point = code.find('\n', last_withcol)
                code = code[:insert_point+1] + f"df = df.withColumn('{col_str}', df['{col_str}'])\n" + code[insert_point+1:]
    return code

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
