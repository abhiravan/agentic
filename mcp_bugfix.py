import os
import tempfile
import subprocess

def run_mcp_bugfix(jira_number, summary, description):
    """
    Triggers the MCP bug fix workflow using the mcp-server CLI and returns status steps and message.
    """
    status_steps = [f"Received request to fix {jira_number}"]
    github_url = os.getenv('GITHUB_URL', 'https://github.com/abhiravan/agentic')
    github_user = os.getenv('GITHUB_USER', 'abhips10@gmail.com')
    github_token = os.getenv('GITHUB_TOKEN')  # Only from env, not hardcoded

    prompt = f"""
Jira: {jira_number}\nSummary: {summary}\nDescription: {description}\n\nFollow the bug fix workflow in Agents.md.\n"""
    with tempfile.NamedTemporaryFile('w+', delete=False, suffix='.txt') as f:
        f.write(prompt)
        prompt_path = f.name

    try:
        status_steps.append("Triggering MCP bug fix workflow via MCP server tools...")
        result = subprocess.check_output([
            'mcp-server',
            'bugfix',
            '--jira', jira_number,
            '--prompt', prompt_path
        ], env=os.environ, stderr=subprocess.STDOUT, text=True)
        status_steps.append(result)
        status_steps.append("MCP workflow completed. PR created or updated.")
        message = f"Fix workflow initiated for {jira_number}."
    except subprocess.CalledProcessError as e:
        status_steps.append(f"MCP tool error: {e.output}")
        message = f"Failed to trigger fix workflow: {e.output}"
    except Exception as e:
        status_steps.append(f"Error: {str(e)}")
        message = f"Failed to trigger fix workflow: {str(e)}"
    finally:
        try:
            os.remove(prompt_path)
        except Exception:
            pass
    return status_steps, message
