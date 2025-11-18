import os
import requests
from dotenv import load_dotenv
from mcp_bugfix import run_mcp_bugfix

load_dotenv()

JIRA_URL = 'https://mercagent.atlassian.net'
JIRA_USER = os.getenv('JIRA_USER')
JIRA_TOKEN = os.getenv('JIRA_TOKEN')

def fetch_jira_issue(jira_number):
    url = f"{JIRA_URL}/rest/api/3/issue/{jira_number}"
    auth = (JIRA_USER, JIRA_TOKEN)
    headers = {"Accept": "application/json"}
    resp = requests.get(url, auth=auth, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Jira fetch failed: {resp.status_code} {resp.text}")
    return resp.json()

def extract_jira_description(description_field):
    if isinstance(description_field, str):
        return description_field
    if not description_field:
        return ''
    if isinstance(description_field, dict) and description_field.get('type') == 'doc':
        return adf_to_text(description_field)
    return str(description_field)

def adf_to_text(adf):
    if not adf or not isinstance(adf, dict):
        return ''
    result = []
    def walk(node):
        if not isinstance(node, dict):
            return
        node_type = node.get('type')
        if node_type == 'text':
            result.append(node.get('text', ''))
        for child in node.get('content', []):
            walk(child)
        if node_type in ('paragraph', 'heading', 'blockquote'):
            result.append('\n')
    walk(adf)
    return ''.join(result).strip()

if __name__ == "__main__":
    jira_number = "AG-3"
    print(f"Fetching Jira details for {jira_number}...")
    issue = fetch_jira_issue(jira_number)
    summary = issue.get('fields', {}).get('summary', '')
    description_field = issue.get('fields', {}).get('description', '')
    description = extract_jira_description(description_field)
    print(f"Summary: {summary}\nDescription: {description}\n")
    print("Triggering MCP bug fix workflow...")
    status_steps, message = run_mcp_bugfix(jira_number, summary, description)
    print("\n--- MCP Workflow Output ---")
    for step in status_steps:
        print(step)
    print(message)