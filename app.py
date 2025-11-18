from flask import Flask, render_template, request, jsonify
import os
import requests
from dotenv import load_dotenv
from mcp_bugfix import run_mcp_bugfix, ai_select_file

load_dotenv()

app = Flask(__name__)

JIRA_URL = 'https://mercagent.atlassian.net'
JIRA_USER = os.getenv('JIRA_USER')
JIRA_TOKEN = os.getenv('JIRA_TOKEN')

@app.route('/', methods=['GET', 'POST'])
def index():
    summary = ''
    description = ''
    error = ''
    if request.method == 'POST':
        jira_number = request.form.get('jira_number', '').strip()
        if jira_number:
            try:
                issue = fetch_jira_issue(jira_number)
                summary = issue.get('fields', {}).get('summary', '')
                description = issue.get('fields', {}).get('description', '')
            except Exception as e:
                error = str(e)
    return render_template('index.html', summary=summary, description=description, error=error)

@app.route('/fetch_jira', methods=['POST'])
def fetch_jira():
    data = request.get_json()
    jira_number = data.get('jira_number', '').strip()
    if not jira_number:
        return jsonify({'error': 'Jira number required'}), 400
    try:
        issue = fetch_jira_issue(jira_number)
        summary = issue.get('fields', {}).get('summary', '')
        description_field = issue.get('fields', {}).get('description', '')
        description = extract_jira_description(description_field)
        return jsonify({'summary': summary, 'description': description})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
def extract_jira_description(description_field):
    # If already a string, return as is
    if isinstance(description_field, str):
        return description_field
    # If None or empty
    if not description_field:
        return ''
    # Atlassian Document Format (ADF) parsing
    if isinstance(description_field, dict) and description_field.get('type') == 'doc':
        return adf_to_text(description_field)
    # Fallback: str()
    return str(description_field)

# Minimal ADF to text (plain) converter
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


@app.route('/fix_issue', methods=['POST'])
def fix_issue():
    data = request.get_json()
    jira_number = data.get('jira_number', '')
    summary = data.get('summary', '')
    description = data.get('description', '')
    status_steps, message = run_mcp_bugfix(jira_number, summary, description)
    return jsonify({'status_steps': status_steps, 'message': message})

@app.route('/find_source', methods=['POST'])
def find_source():
    data = request.get_json()
    summary = data.get('summary', '')
    description = data.get('description', '')
    try:
        candidate_files = [f for f in os.listdir('.') if f.endswith('.py') or f.endswith('.sql')]
        best_file = ai_select_file(summary, description, candidate_files)
        if not best_file:
            return jsonify({'error': 'No candidate file found.'}), 404
        return jsonify({'file': best_file})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def fetch_jira_issue(jira_number):
    url = f"{JIRA_URL}/rest/api/3/issue/{jira_number}"
    auth = (JIRA_USER, JIRA_TOKEN)
    headers = {"Accept": "application/json"}
    resp = requests.get(url, auth=auth, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Jira fetch failed: {resp.status_code} {resp.text}")
    return resp.json()

if __name__ == '__main__':
    app.run(debug=True)
