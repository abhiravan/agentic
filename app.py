from flask import Flask, render_template, request, jsonify
import os
import requests
from dotenv import load_dotenv
from mcp_bugfix import run_mcp_bugfix, ai_select_file
import os
import requests
import importlib
import json
import sys
import types
import traceback

def call_llm_patch(model_provider, prompt, old_code, jira_summary, jira_description):
    """
    Call the selected LLM provider (gemini or azure) to generate a code patch.
    """
    if model_provider == 'gemini':
        try:
            import google.generativeai as genai
        except ImportError:
            raise ImportError("google-generativeai package not installed. Please install it for Gemini support.")
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise Exception('GEMINI_API_KEY not set in environment.')
        genai.configure(api_key=api_key)
        model_name = os.getenv('GEMINI_MODEL', 'gemini-1.5-flash')
        model = genai.GenerativeModel(model_name)
        # Compose prompt for Gemini
        full_prompt = f"""
You are an expert Python/SQL code patcher. Given the following Jira summary and description, and the current code, generate the fixed code as a single code block. Only output the new code, no explanations.

Jira Summary: {jira_summary}
Jira Description: {jira_description}

Current Code:
{old_code}
"""
        response = model.generate_content(full_prompt)
        # Gemini returns a response object; extract text
        if hasattr(response, 'text'):
            return response.text
        elif hasattr(response, 'result'):
            return response.result
        elif hasattr(response, 'candidates') and response.candidates:
            return response.candidates[0].text
        else:
            return str(response)
    elif model_provider == 'azure':
        try:
            from openai import AzureOpenAI
        except ImportError:
            raise ImportError("openai package not installed. Please install it for Azure OpenAI support.")
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        api_version = os.getenv('AZURE_OPENAI_API_VERSION', '2023-07-01-preview')
        if not api_key or not endpoint:
            raise Exception('AZURE_OPENAI_API_KEY or AZURE_OPENAI_ENDPOINT not set in environment.')
        client = AzureOpenAI(azure_endpoint=endpoint, api_key=api_key, api_version=api_version)
        deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT', 'gpt-4')
        full_prompt = f"""
You are an expert Python/SQL code patcher. Given the following Jira summary and description, and the current code, generate the fixed code as a single code block. Only output the new code, no explanations.

Jira Summary: {jira_summary}
Jira Description: {jira_description}

Current Code:
{old_code}
"""
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": full_prompt}],
            max_tokens=4096,
            temperature=0.1
        )
        # Azure returns a response object; extract text
        if hasattr(response, 'choices') and response.choices:
            return response.choices[0].message.content
        return str(response)
    else:
        raise Exception(f"Unknown model provider: {model_provider}")
from github_client import GitHubClient

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
    status_steps = [f"Received request to fix {jira_number}"]
    import patch_utils
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    REPO = "abhiravan/agentic"
    github_client = GitHubClient(GITHUB_TOKEN, REPO)
    model_provider = os.getenv('LLM_PROVIDER', 'gemini')  # 'gemini' or 'azure'
    try:
        candidate_files = [f for f in os.listdir('.') if f.endswith('.py') or f.endswith('.sql')]
        best_file = ai_select_file(summary, description, candidate_files)
        if not best_file:
            status_steps.append("No candidate file found to apply the fix.")
            return jsonify({'status_steps': status_steps, 'message': 'No code changes were made.'})
        status_steps.append(f"Selected file for fix: {best_file}")
        with open(best_file, 'r', encoding='utf-8') as f:
            old_code = f.read()
        # Use LLM to generate patch
        new_code = call_llm_patch(model_provider, '', old_code, summary, description)
        if not new_code or new_code.strip() == old_code.strip():
            status_steps.append("AI did not generate any changes. No fix applied.")
            return jsonify({'status_steps': status_steps, 'message': 'No code changes were made.'})
        # 1. Ensure main is up to date and create branch
        import subprocess
        branch = f"fb_{jira_number}"
        subprocess.run(["git", "checkout", "main"], check=True)
        subprocess.run(["git", "pull"], check=True)
        subprocess.run(["git", "checkout", "-B", branch], check=True)
        status_steps.append(f"Created/checked out branch {branch}.")
        # 2. Apply patch
        patch_utils.apply_patch(best_file, new_code)
        status_steps.append(f"Applied LLM-generated fix to {best_file}.")
        # 3. Commit
        commit_msg = f"fix({jira_number}): {summary}"
        patch_utils.commit_all(commit_msg)
        status_steps.append(f"Committed fix with message: {commit_msg}")
        # 4. Push branch
        patch_utils.push_branch(branch)
        status_steps.append(f"Pushed branch {branch} to origin.")
        # 5. Create PR via GitHub API
        pr = github_client.create_pull_request(
            title=commit_msg,
            body=description,
            head=branch,
            base="main"
        )
        status_steps.append(f"PR created: {pr.get('html_url', pr.get('url'))}")
        return jsonify({'status_steps': status_steps, 'message': f"PR created: {pr.get('html_url', pr.get('url'))}"})
    except Exception as e:
        status_steps.append(f"Error: {str(e)}")
        return jsonify({'status_steps': status_steps, 'message': f"Failed to trigger fix workflow: {str(e)}"})

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
