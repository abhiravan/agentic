import os
import subprocess

def apply_patch(file_path, new_code):
    """
    Write new_code to file_path, simulating a patch application.
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_code)

def commit_all(commit_msg):
    subprocess.run(["git", "add", "-A"], check=True)
    subprocess.run(["git", "commit", "-m", commit_msg], check=True)

def push_branch(branch):
    subprocess.run(["git", "push", "--set-upstream", "origin", branch], check=True)
