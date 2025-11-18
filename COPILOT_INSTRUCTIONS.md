# Copilot Instructions for AI Agents

## Project Overview
- This repository provides tools and automation for agent-driven bug fixing, Jira integration, and GitHub PR workflows for a merchandising or data engineering environment.
- Key files and directories:
  - `app.py`: Flask web UI for fetching Jira issues and triggering automated bug fix workflows.
  - `mcp_bugfix.py`: Encapsulates logic to invoke the MCP server tools for automated bug fixing and PR creation.
  - `mcp.json`: Configuration for MCP server tools (GitHub integration, tool list).
  - `templates/index.html`: Web UI template for interacting with Jira and triggering fixes.
  - `.env`: Stores environment variables for Jira and GitHub credentials.
  - Python scripts (e.g., `nt_pchg_audit.py`, `price_load.py`): Data processing, audit, and price management logic.
  - SQL files (e.g., `complex_promo.sql`, `impact_report.sql`): Data queries and reporting logic.
  - `inbound/`: Contains documentation and possibly inbound data or scripts.

## Data Flow & Architecture
- The web UI allows users to enter a Jira number, fetch its summary/description, and trigger a bug fix workflow.
- The backend uses Jira REST API to fetch issue details and MCP server tools to automate the fix and PR process on GitHub.
- Environment variables are loaded from `.env` for all secrets and credentials.
- All bug fix automation follows the workflow described in `Agents.md` (branching, commit, PR, validation, etc.).

## Key Patterns & Conventions
- All secrets (Jira/GitHub tokens) must be stored in `.env` and never hardcoded in code.
- The MCP workflow is triggered via the `mcp-server` CLI, using prompts that include Jira number, summary, and description.
- The UI is designed for clarity and step-by-step feedback, showing each stage of the fix process.
- Python scripts for data processing (e.g., `nt_pchg_audit.py`) follow explicit, verbose transformation and archiving patterns.

## Developer & Agent Workflows
- To run locally: set up `.env` with required credentials, install dependencies from `requirements.txt`, and run `app.py`.
- To trigger a bug fix: enter a Jira number in the UI, fetch details, and click "Fix Issue" to start the MCP workflow.
- The MCP workflow will:
  1. Create a branch for the Jira issue.
  2. Apply the fix as described in the Jira description.
  3. Commit and push changes.
  4. Open a PR to `main` with a conventional commit message.
  5. Report each step in the UI.
- For data flow or ETL changes, reference the patterns in the Python scripts and SQL files.

## External Integrations
- Jira: Used for issue tracking and fetching bug details.
- GitHub: Used for version control, PRs, and MCP automation.
- MCP server tools: Used for automating the bug fix and PR workflow.

## Examples
- See `app.py` and `mcp_bugfix.py` for the full automation and integration pattern.
- See `templates/index.html` for the UI/UX conventions.
- See Python and SQL files for data processing and reporting logic.

---

**If you are an AI agent, follow these conventions and reference the above files for implementation details. Always use environment variables for secrets, and follow the workflow in `Agents.md` for bug fixes.**
