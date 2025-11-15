# Agent Bug Fix Workflow Guide

Tool-driven process for fixing exactly ONE Jira bug per session using MCP tools (no raw shell commands). All runtime actions must occur inside the local Python virtual environment `.venv`. If `.venv` does not exist, create it before proceeding (environment initialization MCP tool). 

---
## 1. Single-Issue Enforcement
Only one Jira ID (pattern: `JIRA-####`) is allowed in a request.
- If more than one ID is detected: abort with response: `Multiple issues detected – fix one at a time.`
- Do not combine unrelated fixes or opportunistic refactors.

---
## 2. Branch Strategy
Branch naming: `fb_<JIRA-ID>` (example: `fb_JIRA-1234`).

Lifecycle (performed via Version Control MCP tool):
1. Ensure `main` is up to date (fetch/fast-forward).
2. If current branch = target `fb_<JIRA-ID>`, reuse it; else create new branch from latest `main`.
3. Never reuse a bug-fix branch for a different Jira ID.

---
## 3. Environment & Dependencies
Prerequisites:
- Python ≥ 3.10.
- Virtual environment: `.venv/` directory present. If absent, create via environment configuration MCP tool.

Dependency handling (Package Install MCP tool):
- Install pinned packages from `requirements.txt` inside `.venv`.
- No ad-hoc upgrades unless part of a separate maintenance task.

---
## 5. Bug Fix Flow
1. Identify affected code path.
2. Fix the bug.
3. Verify the Fix.

---
## 6. Commit Standards
Conventional commit format with Jira reference:
`fix(JIRA-1234): concise description of resolved defect`

Rules:
- Prefix verb: fix | test | docs | refactor | chore (avoid ambiguous labels).
- Keep summary ≤ 100 chars.
- Extended rationale optional in body (excludes secrets/PII).
- Only include files relevant to the bug and its test.

Pre-commit MCP sequence:

1. Stage only necessary changes; avoid unrelated formatting.
2. Create commit with correct message.

---
## 7. Pull Request Protocol
Target branch: `main`.
PR Title mirrors commit style: `fix(JIRA-1234): concise summary`.

PR Body Template:
```
## Jira
JIRA-1234

## Problem
Describe observed faulty behavior.

## Root Cause
Underlying reason (logic flaw, missing validation, incorrect return type, etc.).

## Fix Summary
High-level what changed (no low-level noise).

## Validation
Environment prepared via MCP tools.
Dependencies installed.

## Rollback
Revert PR or reset to previous commit.
```

## 8. After PR Creation
- Await review; apply requested changes with full test cycle.
- No direct pushes to `main` for bug fixes.
- Squash merge acceptable if final message preserves Jira ID.

---
## 9. Multi-Issue Rejection Logic
Detection: >1 distinct Jira token in prompt.
Response: `Multiple issues supplied. Please provide exactly one Jira ID.`
Do not partially proceed—abort cleanly.

---
## 10. Fast Reference Checklist
1. Single Jira ID confirmed.
2. Branch `fb_<JIRA-ID>` ready.
3. `.venv` exists & active.
4. Dependencies Verified.
5. Fix applied.
6. Commit.
7. PR opened with template.
8. Await review; maintain green state.

---
## 11. FAQ
**Why require a failing test first?** Ensures verifiable fix & guards against regression.
**Can I fix two typos?** Only if both are part of the same Jira bug scope.
**What if `.venv` breaks?** Recreate environment, reinstall pinned deps, re-run tests before continuing.

---
## 12. Tool Usage Summary
- Environment setup: Python environment configuration MCP tool
- Package installation: Python packages install MCP tool
- Branch / commits / PR: Version control MCP tool + GitHub MCP tools
- Behavior verification: Run app & HTTP request tools (where available)

---
Keep changes surgical, traceable, and always validated within `.venv`.
