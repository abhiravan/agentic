import base64
import requests
from typing import List, Optional

class GitHubClient:
    def __init__(self, token: str, repo: str, base_url: str = "https://api.github.com"):
        self.token = token
        self.repo = repo
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            }
        )

    def _url(self, path: str) -> str:
        return f"{self.base_url}/repos/{self.repo}{path}"

    def get_repository(self) -> dict:
        response = self.session.get(self._url(""), timeout=15)
        response.raise_for_status()
        return response.json()

    def get_branch_sha(self, branch: str) -> str:
        resp = self.session.get(self._url(f"/git/refs/heads/{branch}"), timeout=15)
        resp.raise_for_status()
        return resp.json()["object"]["sha"]

    def create_branch(self, new_branch: str, from_branch: str = "main") -> str:
        sha = self.get_branch_sha(from_branch)
        resp = self.session.post(
            self._url("/git/refs"),
            json={"ref": f"refs/heads/{new_branch}", "sha": sha},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["ref"]

    def get_file(self, path: str, ref: str = "main") -> dict:
        resp = self.session.get(self._url(f"/contents/{path}?ref={ref}"), timeout=15)
        resp.raise_for_status()
        return resp.json()

    def update_file(self, path: str, content: str, branch: str, message: str) -> dict:
        file_info = self.get_file(path, ref=branch)
        b64_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        resp = self.session.put(
            self._url(f"/contents/{path}"),
            json={
                "message": message,
                "content": b64_content,
                "branch": branch,
                "sha": file_info["sha"],
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def create_pull_request(
        self,
        *,
        title: str,
        body: str,
        head: str,
        base: str,
        reviewers: Optional[List[str]] = None,
        draft: bool = False,
    ) -> dict:
        payload = {
            "title": title,
            "head": head,
            "base": base,
            "body": body,
            "draft": draft,
        }
        response = self.session.post(self._url("/pulls"), json=payload, timeout=30)
        response.raise_for_status()
        pr = response.json()
        if reviewers:
            self.session.post(
                self._url(f"/pulls/{pr['number']}/requested_reviewers"),
                json={"reviewers": reviewers},
                timeout=15,
            )
        return pr
