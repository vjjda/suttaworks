# Path: src/db_updater/handlers/git_release/git_release_api.py
import logging
import os
import re
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

__all__ = [
    "get_github_headers",
    "parse_repo_url",
    "get_release_info",
]


def get_github_headers() -> Optional[Dict[str, str]]:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    if token:
        log.info("Đã tìm thấy GITHUB_TOKEN. Sử dụng request đã xác thực.")
        return {"Authorization": f"token {token}"}
    log.info("Không tìm thấy GITHUB_TOKEN. Sử dụng request ẩn danh.")
    return None


def parse_repo_url(url: str) -> Optional[Tuple[str, str]]:
    match = re.search(r"github\.com/([^/]+)/([^/]+)", url)
    if match:
        groups = match.groups()
        if len(groups) == 2:
            return (groups[0], groups[1])
    log.error(f"URL repo không hợp lệ: {url}")
    return None


def get_release_info(
    owner: str, repo: str, version: str, headers: Optional[Dict[str, str]]
) -> Optional[Dict[str, Any]]:
    if version == "latest":
        url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    else:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{version}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi gọi GitHub API cho repo '{owner}/{repo}': {e}")
        return None
