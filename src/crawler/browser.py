"""
Browser bridge — wraps opencli browser commands as Python functions.

Uses the user's own Chrome browser via opencli, which naturally bypasses
Cloudflare and other anti-bot protections.
"""

import json
import subprocess
import time
from typing import Optional

from loguru import logger


def _run_opencli(*args: str, timeout: int = 30) -> str:
    """Execute an opencli browser command and return stdout."""
    cmd = ["opencli", "browser", *args]
    logger.debug(f"Running: {' '.join(cmd)}")
    
    # Ensure /opt/homebrew/bin is in PATH (venv activation may strip it)
    import os
    env = os.environ.copy()
    path = env.get("PATH", "")
    if "/opt/homebrew/bin" not in path:
        env["PATH"] = f"/opt/homebrew/bin:{path}"
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode != 0:
            logger.warning(f"opencli stderr: {result.stderr.strip()}")
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        logger.error(f"opencli command timed out after {timeout}s: {' '.join(cmd)}")
        return ""
    except FileNotFoundError:
        logger.error("opencli not found. Install with: npm install -g @jackwener/opencli")
        raise


def open_url(url: str, wait: float = 2.0) -> str:
    """Navigate Chrome to a URL, wait for page load."""
    result = _run_opencli("open", url)
    if wait > 0:
        time.sleep(wait)
    return result


def eval_js(js_code: str, timeout: int = 30) -> str:
    """Execute JavaScript in the browser page context and return result."""
    return _run_opencli("eval", js_code, timeout=timeout)


def eval_js_json(js_code: str, timeout: int = 30) -> Optional[dict | list]:
    """Execute JS that returns JSON, parse and return as Python object."""
    raw = eval_js(js_code, timeout=timeout)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"Failed to parse JSON from eval result: {raw[:200]}")
        return None


def get_page_title() -> str:
    """Get current page title."""
    return _run_opencli("get", "title")


def get_page_url() -> str:
    """Get current page URL."""
    return _run_opencli("get", "url")


def scroll_down(amount: int = 1000) -> str:
    """Scroll down the page."""
    return _run_opencli("scroll", "down", "--amount", str(amount))


def close_browser() -> str:
    """Close the automation window."""
    return _run_opencli("close")
