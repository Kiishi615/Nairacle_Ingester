"""Publish visited and retry URLs without touching the runner checkout."""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def publish(repo: Path, branch: str, attempts: int = 3) -> None:
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")

    def git(*args, input=None, extra_env=None, check=True):
        return subprocess.run(
            ["git", *args], cwd=repo, input=input, text=True,
            encoding="utf-8", capture_output=True, check=check,
            env={**env, **(extra_env or {})},
        )

    git("check-ref-format", f"refs/heads/{branch}")
    # Missing history is an error, not an invitation to create an empty file.
    local_urls = set((repo / "visited_urls.txt").read_text(encoding="utf-8").splitlines())
    if not local_urls:
        raise ValueError("Refusing to publish empty visited history")

    pending_path = repo / "scraped_today.jsonl"
    resolved_retries = set()
    pending_urls = set()
    if pending_path.exists():
        for line in pending_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                url = json.loads(line)["url"]
                if not url or "\r" in url or "\n" in url:
                    raise ValueError("Invalid pending article URL")
                pending_urls.add(url)
        initial_retries = set(git("show", "HEAD:retry_urls.txt").stdout.splitlines())
        resolved_retries = initial_retries - pending_urls

    for attempt in range(attempts):
        git("fetch", "--no-tags", "origin", f"refs/heads/{branch}")
        parent = git("rev-parse", "FETCH_HEAD").stdout.strip()
        remote_text = git("show", f"{parent}:visited_urls.txt").stdout
        remote_urls = set(remote_text.splitlines())
        combined = local_urls | remote_urls
        remote_retries = set(git("show", f"{parent}:retry_urls.txt").stdout.splitlines())
        retries = ((remote_retries - resolved_retries) | pending_urls) - combined
        if combined == remote_urls and retries == remote_retries:
            print("Repository already contains the current visited and retry URLs.")
            return

        # Start from the latest remote tree, changing only the two state files.
        # This also preserves unrelated commits made while ingestion was running.
        with tempfile.TemporaryDirectory(prefix="visited-publish-") as temporary:
            commit_env = {
                "GIT_INDEX_FILE": str(Path(temporary) / "index"),
                "GIT_AUTHOR_NAME": "github-actions[bot]",
                "GIT_AUTHOR_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
                "GIT_COMMITTER_NAME": "github-actions[bot]",
                "GIT_COMMITTER_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
            }
            git("read-tree", parent, extra_env=commit_env)
            for filename, urls in {
                "visited_urls.txt": combined,
                "retry_urls.txt": retries,
            }.items():
                contents = "".join(url + "\n" for url in sorted(urls))
                blob = git("hash-object", "-w", "--stdin", input=contents).stdout.strip()
                git("update-index", "--add", "--cacheinfo", "100644", blob,
                    filename, extra_env=commit_env)
            tree = git("write-tree", extra_env=commit_env).stdout.strip()
            commit = git(
                "commit-tree", tree, "-p", parent,
                input="Save confirmed ingestion and retry queue [skip ci]\n",
                extra_env=commit_env,
            ).stdout.strip()
            result = git("push", "origin", f"{commit}:refs/heads/{branch}", check=False)
        if result.returncode == 0:
            print(f"Published {len(combined)} visited and {len(retries)} retry URLs in {commit}.")
            return
        print(f"Publish attempt {attempt + 1} failed; re-reading the remote branch.", file=sys.stderr)
    raise RuntimeError("Unable to publish ingestion state. Check the run's ingestion-state artifact.")


if __name__ == "__main__":
    publish(Path(__file__).resolve().parents[1], os.environ["DEFAULT_BRANCH"])
