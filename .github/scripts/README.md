Create issues from .github/issues/*.md

This script helps create GitHub issues from the markdown drafts in `.github/issues/`.

Usage

1. Preferred (GH CLI):

   - Install GitHub CLI (gh) and authenticate: `gh auth login`
   - Run from repository root:

```bash
./.github/scripts/create_issues.sh
```

2. Fallback (curl + GITHUB_TOKEN):

   - Export a token with repo scope:

```bash
export GITHUB_TOKEN=ghp_...
```

   - Then run the script above. The script defaults to repository `iridiumdesign/iridium-stomp`.

Notes

- The script looks for a `Title:` header and a `Body:` marker in each markdown file. The body is everything after the `Body:` line.
- If you have `jq` installed, the script will use it to produce safe JSON for the GitHub API. If not, it falls back to a Python helper for JSON escaping.
- The script will skip files that don't have a `Title:` header.

Safety

- The script is idempotent in the sense it will attempt to create an issue for each file every run; it does not check for duplicates. Consider running once and then moving or deleting processed files.
