#!/usr/bin/env python3
"""Push the generated site to the gh-pages branch."""
import argparse
import os
import subprocess
import sys
from pathlib import Path
import shutil
import tempfile
from datetime import datetime


def run(*args, cwd=None):
    subprocess.check_call(args, cwd=cwd)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', default='site', help='Directory containing the generated site')
    parser.add_argument('--branch', default='gh-pages', help='Branch to push to')
    parser.add_argument('--remote', default='origin', help='Remote name')
    parser.add_argument('--repo', default=os.environ.get('REPO_URL', '.'),
                        help='Repository URL or path to clone')
    args = parser.parse_args()

    site_dir = Path(args.site)
    if not site_dir.is_dir():
        print(f"Site directory {site_dir} not found", file=sys.stderr)
        sys.exit(1)

    tmpdir = Path(tempfile.mkdtemp())
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    repo_url = args.repo
    if token and repo_url.startswith('https://') and '@' not in repo_url:
        repo_url = repo_url.replace('https://', f'https://{token}@')

    run('git', 'clone', repo_url, str(tmpdir))
    fetch = subprocess.run(['git', 'fetch', args.remote, args.branch], cwd=tmpdir)
    if fetch.returncode == 0:
        run('git', 'checkout', '-B', args.branch, f'{args.remote}/{args.branch}',
            cwd=tmpdir)
    else:
        run('git', 'checkout', '--orphan', args.branch, cwd=tmpdir)
    if os.environ.get('GH_NAME'):
        run('git', 'config', 'user.name', os.environ['GH_NAME'], cwd=tmpdir)
    if os.environ.get('GH_EMAIL'):
        run('git', 'config', 'user.email', os.environ['GH_EMAIL'], cwd=tmpdir)

    for item in tmpdir.iterdir():
        if item.name == '.git':
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()

    for item in site_dir.iterdir():
        dest = tmpdir / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)

    run('git', 'add', '.', cwd=tmpdir)
    result = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=tmpdir)
    if result.returncode != 0:
        msg = f"Update site {datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}"
        run('git', 'commit', '-m', msg, cwd=tmpdir)
        run('git', 'remote', 'set-url', args.remote, repo_url, cwd=tmpdir)
        run('git', 'push', args.remote, args.branch, cwd=tmpdir)
    else:
        print('No changes to commit')


if __name__ == '__main__':
    main()
