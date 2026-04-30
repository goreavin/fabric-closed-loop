#!/usr/bin/env python3
"""
Build a single-page HTML from process.md using pandoc + mermaid JS.
Converts ```mermaid fences into <pre class="mermaid"> blocks for client-side rendering.
"""
import re
import subprocess
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent
TEMPLATE = DOCS / "template.html"
SOURCE = DOCS / "process.md"
OUTPUT = DOCS / "index.html"


def convert_md_to_html_body(md_path: Path) -> str:
    """Use pandoc to convert markdown to HTML fragment."""
    result = subprocess.run(
        ["pandoc", "--from=gfm", "--to=html5", "--no-highlight", str(md_path)],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"pandoc error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def patch_mermaid_blocks(html: str) -> str:
    """Replace <pre><code class="language-mermaid">...</code></pre>
    with <pre class="mermaid">...</pre> for mermaid.js.
    Also strip the YAML frontmatter from inside mermaid blocks
    (mermaid.js is already configured globally via mermaid.initialize).
    """
    def strip_yaml_frontmatter(code: str) -> str:
        # Remove ---\n...\n--- at the start of mermaid content
        return re.sub(r'^---\n.*?\n---\n', '', code, flags=re.DOTALL).strip()

    def replacer(m):
        code = m.group(1)
        # Unescape HTML entities that pandoc may have introduced
        code = code.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        code = code.replace('&quot;', '"')
        code = strip_yaml_frontmatter(code)
        return f'<pre class="mermaid">\n{code}\n</pre>'

    return re.sub(
        r'<pre><code class="language-mermaid">(.*?)</code></pre>',
        replacer,
        html,
        flags=re.DOTALL,
    )


def main():
    template = TEMPLATE.read_text()
    body = convert_md_to_html_body(SOURCE)
    body = patch_mermaid_blocks(body)
    html = template.replace("<!--CONTENT-->", body)
    OUTPUT.write_text(html)
    print(f"Wrote {OUTPUT} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
