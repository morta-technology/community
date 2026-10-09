"""Build or verify MortaAgent using Python 3.11+ and the standard library.

Run `python build.py` to build, or `python build.py --check` for read-only checks.
Neither mode connects to Morta, installs a connector or executes model scenarios.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import tomllib
from urllib.parse import unquote
import zipfile

ROOT = Path(__file__).resolve().parent
SKILL = ROOT / "morta-agent"
EXPECTED = {
    "LICENSE",
    "SKILL.md",
    "assets/hub-ai-instructions.md",
    "references/setup.md",
    "references/hub-rules.md",
    "references/design-patterns.md",
    "references/morta-practice.md",
    "references/presentation.md",
    "references/data-model.md",
    "references/forms-workflows.md",
    "references/documents.md",
    "references/imports-migrations.md",
    "references/sharing.md",
}
LINK = re.compile(r"(?<!!)\[[^\]\n]+\]\(([^)\s]+)\)")


def digest(data):
    """Return a SHA-256 digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    """Stop with a specific diagnostic when a release invariant fails."""
    if not condition:
        raise ValueError(message)


def slug(value):
    """Map these documents' Markdown headings to GitHub-style anchors."""
    return re.sub(r"[^\w\- ]", "", value.strip().lower()).replace(" ", "-")


def archive_bytes(files):
    """Create a deterministic single-root skill ZIP in memory."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            relative = path.relative_to(SKILL).as_posix()
            info = zipfile.ZipInfo(f"morta-agent/{relative}", (2000, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            archive.writestr(
                info,
                path.read_bytes(),
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )
    return buffer.getvalue()


def check_links(paths, archive_name):
    """Resolve local Markdown links; support entry or full-repository checkout."""
    checked = 0
    for path in paths:
        content = path.read_text(encoding="utf-8")
        for target in LINK.findall(content):
            if target.startswith(("https://", "http://", "mailto:")):
                continue
            location, _, anchor = unquote(target).partition("#")
            resolved = (
                (path.parent / location).resolve() if location else path.resolve()
            )
            if resolved == ROOT / archive_name:
                checked += 1
                continue  # Verified as archive bytes separately.
            require(resolved.is_file(), f"Missing link: {path.name} -> {target}")
            if anchor:
                text = resolved.read_text(encoding="utf-8")
                headings = {
                    slug(m[1]) for m in re.finditer(r"^#{1,6}\s+(.+?)\s*$", text, re.M)
                }
                require(anchor in headings, f"Missing anchor: {path.name} -> {target}")
            checked += 1
    return checked


def verify_archive(data, files):
    """Verify archive safety, CRC, inventory and equality with the source."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        expected = {f"morta-agent/{p}" for p in EXPECTED}
        names = archive.namelist()
        require(len(names) == len(set(names)), "Duplicate archive entries")
        require(set(names) == expected, "Unexpected archive inventory")
        require(archive.testzip() is None, "Archive integrity failed")
        for path in files:
            name = "morta-agent/" + path.relative_to(SKILL).as_posix()
            require(
                archive.read(name) == path.read_bytes(), f"Archive mismatch: {name}"
            )


def main():
    """Validate source and either rebuild or check the committed distribution."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Verify without writing files"
    )
    args = parser.parse_args()
    files = sorted(p for p in SKILL.rglob("*") if p.is_file())
    require(
        {p.relative_to(SKILL).as_posix() for p in files} == EXPECTED,
        "Unexpected skill files",
    )
    require(
        not any(p.is_symlink() for p in files),
        "Package must contain files, not symlinks",
    )
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    version_match = re.search(r'^  version: "(\d+\.\d+\.\d+)"$', skill, re.M)
    require(version_match is not None, "Missing semantic version")
    version = version_match[1]
    description = re.search(r"^description: (.+)$", skill, re.M)
    require(
        description is not None and len(description[1]) <= 200,
        "Description exceeds host limit",
    )
    require(
        "status: test-candidate" in skill, "Review release status before changing it"
    )
    archive_name = f"morta-agent-v{version}.zip"
    require(
        version in (ROOT / "START-HERE.md").read_text(encoding="utf-8"),
        "Stale setup version",
    )
    require(
        (SKILL / "LICENSE").read_text(encoding="utf-8")
        == (ROOT.parents[2] / "LICENSE").read_text(encoding="utf-8"),
        "Repository license mismatch",
    )
    setup = (SKILL / "references/setup.md").read_text(encoding="utf-8")
    claude = json.loads(re.search(r"```json\n(.*?)\n```", setup, re.S)[1])
    codex = tomllib.loads(re.search(r"```toml\n(.*?)\n```", setup, re.S)[1])
    for config in (
        claude["mcpServers"]["morta_api"],
        codex["mcp_servers"]["morta_api"],
    ):
        require(
            config["env"]["MORTA_API_KEY"] == "REPLACE_LOCALLY_WITH_YOUR_KEY",
            "Unexpected credential",
        )
    for path in (ROOT / "tests").glob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))

    archive_path = ROOT / archive_name
    data = archive_path.read_bytes() if args.check else archive_bytes(files)
    verify_archive(data, files)
    # Generate prospective manifest before writes, so all links can be checked.
    support = sorted(
        p
        for p in ROOT.rglob("*")
        if p.is_file()
        and not p.is_relative_to(SKILL)
        and (p.suffix in {".md", ".py", ".json"} or p.name == ".gitattributes")
        and p.name != "package-manifest.json"
    )
    manifest = {
        "version": version,
        "status": "test-candidate",
        "distribution": "community; reviewed instructions plus repository MIT notice",
        "archive": archive_name,
        "sha256": digest(data),
        "files": [
            {"path": p.relative_to(ROOT).as_posix(), "sha256": digest(p.read_bytes())}
            for p in files
        ],
        "support_files": [
            {"path": p.relative_to(ROOT).as_posix(), "sha256": digest(p.read_bytes())}
            for p in support
        ],
        "validation": {
            "structural": "links, configuration syntax, license, ZIP integrity and content equality",
            "behavioural": "Original 21-case result in tests/evaluation-summary.json; not rerun by this script",
            "live_connector": "not tested",
            "claude_installation": "not performed",
            "visual_verification": "not performed",
        },
    }
    if args.check:
        require(
            load_manifest() == manifest,
            "Manifest differs from current files; rebuild intentionally",
        )
    else:
        archive_path.write_bytes(data)
        (ROOT / "package-manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
    links = check_links(sorted(ROOT.rglob("*.md")), archive_name)
    print(
        json.dumps(
            {
                "ok": True,
                "mode": "check" if args.check else "build",
                "version": version,
                "skill_files": len(files),
                "local_links": links,
                "archive_sha256": digest(data),
            }
        )
    )


def load_manifest():
    """Read the committed package manifest."""
    return json.loads((ROOT / "package-manifest.json").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
