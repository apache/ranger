#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# Apply Apache Ranger Java import layout and whitespace rules from
# dev-support/checkstyle.xml (master) and RangerCodeScheme-IntelliJ.xml:
#   import groups: third-party/org (*), javax, java, static — each separated by a blank line.

import re
import subprocess
import sys

TAB_MODULES = {
    "agents-common",
    "hive-agent",
    "embeddedwebserver",
    "agents-audit/dest-solr",
}


def classify_import(line):
    stripped = line.strip()
    if not stripped.startswith("import "):
        return None
    if stripped.startswith("import static "):
        return "static", stripped
    target = stripped[len("import ") :].rstrip(";")
    if target.startswith("java."):
        return "java", stripped
    if target.startswith("javax."):
        return "javax", stripped
    return "other", stripped


def reorder_import_block(import_lines):
    groups = {"other": [], "javax": [], "java": [], "static": []}
    for line in import_lines:
        classified = classify_import(line)
        if classified is None:
            continue
        group, stripped = classified
        groups[group].append(stripped)

    for key in groups:
        groups[key] = sorted(set(groups[key]))

    ordered = []
    for name in ("other", "javax", "java", "static"):
        if not groups[name]:
            continue
        if ordered:
            ordered.append("")
        ordered.extend(groups[name])
    return ordered


def module_for(path):
    if "/src/" in path:
        return path.split("/src/")[0]
    return path


def uses_tabs(path):
    return module_for(path) in TAB_MODULES


def normalize_whitespace(text, keep_tabs):
    lines = text.splitlines()
    cleaned = [line.rstrip() for line in lines]
    text = "\n".join(cleaned)
    if not keep_tabs:
        text = text.replace("\t", "    ")
    if text and not text.endswith("\n"):
        text += "\n"
    while text.endswith("\n\n\n"):
        text = text[:-1]
    if text.endswith("\n\n"):
        text = text[:-1]
    if not text.endswith("\n"):
        text += "\n"
    return text


def format_java(content, keep_tabs):
    lines = content.splitlines()
    pkg_idx = next((i for i, line in enumerate(lines) if line.startswith("package ")), None)
    if pkg_idx is None:
        return normalize_whitespace(content, keep_tabs)

    import_start = None
    import_end = None
    for i, line in enumerate(lines):
        if line.startswith("import "):
            if import_start is None:
                import_start = i
            import_end = i
    if import_start is None:
        return normalize_whitespace(content, keep_tabs)

    header = lines[:import_start]
    import_lines = [lines[i] for i in range(import_start, import_end + 1) if lines[i].strip()]
    tail = lines[import_end + 1 :]
    while tail and tail[0].strip() == "":
        tail = tail[1:]

    new_imports = reorder_import_block(import_lines)
    body = header + new_imports
    if new_imports:
        body.append("")
    body.extend(tail)
    return normalize_whitespace("\n".join(body), keep_tabs)


def changed_java_files():
    out = subprocess.check_output(
        ["git", "diff", "origin/ranger-2.10..HEAD", "--name-only", "--", "*.java"],
        text=True,
    )
    files = [line.strip() for line in out.splitlines() if line.strip()]
    status = subprocess.check_output(["git", "status", "--porcelain", "--", "*.java"], text=True)
    for line in status.splitlines():
        if line.startswith("??") or line.startswith(" M") or line.startswith("M "):
            path = line[3:].strip()
            if path.endswith(".java") and path not in files:
                files.append(path)
    return sorted(set(files))


def main():
    files = changed_java_files()
    if not files:
        print("No changed Java files found.")
        return 0

    updated = 0
    for path in files:
        with open(path, encoding="utf-8") as handle:
            original = handle.read()
        formatted = format_java(original, keep_tabs=uses_tabs(path))
        if formatted != original:
            with open(path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(formatted)
            updated += 1
            print(f"formatted: {path}")
    print(f"Done. Updated {updated} of {len(files)} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
