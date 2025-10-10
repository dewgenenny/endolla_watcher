#!/usr/bin/env python3
"""Utility to update image tags within a Kustomize file."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List


def update_image_tag(text: str, image: str, tag: str) -> str:
    lines: List[str] = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("- name:"):
            parts = stripped.split(":", 1)
            if len(parts) != 2:
                continue
            current_name = parts[1].strip()
            if current_name != image:
                continue

            j = index + 1
            while j < len(lines):
                next_stripped = lines[j].strip()
                if next_stripped.startswith("- name:"):
                    break
                if next_stripped.startswith("newTag:"):
                    indent = lines[j][: len(lines[j]) - len(lines[j].lstrip())]
                    lines[j] = f"{indent}newTag: {tag}"
                    return "\n".join(lines) + "\n"
                j += 1

            base_indent = lines[index][: len(lines[index]) - len(lines[index].lstrip())] + "  "
            lines.insert(j, f"{base_indent}newTag: {tag}")
            return "\n".join(lines) + "\n"

    # If we did not find the image block, append a new one.
    if lines and lines[-1] != "":
        lines.append("")

    if any(line.strip() == "images:" for line in lines):
        lines.append(f"  - name: {image}")
        lines.append(f"    newName: {image}")
        lines.append(f"    newTag: {tag}")
    else:
        lines.append("images:")
        lines.append(f"  - name: {image}")
        lines.append(f"    newName: {image}")
        lines.append(f"    newTag: {tag}")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path, help="Path to kustomization.yaml")
    parser.add_argument("image", help="Container image name to update")
    parser.add_argument("tag", help="Tag to set for the image")
    args = parser.parse_args()

    original = args.file.read_text()
    updated = update_image_tag(original, args.image, args.tag)
    args.file.write_text(updated)


if __name__ == "__main__":
    main()
