"""ODIN V2 Backend — Skills Routes

Real-time skills catalog endpoint.
Scans the skills directory and returns the current skill catalog.
"""

import os
import re
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

# Skills directory (resolved from the same home as theHermes process)
SKILLS_DIR = Path(os.path.expanduser("~/.hermes/skills"))


def _parse_skills_from_disk() -> List[Dict[str, Any]]:
    """Scan the skills directory and extract metadata from each SKILL.md."""
    skills = []
    if not SKILLS_DIR.exists():
        return skills

    for skill_path in SKILLS_DIR.rglob("SKILL.md"):
        try:
            rel_path = skill_path.relative_to(SKILLS_DIR).as_posix()
            category = skill_path.parent.name

            content = skill_path.read_text(encoding="utf-8")

            # Extract YAML frontmatter
            fm_match = re.match(r'^---\s*(.*?)\s*---\s*(.*)$', content, re.DOTALL)
            metadata = {}
            body = content
            if fm_match:
                metadata = _parse_yaml_block(fm_match.group(1))
                body = fm_match.group(2)

            name = metadata.get("name", "")
            description = metadata.get("description", "").replace("\n", " ").strip()
            version = metadata.get("version", "")
            author = metadata.get("author", "")
            license = metadata.get("license", "")
            tags = metadata.get("metadata", {}).get("hermes", {}).get("tags", []) if isinstance(metadata.get("metadata"), dict) else []
            related = metadata.get("metadata", {}).get("hermes", {}).get("related_skills", []) if isinstance(metadata.get("metadata"), dict) else []

            skills.append({
                "path": rel_path,
                "category": category,
                "name": name,
                "description": description,
                "version": version,
                "author": author,
                "license": license,
                "tags": tags,
                "related_skills": related,
                "has_script_dir": (skill_path.parent / "scripts").is_dir(),
                "has_reference_dir": (skill_path.parent / "references").is_dir(),
            })
        except Exception as e:
            # Skip unreadable skill files, log to stderr
            print(f"Warning: failed to parse {skill_path}: {e}", file=__import__("sys").stderr)

    return skills


def _parse_yaml_block(text: str) -> Dict[str, Any]:
    """Minimal YAML block parser for flat key-value pairs and simple lists."""
    result = {}
    current_key = None
    current_value = None
    in_list = False
    list_items = []

    for raw_line in text.split("\n"):
        line = raw_line.rstrip()
        if not line.strip() or line.strip().startswith("#"):
            continue

        indent = len(line) - len(line.lstrip())
        stripped = line.strip()

        # Remove inline comments
        if "#" in stripped and not (stripped.startswith('"') or stripped.startswith("'")):
            stripped = stripped.split("#", 1)[0].strip()
            if not stripped:
                continue

        if ":" in stripped:
            key, _, val = stripped.partition(":")
            key = key.strip()
            val = val.strip()

            if val == "":
                # Nested key
                current_key = key
                result[key] = {}
                current_value = result[key]
                in_list = False
                list_items = []
            else:
                # Inline value or list
                if (val.startswith("[") and val.endswith("]")) or (val.startswith("- ") or val.startswith(" -")):
                    # List
                    items = re.findall(r"['\"]?([^,'\"]+)['\"]?", val)
                    result[key] = [i.strip() for i in items if i.strip()]
                    current_key = None
                    current_value = None
                else:
                    # Simple value
                    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    result[key] = val
                    current_key = None
                    current_value = None
        else:
            # List item continuation (e.g., "- item" under a parent)
            if stripped.startswith("- "):
                item = stripped[2:].strip()
                if current_key:
                    if current_key not in result:
                        result[current_key] = []
                    result[current_key].append(item)
                elif current_value is not None and isinstance(current_value, dict):
                    # Find last list in current_value
                    for k, v in current_value.items():
                        if isinstance(v, list):
                            v.append(item)
                            break
                    else:
                        current_value["_list"] = [item]
                continue

    return result


class SkillsResponse(BaseModel):
    """Response model for skills catalog."""
    skills: List[Dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    categories: Dict[str, int] = Field(default_factory=dict)
    updated_at: Optional[str] = None
    source: str = "~/.hermes/skills"


@router.get("", response_model=SkillsResponse)
async def list_skills():
    """Return the full skills catalog from the filesystem."""
    skills = _parse_skills_from_disk()

    categories: Dict[str, int] = {}
    for skill in skills:
        cat = skill["category"]
        categories[cat] = categories.get(cat, 0) + 1

    return SkillsResponse(
        skills=skills,
        total=len(skills),
        categories=categories,
        updated_at=datetime.utcnow().isoformat(),
    )
