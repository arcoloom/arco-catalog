#!/usr/bin/env python3
"""
Extract and categorize AWS EC2 instance series.
Usage: python instance_series.py [file_path]
"""

import json
import sys
import re
from pathlib import Path
from collections import defaultdict


def extract_series(instance_type):
    """Extract the series from an instance type, e.g. 'c6g.xlarge' -> 'c6g'."""
    # Match the series prefix: letters + digits + optional letter suffix.
    match = re.match(r'^([a-z]+\d+[a-z]*)', instance_type.lower())
    return match.group(1) if match else None


def get_series_category(series):
    """Return the series category, e.g. 'c6g' -> 'c-series'."""
    if not series:
        return "Other"
    # Use the first character as the category prefix.
    first_char = series[0].lower()
    return f"{first_char}-series"


def iter_instance_types(data):
    """Yield instance type names from supported JSON shapes."""
    if isinstance(data, dict):
        yield from data.keys()
        return

    if isinstance(data, list):
        for index, item in enumerate(data):
            if not isinstance(item, dict):
                continue

            instance_type = item.get("instance_type")
            if not instance_type:
                print(
                    f"Warning: record {index + 1} is missing the instance_type field and was skipped",
                    file=sys.stderr,
                )
                continue

            yield instance_type
        return

    print(
        f"Error: unsupported JSON top-level structure: {type(data).__name__}",
        file=sys.stderr,
    )
    sys.exit(1)


def main(file_path=None):
    # Default input path.
    if file_path is None:
        file_path = Path.cwd() / "downloads" / "instances.json"
    else:
        file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"Error: file not found: {file_path}", file=sys.stderr)
        sys.exit(1)
    
    # Load the JSON file.
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: failed to parse JSON: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Extract all distinct series.
    all_series = set()
    for instance_type in iter_instance_types(data):
        series = extract_series(instance_type)
        if series:
            all_series.add(series)
    
    # Group series by category.
    categories = defaultdict(set)
    for series in sorted(all_series):
        category = get_series_category(series)
        categories[category].add(series)
    
    # Print the results.
    print(f"File: {file_path}")
    print(f"Total valid series: {len(all_series)}\n")
    
    for category in sorted(categories.keys()):
        series_list = sorted(categories[category])
        print(f"{category} ({len(series_list)}): {', '.join(series_list)}")


if __name__ == "__main__":
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    main(file_path)
