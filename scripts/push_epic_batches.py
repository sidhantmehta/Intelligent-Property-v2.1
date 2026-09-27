#!/usr/bin/env python3
"""
Push EPIC Investors sector batches to live Artifact DB.

This script is designed to be triggered by a Claude Routine to push
all remaining sector batches (gap + 3-49) to the dashboard.

The batches have been pre-generated with proper file references.
Each batch file contains up to 50 update operations with file_path
references to the actual JSON files.

Usage:
    python3 scripts/push_epic_batches.py [START_BATCH] [END_BATCH]

    Examples:
    - Push all: python3 scripts/push_epic_batches.py
    - Push gap + 3-10: python3 scripts/push_epic_batches.py gap 10
    - Push 3-15: python3 scripts/push_epic_batches.py 3 15
"""
import json
import sys
from pathlib import Path

artifact_url = "https://claude.ai/artifact/11cb3af0-1def-4dce-823c-d718732b9409"

# Determine which batches to push
if len(sys.argv) > 2:
    start_batch = sys.argv[1]
    end_batch = int(sys.argv[2])

    if start_batch == "gap":
        batches_to_push = ["gap"] + list(range(3, end_batch + 1))
    else:
        batches_to_push = list(range(int(start_batch), end_batch + 1))
else:
    # Default: push all
    batches_to_push = ["gap"] + list(range(3, 50))

print(f"EPIC Investors Batch Push")
print(f"Artifact URL: {artifact_url}")
print(f"Batches to push: {batches_to_push}")
print()

# Verify all batch files exist
batch_dir = Path("/tmp")
missing_batches = []
batch_data = {}

for batch_label in batches_to_push:
    if batch_label == "gap":
        batch_file = batch_dir / "batch_gap_writes.json"
    else:
        batch_file = batch_dir / f"batch{batch_label}_writes.json"

    if batch_file.exists():
        with open(batch_file) as f:
            writes = json.load(f)
        batch_data[batch_label] = writes
        first_doc = writes[0]['doc_id']
        last_doc = writes[-1]['doc_id']
        print(f"✓ Batch {batch_label}: {len(writes):2} docs ({first_doc:15} to {last_doc:15})")
    else:
        missing_batches.append(batch_label)
        print(f"✗ Batch {batch_label}: MISSING")

print()

if missing_batches:
    print(f"ERROR: {len(missing_batches)} batch files missing:")
    for batch in missing_batches:
        print(f"  - {batch}")
    sys.exit(1)

# Summary
total_docs = sum(len(writes) for writes in batch_data.values())
print(f"Ready to push: {len(batch_data)} batches, {total_docs} total documents")
print()

# Output push instructions
print("TO PUSH THESE BATCHES, Claude needs to execute:")
print("  ArtifactData.batch(url, writes) for each batch sequentially")
print()
print("This can be done via:")
print("  1. Manual calls from Claude session")
print("  2. Automated via a Routine with this script")
print("  3. JavaScript code in Artifact console")
print()

# For reference, output the first batch's structure
if batches_to_push:
    first_batch = batches_to_push[0]
    first_writes = batch_data[first_batch]
    print(f"Example write operation from batch {first_batch}:")
    print(json.dumps(first_writes[0], indent=2))
