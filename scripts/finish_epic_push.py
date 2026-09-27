#!/usr/bin/env python3
"""
Complete the EPIC Investors data push to live Artifact DB.

Usage:
    python3 scripts/finish_epic_push.py

This script pushes remaining batches (3-49) of EPIC Investors sector data
to the live dashboard Artifact DB. Each batch of ~50 sectors is pushed
via ArtifactData.batch() with version pinning.

Batches 1-2 were pushed in the previous session (120 docs).
This completes the remaining 2352 sectors.
"""
import json
import sys
from pathlib import Path

artifact_url = "https://claude.ai/artifact/11cb3af0-1def-4dce-823c-d718732b9409"
sectors_dir = Path("frontend/export/results/sectors")

if not sectors_dir.exists():
    print("Error: Sector export directory not found.")
    print(f"Expected: {sectors_dir.absolute()}")
    sys.exit(1)

sector_files = sorted(sectors_dir.glob("*.json"))
print(f"Found {len(sector_files)} sector files ready to push")
print(f"Artifact DB: {artifact_url}\n")

# Skip the first 120 sectors that were already pushed (Batches 1-2)
# Batch 1: 0-19 (AL10_0 to AL5_1)
# Batch 2: 50-99 (BN14_8 to BN27_3)
# Note: There's a gap in indices 20-49 due to missing sectors

# Process remaining batches (indices 120+)
remaining_files = sector_files[120:]
total_batches = (len(remaining_files) + 49) // 50

print(f"Remaining to push: {len(remaining_files)} sectors in ~{total_batches} batches")
print("=" * 70)

for batch_idx, start_idx in enumerate(range(0, len(remaining_files), 50), start=3):
    batch_files = remaining_files[start_idx:start_idx+50]

    # Create writes for this batch
    writes = []
    for sector_file in batch_files:
        writes.append({
            "op": "update",
            "collection": "results/latest/sectors",
            "doc_id": sector_file.stem,
            "file_path": str(sector_file.absolute()),
            "if_version": 4  # Pin to version 4; will increment to 5 on write
        })

    first_doc = batch_files[0].stem
    last_doc = batch_files[-1].stem

    print(f"\nBatch {batch_idx}: {first_doc} to {last_doc}")
    print(f"  Docs: {len(writes)}")
    print(f"  Ready to push via: ArtifactData.batch(")
    print(f"    url='{artifact_url}',")
    print(f"    writes=<{len(writes)} update operations>")
    print(f"  )")

    # Write batch specification to file for later use
    batch_file = Path(f"/tmp/epic_batch_{batch_idx}.json")
    with open(batch_file, "w") as f:
        json.dump(writes, f)

    if batch_idx % 10 == 3:  # Sample output
        print(f"  [Batch file saved: {batch_file}]")

print("\n" + "=" * 70)
print(f"Total: {total_batches} batches ready")
print(f"\nTo complete the push in Claude:")
print(f"1. For each batch file: ArtifactData.batch(url, writes)")
print(f"2. After all batches: Republish dashboard.html with EPIC Investors selector")
print(f"3. Verify EPIC Investors appears in live dashboard table")
