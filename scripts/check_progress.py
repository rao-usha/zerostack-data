"""
Check progress of SEC data fetch.
"""
import json
from pathlib import Path

OUTPUT_DIR = Path("sec_data_output")

def check_progress():
    """Check how many companies have been fetched."""
    if not OUTPUT_DIR.exists():
        print("Output directory doesn't exist yet. Fetch may not have started.")
        return
    
    # Count JSON files (excluding summary)
    json_files = [f for f in OUTPUT_DIR.glob("*.json") if f.name != "fetch_summary.json"]
    
    print("\n" + "="*80)
    print("SEC DATA FETCH - PROGRESS CHECK")
    print("="*80)
    print(f"\nCompanies fetched: {len(json_files)}/100")
    print(f"Progress: {len(json_files)/100*100:.1f}%")
    
    if len(json_files) > 0:
        print(f"\nOutput directory: {OUTPUT_DIR.absolute()}")
        print(f"\nRecent files:")
        recent_files = sorted(json_files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]
        for f in recent_files:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  • {f.name} ({size_mb:.2f} MB)")
        
        # Calculate total size
        total_size = sum(f.stat().st_size for f in json_files) / (1024 * 1024)
        print(f"\nTotal data downloaded: {total_size:.2f} MB")
    
    print("="*80 + "\n")
    
    # Check if summary exists (means fetch is complete)
    summary_file = OUTPUT_DIR / "fetch_summary.json"
    if summary_file.exists():
        print("✓ Fetch is COMPLETE! Summary available.")
        with open(summary_file) as f:
            summary = json.load(f)
        print(f"  Successful: {summary['successful']}")
        print(f"  Failed: {summary['failed']}")
        print(f"\nTo view full details: {summary_file}")
    else:
        print("⏳ Fetch is still in progress...")
        estimated_remaining = (100 - len(json_files)) * 0.25  # ~15 seconds per company
        print(f"   Estimated time remaining: {estimated_remaining:.0f} minutes")
    
    print()

if __name__ == "__main__":
    check_progress()

