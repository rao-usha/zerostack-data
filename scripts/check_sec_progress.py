"""Quick progress checker for SEC data pull."""
from pathlib import Path
import json

OUTPUT_DIR = Path("sec_data_output")

def check():
    if not OUTPUT_DIR.exists():
        print("No data yet - pull just starting...")
        return
    
    files = [f for f in OUTPUT_DIR.glob("*.json") if f.name != "summary.json"]
    total = 110
    
    print(f"\n{'='*60}")
    print(f"SEC DATA PULL PROGRESS")
    print(f"{'='*60}")
    print(f"Files downloaded: {len(files)}/{total} ({len(files)/total*100:.1f}%)")
    
    if files:
        total_mb = sum(f.stat().st_size for f in files) / (1024*1024)
        print(f"Total data size: {total_mb:.1f} MB")
        
        print(f"\nRecent files:")
        recent = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]
        for f in recent:
            size_mb = f.stat().st_size / (1024*1024)
            print(f"  • {f.name[:50]:<50} {size_mb:>5.1f} MB")
    
    print(f"{'='*60}\n")
    
    if len(files) >= total:
        print("✓ COMPLETE! All companies downloaded.\n")
    else:
        remaining = total - len(files)
        eta_min = remaining * 0.4
        print(f"⏳ In progress... ~{eta_min:.0f} minutes remaining\n")

if __name__ == "__main__":
    check()

