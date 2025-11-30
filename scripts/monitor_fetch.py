"""
Real-time monitoring of SEC data fetch progress.
"""
import json
import time
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = Path("sec_data_output")

def monitor():
    """Monitor fetch progress in real-time."""
    print("\n" + "="*80)
    print("SEC DATA FETCH - LIVE MONITOR")
    print("="*80)
    print("\nPress Ctrl+C to stop monitoring\n")
    
    last_count = 0
    start_time = datetime.now()
    
    try:
        while True:
            if not OUTPUT_DIR.exists():
                print("Waiting for fetch to start...")
                time.sleep(5)
                continue
            
            # Count files
            json_files = [f for f in OUTPUT_DIR.glob("*.json") 
                         if f.name not in ["fetch_summary.json", "final_summary_229.json"]]
            current_count = len(json_files)
            
            # Calculate stats
            elapsed = (datetime.now() - start_time).total_seconds() / 60
            progress_pct = (current_count / 229) * 100
            
            if current_count > last_count:
                new_files = current_count - last_count
                print(f"[{datetime.now().strftime('%H:%M:%S')}] " 
                      f"{current_count}/229 ({progress_pct:.1f}%) "
                      f"[+{new_files} new]")
                last_count = current_count
            
            # Check if complete
            if current_count >= 227:  # Account for a few that might not have data
                print(f"\n{'='*80}")
                print("✓ FETCH APPEARS COMPLETE!")
                print(f"{'='*80}")
                print(f"Total files: {current_count}")
                print(f"Total time: {elapsed:.1f} minutes")
                
                # Calculate total size
                total_size = sum(f.stat().st_size for f in json_files) / (1024 * 1024)
                print(f"Total data: {total_size:.2f} MB")
                print(f"\nCheck final_summary_229.json for details")
                break
            
            time.sleep(10)  # Check every 10 seconds
            
    except KeyboardInterrupt:
        print(f"\n\nMonitoring stopped.")
        print(f"Current progress: {current_count}/229 ({progress_pct:.1f}%)")

if __name__ == "__main__":
    monitor()

