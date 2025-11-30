"""
Comprehensive Family Office Form ADV Ingestion Script.

This script:
1. Ingests Form ADV data for ALL family offices in the list
2. Tracks which ones are found vs not found
3. Updates EXTERNAL_DATA_SOURCES.md with actual results
4. Generates a detailed report

Usage:
    python ingest_all_family_offices.py
"""
import requests
import time
import json
from typing import List, Dict, Any
from datetime import datetime


BASE_URL = "http://localhost:8001"  # Based on docker-compose output


# Complete list of family offices from user's document
FAMILY_OFFICES = {
    "US": [
        "Soros Fund Management",
        "Cohen Private Ventures",
        "MSD Capital",
        "MSD Partners",
        "Cascade Investment",
        "Walton Family Office",
        "Bezos Expeditions",
        "Emerson Collective",
        "Shad Khan Family Office",
        "Perot Investments",
        "Pritzker Group",
        "Ballmer Group",
        "Ballmer Investments",
        "Arnold Ventures",
        "Hewlett Foundation",
        "Packard Foundation",
        "Raine Group",
    ],
    "Europe": [
        "Cevian Capital",
        "LGT Group",
        "Bertelsmann",
        "Mohn Family Office",
        "JAB Holding Company",
        "Reimann Family",
        "Kyocera Family Office",
        "Agnelli Family",
        "Exor",
        "BMW Quandt Family Office",
        "Quandt Family",
        "Ferrero Family Office",
        "Heineken Family Office",
        "Hermès Family Office",
        "Axile",
    ],
    "Middle East & Asia": [
        "Kingdom Holding",
        "Alwaleed Bin Talal",
        "Olayan Group",
        "Al-Futtaim Family Office",
        "Mitsubishi Kinzoku",
        "Tata Group",
        "Cheng Family Office",
        "New World Development",
        "Chow Tai Fook",
        "Lee Family Office",
        "Samsung Family Office",
        "Kuok Group",
    ],
    "Latin America": [
        "Safra Family Office",
        "Lemann Family",
        "3G Capital",
        "Marinho Family",
        "Santo Domingo Family Office",
        "Paulmann Family",
        "Cencosud Family Office",
        "Luksic Family Office",
    ]
}


def ingest_batch(family_office_names: List[str], batch_name: str) -> Dict[str, Any]:
    """Ingest a batch of family offices."""
    url = f"{BASE_URL}/api/v1/sec/form-adv/ingest/family-offices"
    
    payload = {
        "family_office_names": family_office_names,
        "max_concurrency": 1,
        "max_requests_per_second": 2.0
    }
    
    print(f"\n{'='*80}")
    print(f"BATCH: {batch_name}")
    print(f"{'='*80}")
    print(f"📤 Requesting Form ADV ingestion for {len(family_office_names)} firms...")
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Job created: ID={result['job_id']}, Status={result['status']}")
        
        return result
    except requests.exceptions.RequestException as e:
        print(f"❌ HTTP Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status code: {e.response.status_code}")
            print(f"   Response: {e.response.text}")
        return None


def check_job_status(job_id: int) -> Dict[str, Any]:
    """Check job status."""
    url = f"{BASE_URL}/api/v1/jobs/{job_id}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def wait_for_job(job_id: int, timeout: int = 600, poll_interval: int = 5) -> Dict[str, Any]:
    """Wait for job completion."""
    print(f"⏳ Waiting for job {job_id} to complete...")
    
    start_time = time.time()
    last_status = None
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed > timeout:
            print(f"\n⚠️  Timeout reached ({timeout}s)")
            break
        
        try:
            job_status = check_job_status(job_id)
            status = job_status["status"]
            
            if status != last_status:
                print(f"   [{int(elapsed)}s] Status: {status}")
                last_status = status
            
            if status in ["success", "failed"]:
                print()
                return job_status
            
            time.sleep(poll_interval)
        except Exception as e:
            print(f"   Error checking status: {e}")
            time.sleep(poll_interval)
    
    return check_job_status(job_id)


def query_results() -> List[Dict[str, Any]]:
    """Query all ingested Form ADV data."""
    print("\n" + "="*80)
    print("QUERYING RESULTS FROM DATABASE")
    print("="*80)
    
    # We'll need to query via SQL since there's no query endpoint yet
    # For now, return empty - will be filled by SQL query
    return []


def generate_report(all_results: Dict[str, Dict[str, Any]]) -> str:
    """Generate a comprehensive report."""
    report = []
    report.append("="*80)
    report.append("FAMILY OFFICE FORM ADV INGESTION REPORT")
    report.append(f"Generated: {datetime.now().isoformat()}")
    report.append("="*80)
    report.append("")
    
    total_searched = 0
    total_found = 0
    total_ingested = 0
    
    for region, result in all_results.items():
        if not result:
            continue
            
        report.append(f"\n{region}")
        report.append("-" * len(region))
        
        metadata = result.get("metadata", {})
        searched = metadata.get("searched_offices", 0)
        found = metadata.get("total_matches_found", 0)
        ingested = metadata.get("total_ingested", 0)
        
        total_searched += searched
        total_found += found
        total_ingested += ingested
        
        report.append(f"  Searched: {searched}")
        report.append(f"  Matches Found: {found}")
        report.append(f"  Successfully Ingested: {ingested}")
        
        if metadata.get("errors"):
            report.append(f"  Errors: {len(metadata['errors'])}")
    
    report.append(f"\n{'='*80}")
    report.append("SUMMARY")
    report.append("="*80)
    report.append(f"Total Searched: {total_searched}")
    report.append(f"Total Matches Found: {total_found}")
    report.append(f"Total Ingested: {total_ingested}")
    report.append(f"Success Rate: {(total_ingested/total_searched*100) if total_searched > 0 else 0:.1f}%")
    report.append("")
    report.append("⚠️  Note: Many family offices have registration exemptions")
    report.append("   and will not appear in SEC Form ADV data. This is normal.")
    report.append("")
    
    return "\n".join(report)


def update_external_data_sources(results: Dict[str, Dict[str, Any]]):
    """Update the EXTERNAL_DATA_SOURCES.md file with results."""
    print("\n" + "="*80)
    print("UPDATING EXTERNAL_DATA_SOURCES.md")
    print("="*80)
    
    try:
        with open("docs/EXTERNAL_DATA_SOURCES.md", "r", encoding="utf-8") as f:
            content = f.read()
        
        # Add results summary at the top of the Family Office section
        summary = f"""

### **Ingestion Results Summary**

**Last Run:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

**Results:**
"""
        
        for region, result in results.items():
            if result and result.get("metadata"):
                metadata = result["metadata"]
                summary += f"\n- **{region}:** {metadata.get('total_ingested', 0)} firms ingested out of {metadata.get('searched_offices', 0)} searched"
        
        summary += "\n\n**Next Steps:**\n"
        summary += "1. Query database: `SELECT * FROM sec_form_adv WHERE is_family_office = TRUE`\n"
        summary += "2. Review personnel: `SELECT * FROM sec_form_adv_personnel`\n"
        summary += "3. Export contact list for CRM integration\n"
        
        # Insert after the "### **U.S. Large Family Offices**" line
        insert_marker = "### **U.S. Large Family Offices**"
        if insert_marker in content:
            parts = content.split(insert_marker, 1)
            new_content = parts[0] + insert_marker + summary + "\n" + parts[1]
            
            with open("docs/EXTERNAL_DATA_SOURCES.md", "w", encoding="utf-8") as f:
                f.write(new_content)
            
            print("✅ Updated EXTERNAL_DATA_SOURCES.md")
        else:
            print("⚠️  Could not find insertion point in EXTERNAL_DATA_SOURCES.md")
    
    except Exception as e:
        print(f"❌ Error updating file: {e}")


def main():
    """Main execution flow."""
    print("="*80)
    print("FAMILY OFFICE FORM ADV COMPREHENSIVE INGESTION")
    print("="*80)
    print(f"\nTarget: {sum(len(v) for v in FAMILY_OFFICES.values())} family offices")
    print(f"Regions: {len(FAMILY_OFFICES)}")
    print(f"\n⚠️  IMPORTANT NOTES:")
    print(f"   - Many family offices have SEC registration exemptions")
    print(f"   - Only registered investment advisers will be found")
    print(f"   - This is normal and expected behavior")
    print(f"   - We retrieve BUSINESS contact info only (not personal PII)")
    print(f"\n🕐 Estimated time: 10-20 minutes (rate limited for API safety)")
    print(f"\nPress Ctrl+C to cancel...")
    
    time.sleep(3)
    
    all_results = {}
    
    # Process each region
    for region, offices in FAMILY_OFFICES.items():
        print(f"\n\n{'#'*80}")
        print(f"# REGION: {region}")
        print(f"{'#'*80}")
        
        # Ingest this batch
        result = ingest_batch(offices, region)
        
        if not result:
            print(f"❌ Failed to create job for {region}")
            all_results[region] = None
            continue
        
        job_id = result["job_id"]
        
        # Wait for completion
        final_status = wait_for_job(job_id, timeout=600, poll_interval=10)
        
        # Display results
        print(f"\n{'='*80}")
        print(f"BATCH COMPLETE: {region}")
        print(f"{'='*80}")
        print(f"Status: {final_status.get('status')}")
        
        if final_status.get("metadata"):
            metadata = final_status["metadata"]
            print(f"\nResults:")
            print(f"   Searched: {metadata.get('searched_offices', 'N/A')}")
            print(f"   Found: {metadata.get('total_matches_found', 'N/A')}")
            print(f"   Ingested: {metadata.get('total_ingested', 'N/A')}")
            
            if metadata.get("errors"):
                print(f"\nSample Errors:")
                for error in metadata["errors"][:3]:
                    print(f"   - {error}")
        
        if final_status.get("error_message"):
            print(f"\nError: {final_status['error_message']}")
        
        all_results[region] = final_status
        
        # Brief pause between regions
        if region != list(FAMILY_OFFICES.keys())[-1]:
            print(f"\n⏸️  Pausing 5 seconds before next region...")
            time.sleep(5)
    
    # Generate final report
    print("\n\n" + "="*80)
    print("ALL BATCHES COMPLETE")
    print("="*80)
    
    report = generate_report(all_results)
    print("\n" + report)
    
    # Save report to file
    report_filename = f"form_adv_ingestion_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_filename, "w") as f:
        f.write(report)
    print(f"\n📄 Report saved to: {report_filename}")
    
    # Update EXTERNAL_DATA_SOURCES.md
    update_external_data_sources(all_results)
    
    # Show how to query results
    print("\n" + "="*80)
    print("NEXT STEPS - QUERY YOUR DATA")
    print("="*80)
    print("\n1. Query all ingested firms:")
    print("   docker-compose exec postgres psql -U nexdata -d nexdata -c \\")
    print("     \"SELECT firm_name, business_phone, business_email, business_address_state, assets_under_management FROM sec_form_adv ORDER BY assets_under_management DESC NULLS LAST LIMIT 20;\"")
    
    print("\n2. Query family offices specifically:")
    print("   docker-compose exec postgres psql -U nexdata -d nexdata -c \\")
    print("     \"SELECT firm_name, business_phone, business_email, website FROM sec_form_adv WHERE is_family_office = TRUE;\"")
    
    print("\n3. Query key personnel:")
    print("   docker-compose exec postgres psql -U nexdata -d nexdata -c \\")
    print("     \"SELECT f.firm_name, p.full_name, p.title, p.email, p.phone FROM sec_form_adv f JOIN sec_form_adv_personnel p ON f.crd_number = p.crd_number LIMIT 50;\"")
    
    print("\n4. Export to CSV:")
    print("   docker-compose exec postgres psql -U nexdata -d nexdata -c \\")
    print("     \"\\copy (SELECT * FROM sec_form_adv) TO '/tmp/family_offices.csv' CSV HEADER;\"")
    
    print("\n✅ INGESTION COMPLETE!")
    print("="*80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

