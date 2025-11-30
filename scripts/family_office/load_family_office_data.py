"""
Load family office data from JSON file into database.

Usage:
    python load_family_office_data.py [json_file]
    
Default: data/family_offices_data.json
"""
import sys
import json
import requests
from pathlib import Path


BASE_URL = "http://localhost:8001"


def load_family_offices(json_file):
    """Load family offices from JSON file via API."""
    print("="*80)
    print("FAMILY OFFICE DATA LOADER")
    print("="*80)
    print(f"\n📂 Reading: {json_file}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        offices = json.load(f)
    
    print(f"📊 Found {len(offices)} family offices to load\n")
    
    loaded = 0
    errors = []
    
    for i, office in enumerate(offices, 1):
        name = office.get('name', 'Unknown')
        
        try:
            response = requests.post(
                f"{BASE_URL}/api/v1/family-offices/",
                json=office,
                timeout=10
            )
            response.raise_for_status()
            
            print(f"  ✅ {i}. {name}")
            loaded += 1
            
        except requests.exceptions.RequestException as e:
            error_msg = f"{name}: {str(e)}"
            print(f"  ❌ {i}. {error_msg}")
            errors.append(error_msg)
            
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    print(f"     Detail: {error_detail.get('detail', 'Unknown error')}")
                except:
                    pass
    
    print(f"\n{'='*80}")
    print("LOAD COMPLETE")
    print(f"{'='*80}")
    print(f"✅ Successfully loaded: {loaded}/{len(offices)}")
    
    if errors:
        print(f"❌ Errors: {len(errors)}")
        for error in errors:
            print(f"   - {error}")
    
    if loaded > 0:
        print(f"\n🎉 Family offices loaded successfully!")
        print("\nQuery your data:")
        print(f"   curl {BASE_URL}/api/v1/family-offices")
        print(f"   curl {BASE_URL}/api/v1/family-offices/stats/overview")
        print("\nOr open Swagger UI:")
        print(f"   {BASE_URL}/docs")
    
    return loaded, errors


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    else:
        json_file = "data/family_offices_data.json"
    
    json_path = Path(json_file)
    if not json_path.exists():
        print(f"❌ JSON file not found: {json_file}")
        print("\nUsage: python load_family_office_data.py [json_file]")
        print("Default: data/family_offices_data.json")
        sys.exit(1)
    
    try:
        loaded, errors = load_family_offices(json_file)
        
        if loaded == 0:
            print("\n⚠️  No records loaded. Check errors above.")
            sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

