"""Fetch First Citizens DEF 14A proxy and extract ALL executive officers with GPT."""
import requests
import re
import json
import os

HEADERS = {"User-Agent": "Nexdata admin@nexdata.com", "Accept": "text/html"}

url = "https://www.sec.gov/Archives/edgar/data/798941/000119312525056659/d825240ddef14a.htm"
r = requests.get(url, headers=HEADERS)
text = r.text

# Strip HTML
clean = re.sub(r"<[^>]+>", " ", text)
clean = re.sub(r"&nbsp;", " ", clean)
clean = re.sub(r"&#\d+;", " ", clean)
clean = re.sub(r"\s+", " ", clean).strip()

# The search output showed NEO names at pos 144459:
# "Holding, Jr. Chairman and Chief Executive Officer Craig L. Nix Chief Financial Officer Hope H. Bryant Vice Chairwoman Peter M. Bristow President Lorie..."
# This is the Summary Compensation Table area - grab a big chunk
idx = clean.lower().find("craig l. nix")
chunk1 = clean[max(0,idx-500):idx+8000]

# Also grab director bios section (around pos 22000-50000)
idx2 = clean.lower().find("director nominees")
if idx2 < 0:
    idx2 = clean.lower().find("nominees for election")
chunk2 = clean[max(0,idx2-200):idx2+15000] if idx2 >= 0 else ""

# Also grab "executive officers who are not directors" or similar
idx3 = clean.lower().find("executive officers who are not")
if idx3 < 0:
    idx3 = clean.lower().find("other executive officers")
if idx3 < 0:
    # Try finding after the directors section
    idx3 = clean.lower().find("lorie k. peek")
    if idx3 < 0:
        idx3 = clean.lower().find("donald a. smart")
chunk3 = clean[max(0,idx3-500):idx3+10000] if idx3 >= 0 else ""

combined = f"""=== NEO COMPENSATION SECTION ===
{chunk1}

=== DIRECTOR NOMINEES SECTION ===
{chunk2}

=== OTHER OFFICERS SECTION ===
{chunk3}"""

print(f"Combined text: {len(combined)} chars")

from openai import OpenAI
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{
        "role": "user",
        "content": f"""Extract ALL executive officers and directors from this SEC DEF 14A proxy statement for First Citizens BancShares (a top-20 US bank, ~20,000 employees).

For each person return:
- name: Full legal name
- title: Their exact current title/position
- age: If mentioned
- start_year: Year appointed to current role (look for "since YYYY", "appointed in YYYY", "has served as...since")
- is_director: true/false
- bio: 1-2 sentence summary

Return JSON: {{"officers": [...]}}

IMPORTANT: Include ALL named executive officers (NEOs) from the compensation table AND all director nominees AND any other officers mentioned. I expect 15-25+ people for a bank this size.

{combined[:40000]}"""
    }],
    response_format={"type": "json_object"},
    temperature=0,
)

result = response.choices[0].message.content
parsed = json.loads(result)
officers = parsed.get("officers", [])
print(f"\n=== EXTRACTED {len(officers)} PEOPLE ===\n")
for o in officers:
    director = " [DIRECTOR]" if o.get("is_director") else ""
    age = f", age {o['age']}" if o.get("age") else ""
    since = f", since {o['start_year']}" if o.get("start_year") else ""
    print(f"  {o['name']} — {o['title']}{age}{since}{director}")
