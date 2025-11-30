"""
Top 200+ U.S. Public Companies for SEC Data Ingestion

Organized by sector with CIK numbers.
"""

COMPANIES_200 = {
    "Technology": {
        # Mega Cap Tech
        "Apple Inc.": "0000320193",
        "Microsoft Corp": "0000789019",
        "Alphabet Inc (Google)": "0001652044",
        "Amazon.com Inc": "0001018724",
        "Meta Platforms Inc (Facebook)": "0001326801",
        "Tesla Inc": "0001318605",
        "NVIDIA Corp": "0001045810",
        
        # Large Cap Tech
        "Oracle Corp": "0001341439",
        "Adobe Inc": "0000796343",
        "Salesforce Inc": "0001108524",
        "Intel Corp": "0000050863",
        "Cisco Systems Inc": "0000858877",
        "IBM Corp": "0000051143",
        "Qualcomm Inc": "0000804328",
        "Texas Instruments Inc": "0000097476",
        "Broadcom Inc": "0001730168",
        "Advanced Micro Devices Inc": "0000002488",
        "Micron Technology Inc": "0000723125",
        "Applied Materials Inc": "0000006951",
        "Analog Devices Inc": "0000006281",
        
        # Software & Services
        "ServiceNow Inc": "0001373715",
        "Intuit Inc": "0000896878",
        "Autodesk Inc": "0000769397",
        "Workday Inc": "0001327811",
        "Snowflake Inc": "0001640147",
        "Palantir Technologies Inc": "0001321655",
        "CrowdStrike Holdings Inc": "0001535527",
        "Datadog Inc": "0001561550",
        "MongoDB Inc": "0001441110",
        "Atlassian Corp": "0001650372",
        
        # Semiconductors
        "Lam Research Corp": "0000707549",
        "KLA Corp": "0000319201",
        "Marvell Technology Inc": "0001058057",
        "Microchip Technology Inc": "0000827054",
        "NXP Semiconductors NV": "0001413447",
        "ON Semiconductor Corp": "0001097864",
    },
    
    "Financial Services": {
        # Banks
        "JPMorgan Chase & Co": "0000019617",
        "Bank of America Corp": "0000070858",
        "Wells Fargo & Co": "0000072971",
        "Citigroup Inc": "0000831001",
        "Goldman Sachs Group Inc": "0000886982",
        "Morgan Stanley": "0000895421",
        "U.S. Bancorp": "0000036104",
        "PNC Financial Services Group": "0000713676",
        "Truist Financial Corp": "0000092230",
        "Capital One Financial Corp": "0000927628",
        "Bank of New York Mellon Corp": "0001126328",
        "State Street Corp": "0000093751",
        "Charles Schwab Corp": "0000316709",
        
        # Investment Management
        "BlackRock Inc": "0001364742",
        "S&P Global Inc": "0000064040",
        "Moody's Corp": "0001059556",
        "CME Group Inc": "0001156375",
        "Intercontinental Exchange Inc": "0001174746",
        "MarketAxess Holdings Inc": "0001278021",
        
        # Insurance
        "Berkshire Hathaway Inc": "0001067983",
        "UnitedHealth Group Inc": "0000731766",
        "Progressive Corp": "0000080661",
        "Chubb Ltd": "0000896159",
        "Travelers Companies Inc": "0000086312",
        "Allstate Corp": "0000899051",
        "MetLife Inc": "0001099219",
        "Prudential Financial Inc": "0001137774",
        "Marsh & McLennan Companies": "0000062709",
        "Aon PLC": "0001404932",
        
        # Payment Processors
        "Visa Inc": "0001403161",
        "Mastercard Inc": "0001141391",
        "American Express Co": "0000004962",
        "PayPal Holdings Inc": "0001633917",
        "Block Inc (Square)": "0001512673",
    },
    
    "Healthcare": {
        # Pharmaceuticals
        "Johnson & Johnson": "0000200406",
        "Eli Lilly and Co": "0000059478",
        "AbbVie Inc": "0001551152",
        "Merck & Co Inc": "0000310158",
        "Pfizer Inc": "0000078003",
        "Bristol-Myers Squibb Co": "0000014272",
        "Amgen Inc": "0000318154",
        "Gilead Sciences Inc": "0000882095",
        "Regeneron Pharmaceuticals Inc": "0000872589",
        "Vertex Pharmaceuticals Inc": "0000875320",
        "Moderna Inc": "0001682852",
        "Biogen Inc": "0000875045",
        
        # Medical Devices
        "Medtronic PLC": "0001613103",
        "Abbott Laboratories": "0000001800",
        "Thermo Fisher Scientific Inc": "0000097745",
        "Danaher Corp": "0000313616",
        "Intuitive Surgical Inc": "0001035267",
        "Stryker Corp": "0000310764",
        "Boston Scientific Corp": "0000885725",
        "Becton Dickinson and Co": "0000010795",
        
        # Health Insurance & Services
        "CVS Health Corp": "0000064803",
        "Cigna Group": "0001739940",
        "Humana Inc": "0000049071",
        "Elevance Health Inc (Anthem)": "0001156039",
        "Centene Corp": "0001071739",
        "HCA Healthcare Inc": "0001057352",
        "Quest Diagnostics Inc": "0001022079",
        "Laboratory Corp of America": "0000920148",
        
        # Biotech
        "Illumina Inc": "0001110803",
        "BioNTech SE": "0001776985",
        "Seagen Inc": "0001095725",
    },
    
    "Consumer Discretionary": {
        # Retail
        "Walmart Inc": "0000104169",
        "Home Depot Inc": "0000354950",
        "Costco Wholesale Corp": "0000909832",
        "Target Corp": "0000027419",
        "Lowe's Companies Inc": "0000060667",
        "TJX Companies Inc": "0000109198",
        "Dollar General Corp": "0000029534",
        "Dollar Tree Inc": "0000935703",
        "Best Buy Co Inc": "0000764478",
        
        # Automotive
        "General Motors Co": "0001467858",
        "Ford Motor Co": "0000037996",
        "Rivian Automotive Inc": "0001874178",
        "Lucid Group Inc": "0001811210",
        
        # Restaurants
        "McDonald's Corp": "0000063908",
        "Starbucks Corp": "0000829224",
        "Chipotle Mexican Grill Inc": "0001058090",
        "Yum! Brands Inc": "0001041061",
        "Restaurant Brands International": "0001618756",
        
        # E-commerce & Online
        "eBay Inc": "0001065088",
        "Booking Holdings Inc": "0001075531",
        "Airbnb Inc": "0001559720",
        "Etsy Inc": "0001370637",
        
        # Apparel & Luxury
        "Nike Inc": "0000320187",
        "Lululemon Athletica Inc": "0001397187",
        "Ralph Lauren Corp": "0001037038",
        "Tapestry Inc (Coach)": "0001116132",
        "VF Corp": "0000103379",
    },
    
    "Consumer Staples": {
        "Procter & Gamble Co": "0000080424",
        "Coca-Cola Co": "0000021344",
        "PepsiCo Inc": "0000077476",
        "Costco Wholesale Corp": "0000909832",
        "Walmart Inc": "0000104169",
        "Mondelez International Inc": "0001103982",
        "Colgate-Palmolive Co": "0000021665",
        "Kimberly-Clark Corp": "0000055785",
        "General Mills Inc": "0000040704",
        "Kraft Heinz Co": "0001637459",
        "Conagra Brands Inc": "0000023217",
        "Hershey Co": "0000047111",
        "Kellogg Co": "0000055067",
        "Campbell Soup Co": "0000016732",
        "Hormel Foods Corp": "0000048465",
    },
    
    "Energy": {
        "Exxon Mobil Corp": "0000034088",
        "Chevron Corp": "0000093410",
        "ConocoPhillips": "0001163165",
        "EOG Resources Inc": "0001050606",
        "Schlumberger NV": "0000087347",
        "Marathon Petroleum Corp": "0001510295",
        "Phillips 66": "0001534701",
        "Valero Energy Corp": "0001035002",
        "Occidental Petroleum Corp": "0000797468",
        "Baker Hughes Co": "0001701605",
        "Halliburton Co": "0000045012",
        "Pioneer Natural Resources Co": "0001038357",
        "Devon Energy Corp": "0001090012",
        "Hess Corp": "0000004447",
        "Kinder Morgan Inc": "0001506307",
    },
    
    "Industrials": {
        # Aerospace & Defense
        "Boeing Co": "0000012927",
        "Lockheed Martin Corp": "0000936468",
        "Raytheon Technologies Corp": "0000101829",
        "Northrop Grumman Corp": "0001133421",
        "General Dynamics Corp": "0000040533",
        "L3Harris Technologies Inc": "0001039101",
        
        # Industrial Conglomerates
        "General Electric Co": "0000040545",
        "Honeywell International Inc": "0000773840",
        "3M Co": "0000066740",
        "Caterpillar Inc": "0000018230",
        "Deere & Co": "0000315189",
        
        # Transportation & Logistics
        "United Parcel Service Inc": "0001090727",
        "FedEx Corp": "0001048911",
        "Union Pacific Corp": "0000100885",
        "CSX Corp": "0000277948",
        "Norfolk Southern Corp": "0000702165",
        "Delta Air Lines Inc": "0000027904",
        "United Airlines Holdings Inc": "0000100517",
        "American Airlines Group Inc": "0000006201",
        "Southwest Airlines Co": "0000092380",
        
        # Machinery & Equipment
        "Emerson Electric Co": "0000032604",
        "Illinois Tool Works Inc": "0000049826",
        "Parker-Hannifin Corp": "0000076334",
        "Rockwell Automation Inc": "0001024478",
        "Eaton Corp PLC": "0001551182",
    },
    
    "Communication Services": {
        # Telecom
        "AT&T Inc": "0000732717",
        "Verizon Communications Inc": "0000732712",
        "T-Mobile US Inc": "0001283699",
        "Comcast Corp": "0001166691",
        "Charter Communications Inc": "0001091667",
        
        # Media & Entertainment
        "Walt Disney Co": "0001744489",
        "Netflix Inc": "0001065280",
        "Warner Bros Discovery Inc": "0001737996",
        "Paramount Global": "0000813828",
        "Fox Corp": "0001754301",
        "Spotify Technology SA": "0001639920",
        "Match Group Inc": "0001575189",
        
        # Gaming
        "Electronic Arts Inc": "0000712515",
        "Activision Blizzard Inc": "0000718877",
        "Take-Two Interactive Software": "0000946581",
        "Roblox Corp": "0001315098",
    },
    
    "Utilities": {
        "NextEra Energy Inc": "0000753308",
        "Duke Energy Corp": "0001326160",
        "Southern Co": "0000092122",
        "Dominion Energy Inc": "0000715957",
        "Exelon Corp": "0001109357",
        "American Electric Power Co": "0000004904",
        "Sempra Energy": "0000086521",
        "Xcel Energy Inc": "0000072903",
        "Public Service Enterprise Group": "0000788784",
        "Edison International": "0000092103",
    },
    
    "Real Estate": {
        "American Tower Corp": "0001053507",
        "Prologis Inc": "0001045609",
        "Crown Castle Inc": "0001051470",
        "Equinix Inc": "0001101239",
        "Public Storage": "0001393311",
        "Welltower Inc": "0000945841",
        "Digital Realty Trust Inc": "0001297996",
        "Simon Property Group Inc": "0001063761",
        "Realty Income Corp": "0000726728",
        "CBRE Group Inc": "0001138118",
    },
    
    "Materials": {
        "Linde PLC": "0001707925",
        "Air Products and Chemicals Inc": "0000002969",
        "Dow Inc": "0001751788",
        "DuPont de Nemours Inc": "0001666700",
        "Sherwin-Williams Co": "0000089800",
        "Freeport-McMoRan Inc": "0000831259",
        "Newmont Corp": "0001164727",
        "Nucor Corp": "0000073309",
        "Steel Dynamics Inc": "0001022671",
        "Mosaic Co": "0001285785",
    },
}


def get_all_companies():
    """Get all companies as a flat list with CIK numbers."""
    companies = []
    for sector, sector_companies in COMPANIES_200.items():
        for name, cik in sector_companies.items():
            companies.append({
                "name": name,
                "cik": cik,
                "sector": sector
            })
    return companies


def print_summary():
    """Print summary of companies."""
    total = 0
    print("\n" + "="*80)
    print("TOP 200+ U.S. PUBLIC COMPANIES - SEC DATA INGESTION")
    print("="*80 + "\n")
    
    for sector, companies in COMPANIES_200.items():
        count = len(companies)
        total += count
        print(f"{sector:30s}: {count:3d} companies")
    
    print(f"\n{'TOTAL':30s}: {total:3d} companies\n")
    print("="*80 + "\n")


if __name__ == "__main__":
    print_summary()
    
    companies = get_all_companies()
    print(f"Full list contains {len(companies)} companies across {len(COMPANIES_200)} sectors.\n")
    
    # Print first 10 as sample
    print("Sample (first 10 companies):")
    for i, company in enumerate(companies[:10], 1):
        print(f"  {i:2d}. {company['name']:40s} CIK: {company['cik']}  ({company['sector']})")

