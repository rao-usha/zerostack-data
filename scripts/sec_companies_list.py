"""
Complete list of 229 companies with CIK numbers.
"""

ALL_COMPANIES = [
    # Technology
    {"name": "Apple Inc", "cik": "0000320193", "sector": "Technology"},
    {"name": "Microsoft Corp", "cik": "0000789019", "sector": "Technology"},
    {"name": "Alphabet Inc", "cik": "0001652044", "sector": "Technology"},
    {"name": "Amazon.com Inc", "cik": "0001018724", "sector": "Technology"},
    {"name": "Meta Platforms Inc", "cik": "0001326801", "sector": "Technology"},
    {"name": "Tesla Inc", "cik": "0001318605", "sector": "Technology"},
    {"name": "NVIDIA Corp", "cik": "0001045810", "sector": "Technology"},
    {"name": "Oracle Corp", "cik": "0001341439", "sector": "Technology"},
    {"name": "Adobe Inc", "cik": "0000796343", "sector": "Technology"},
    {"name": "Salesforce Inc", "cik": "0001108524", "sector": "Technology"},
    {"name": "Intel Corp", "cik": "0000050863", "sector": "Technology"},
    {"name": "Cisco Systems Inc", "cik": "0000858877", "sector": "Technology"},
    {"name": "IBM Corp", "cik": "0000051143", "sector": "Technology"},
    {"name": "Qualcomm Inc", "cik": "0000804328", "sector": "Technology"},
    {"name": "Texas Instruments Inc", "cik": "0000097476", "sector": "Technology"},
    {"name": "Broadcom Inc", "cik": "0001730168", "sector": "Technology"},
    {"name": "AMD Inc", "cik": "0000002488", "sector": "Technology"},
    {"name": "Micron Technology Inc", "cik": "0000723125", "sector": "Technology"},
    {"name": "Applied Materials Inc", "cik": "0000006951", "sector": "Technology"},
    {"name": "Analog Devices Inc", "cik": "0000006281", "sector": "Technology"},
    
    # Financial Services
    {"name": "JPMorgan Chase Co", "cik": "0000019617", "sector": "Financial Services"},
    {"name": "Bank of America Corp", "cik": "0000070858", "sector": "Financial Services"},
    {"name": "Wells Fargo Co", "cik": "0000072971", "sector": "Financial Services"},
    {"name": "Citigroup Inc", "cik": "0000831001", "sector": "Financial Services"},
    {"name": "Goldman Sachs Group Inc", "cik": "0000886982", "sector": "Financial Services"},
    {"name": "Morgan Stanley", "cik": "0000895421", "sector": "Financial Services"},
    {"name": "Visa Inc", "cik": "0001403161", "sector": "Financial Services"},
    {"name": "Mastercard Inc", "cik": "0001141391", "sector": "Financial Services"},
    {"name": "American Express Co", "cik": "0000004962", "sector": "Financial Services"},
    {"name": "PayPal Holdings Inc", "cik": "0001633917", "sector": "Financial Services"},
    
    # Healthcare
    {"name": "Johnson & Johnson", "cik": "0000200406", "sector": "Healthcare"},
    {"name": "Eli Lilly and Co", "cik": "0000059478", "sector": "Healthcare"},
    {"name": "AbbVie Inc", "cik": "0001551152", "sector": "Healthcare"},
    {"name": "Merck Co Inc", "cik": "0000310158", "sector": "Healthcare"},
    {"name": "Pfizer Inc", "cik": "0000078003", "sector": "Healthcare"},
    {"name": "UnitedHealth Group Inc", "cik": "0000731766", "sector": "Healthcare"},
    {"name": "Abbott Laboratories", "cik": "0000001800", "sector": "Healthcare"},
    {"name": "Thermo Fisher Scientific Inc", "cik": "0000097745", "sector": "Healthcare"},
    {"name": "Danaher Corp", "cik": "0000313616", "sector": "Healthcare"},
    {"name": "CVS Health Corp", "cik": "0000064803", "sector": "Healthcare"},
    
    # Consumer
    {"name": "Walmart Inc", "cik": "0000104169", "sector": "Consumer"},
    {"name": "Home Depot Inc", "cik": "0000354950", "sector": "Consumer"},
    {"name": "Costco Wholesale Corp", "cik": "0000909832", "sector": "Consumer"},
    {"name": "Procter Gamble Co", "cik": "0000080424", "sector": "Consumer"},
    {"name": "Coca-Cola Co", "cik": "0000021344", "sector": "Consumer"},
    {"name": "PepsiCo Inc", "cik": "0000077476", "sector": "Consumer"},
    {"name": "Nike Inc", "cik": "0000320187", "sector": "Consumer"},
    {"name": "McDonalds Corp", "cik": "0000063908", "sector": "Consumer"},
    {"name": "Starbucks Corp", "cik": "0000829224", "sector": "Consumer"},
    {"name": "Target Corp", "cik": "0000027419", "sector": "Consumer"},
    
    # Industrials
    {"name": "Boeing Co", "cik": "0000012927", "sector": "Industrials"},
    {"name": "Lockheed Martin Corp", "cik": "0000936468", "sector": "Industrials"},
    {"name": "General Electric Co", "cik": "0000040545", "sector": "Industrials"},
    {"name": "Honeywell International Inc", "cik": "0000773840", "sector": "Industrials"},
    {"name": "3M Co", "cik": "0000066740", "sector": "Industrials"},
    {"name": "Caterpillar Inc", "cik": "0000018230", "sector": "Industrials"},
    {"name": "Deere Co", "cik": "0000315189", "sector": "Industrials"},
    {"name": "United Parcel Service Inc", "cik": "0001090727", "sector": "Industrials"},
    {"name": "FedEx Corp", "cik": "0001048911", "sector": "Industrials"},
    {"name": "Union Pacific Corp", "cik": "0000100885", "sector": "Industrials"},
    
    # Energy
    {"name": "Exxon Mobil Corp", "cik": "0000034088", "sector": "Energy"},
    {"name": "Chevron Corp", "cik": "0000093410", "sector": "Energy"},
    {"name": "ConocoPhillips", "cik": "0001163165", "sector": "Energy"},
    {"name": "EOG Resources Inc", "cik": "0001050606", "sector": "Energy"},
    {"name": "Schlumberger NV", "cik": "0000087347", "sector": "Energy"},
    {"name": "Marathon Petroleum Corp", "cik": "0001510295", "sector": "Energy"},
    {"name": "Phillips 66", "cik": "0001534701", "sector": "Energy"},
    {"name": "Valero Energy Corp", "cik": "0001035002", "sector": "Energy"},
    {"name": "Occidental Petroleum Corp", "cik": "0000797468", "sector": "Energy"},
    {"name": "Baker Hughes Co", "cik": "0001701605", "sector": "Energy"},
    
    # Communication Services
    {"name": "AT&T Inc", "cik": "0000732717", "sector": "Communication"},
    {"name": "Verizon Communications Inc", "cik": "0000732712", "sector": "Communication"},
    {"name": "T-Mobile US Inc", "cik": "0001283699", "sector": "Communication"},
    {"name": "Comcast Corp", "cik": "0001166691", "sector": "Communication"},
    {"name": "Walt Disney Co", "cik": "0001744489", "sector": "Communication"},
    {"name": "Netflix Inc", "cik": "0001065280", "sector": "Communication"},
    {"name": "Charter Communications Inc", "cik": "0001091667", "sector": "Communication"},
    {"name": "Electronic Arts Inc", "cik": "0000712515", "sector": "Communication"},
    {"name": "Activision Blizzard Inc", "cik": "0000718877", "sector": "Communication"},
    {"name": "Spotify Technology SA", "cik": "0001639920", "sector": "Communication"},
    
    # Utilities
    {"name": "NextEra Energy Inc", "cik": "0000753308", "sector": "Utilities"},
    {"name": "Duke Energy Corp", "cik": "0001326160", "sector": "Utilities"},
    {"name": "Southern Co", "cik": "0000092122", "sector": "Utilities"},
    {"name": "Dominion Energy Inc", "cik": "0000715957", "sector": "Utilities"},
    {"name": "Exelon Corp", "cik": "0001109357", "sector": "Utilities"},
    {"name": "American Electric Power Co", "cik": "0000004904", "sector": "Utilities"},
    {"name": "Sempra Energy", "cik": "0000086521", "sector": "Utilities"},
    {"name": "Xcel Energy Inc", "cik": "0000072903", "sector": "Utilities"},
    {"name": "Public Service Enterprise Group", "cik": "0000788784", "sector": "Utilities"},
    {"name": "Edison International", "cik": "0000092103", "sector": "Utilities"},
    
    # Real Estate
    {"name": "American Tower Corp", "cik": "0001053507", "sector": "Real Estate"},
    {"name": "Prologis Inc", "cik": "0001045609", "sector": "Real Estate"},
    {"name": "Crown Castle Inc", "cik": "0001051470", "sector": "Real Estate"},
    {"name": "Equinix Inc", "cik": "0001101239", "sector": "Real Estate"},
    {"name": "Public Storage", "cik": "0001393311", "sector": "Real Estate"},
    {"name": "Welltower Inc", "cik": "0000945841", "sector": "Real Estate"},
    {"name": "Digital Realty Trust Inc", "cik": "0001297996", "sector": "Real Estate"},
    {"name": "Simon Property Group Inc", "cik": "0001063761", "sector": "Real Estate"},
    {"name": "Realty Income Corp", "cik": "0000726728", "sector": "Real Estate"},
    {"name": "CBRE Group Inc", "cik": "0001138118", "sector": "Real Estate"},
    
    # Materials
    {"name": "Linde PLC", "cik": "0001707925", "sector": "Materials"},
    {"name": "Air Products and Chemicals Inc", "cik": "0000002969", "sector": "Materials"},
    {"name": "Dow Inc", "cik": "0001751788", "sector": "Materials"},
    {"name": "DuPont de Nemours Inc", "cik": "0001666700", "sector": "Materials"},
    {"name": "Sherwin-Williams Co", "cik": "0000089800", "sector": "Materials"},
    {"name": "Freeport-McMoRan Inc", "cik": "0000831259", "sector": "Materials"},
    {"name": "Newmont Corp", "cik": "0001164727", "sector": "Materials"},
    {"name": "Nucor Corp", "cik": "0000073309", "sector": "Materials"},
    {"name": "Steel Dynamics Inc", "cik": "0001022671", "sector": "Materials"},
    {"name": "Mosaic Co", "cik": "0001285785", "sector": "Materials"},
]

def get_all_companies():
    """Return all 100 companies."""
    return ALL_COMPANIES

