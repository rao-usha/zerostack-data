"""
SEC 13F Strategy - Extract public equity holdings from SEC 13F filings.

Coverage: 40-60 large investors with >$100M equity
Confidence: HIGH (regulatory filing)

Implementation:
- Search SEC EDGAR for 13F filers by investor name
- Download most recent 13F-HR XML filing
- Parse holdings table (ticker, shares, value, date)
- Resolve tickers to company names
- Store with source_type='sec_13f', confidence_level='high'
"""
import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import httpx

from app.agentic.strategies.base import BaseStrategy, InvestorContext, StrategyResult

logger = logging.getLogger(__name__)


class SEC13FStrategy(BaseStrategy):
    """
    Strategy for extracting portfolio holdings from SEC 13F filings.
    
    13F filings are required quarterly from institutional investment managers
    with over $100M in AUM. They disclose public equity holdings.
    
    Confidence: HIGH - This is regulatory data
    """
    
    name = "sec_13f"
    display_name = "SEC 13F Filings"
    source_type = "sec_13f"
    default_confidence = "high"
    
    # SEC EDGAR rate limits are 10 requests/second
    # We use 2/second to be conservative
    max_requests_per_second = 2.0
    max_concurrent_requests = 1
    timeout_seconds = 300
    
    # SEC EDGAR URLs
    EDGAR_BASE = "https://www.sec.gov"
    EDGAR_FULL_TEXT_SEARCH = "https://efts.sec.gov/LATEST/search-index"
    EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions"
    EDGAR_FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
    
    # User-Agent is REQUIRED by SEC
    USER_AGENT = "Nexdata Research nexdata@example.com"
    
    def __init__(
        self,
        max_requests_per_second: Optional[float] = None,
        max_concurrent_requests: Optional[int] = None,
        timeout_seconds: Optional[int] = None
    ):
        super().__init__(max_requests_per_second, max_concurrent_requests, timeout_seconds)
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = asyncio.Semaphore(1)
        self._last_request_time = 0.0
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={
                    "User-Agent": self.USER_AGENT,
                    "Accept": "application/json, text/html, application/xml"
                },
                follow_redirects=True
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limited_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make a rate-limited request to SEC EDGAR."""
        import time
        
        async with self._rate_limiter:
            # Enforce rate limit
            now = time.time()
            elapsed = now - self._last_request_time
            wait_time = (1.0 / self.max_requests_per_second) - elapsed
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            client = await self._get_client()
            response = await client.request(method, url, **kwargs)
            self._last_request_time = time.time()
            
            return response
    
    def is_applicable(self, context: InvestorContext) -> Tuple[bool, str]:
        """
        Check if SEC 13F strategy is applicable.
        
        13F is required for:
        - Institutional investment managers with >$100M AUM
        - Public pensions, endowments, large family offices
        
        NOT applicable for:
        - Small investors
        - Non-US investors (usually)
        - Investors without significant public equity
        """
        # Large AUM is a strong indicator
        if context.aum_usd and context.aum_usd >= 100_000_000:
            return True, f"AUM ${context.aum_usd/1e9:.1f}B >= $100M threshold for 13F filing"
        
        # Public pensions almost always file 13F
        if context.lp_type == "public_pension":
            return True, "Public pensions typically file 13F for public equity holdings"
        
        # Sovereign wealth funds often file
        if context.lp_type == "sovereign_wealth":
            return True, "Sovereign wealth funds often file 13F for US equity holdings"
        
        # Large endowments
        if context.lp_type == "endowment":
            return True, "Major endowments typically file 13F"
        
        # Family offices - check for SEC registration
        if context.investor_type == "family_office" and context.sec_crd_number:
            return True, "SEC-registered family office likely files 13F"
        
        # Check if investor name suggests large institution
        large_keywords = ["pension", "retirement", "endowment", "foundation", "trust"]
        if any(kw in context.investor_name.lower() for kw in large_keywords):
            return True, "Name suggests institutional investor that may file 13F"
        
        return False, "No indicators that investor files 13F"
    
    def calculate_priority(self, context: InvestorContext) -> int:
        """
        Calculate priority for 13F strategy.
        
        Higher priority for:
        - Large AUM
        - Public pensions
        - Known 13F filers
        """
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0
        
        # Base priority
        priority = 7
        
        # Boost for large AUM
        if context.aum_usd:
            if context.aum_usd >= 1_000_000_000_000:  # $1T+
                priority = 10
            elif context.aum_usd >= 100_000_000_000:  # $100B+
                priority = 10
            elif context.aum_usd >= 10_000_000_000:  # $10B+
                priority = 9
            elif context.aum_usd >= 1_000_000_000:  # $1B+
                priority = 8
        
        # Boost for public pensions (always file)
        if context.lp_type == "public_pension":
            priority = max(priority, 10)
        
        return priority
    
    async def execute(self, context: InvestorContext) -> StrategyResult:
        """
        Execute SEC 13F collection for the investor.
        
        Steps:
        1. Search SEC EDGAR for the investor's CIK
        2. Find most recent 13F-HR filing
        3. Download and parse the information table XML
        4. Extract holdings (company name, shares, value)
        """
        started_at = datetime.utcnow()
        requests_made = 0
        companies = []
        reasoning_parts = []
        
        try:
            logger.info(f"Executing SEC 13F strategy for {context.investor_name}")
            reasoning_parts.append(f"Starting 13F search for '{context.investor_name}'")
            
            # Step 1: Find the CIK for this investor
            cik, cik_reasoning = await self._find_cik(context.investor_name)
            requests_made += 1
            reasoning_parts.append(cik_reasoning)
            
            if not cik:
                return self._create_result(
                    success=False,
                    error_message="Could not find CIK for investor in SEC EDGAR",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            # Step 2: Get recent 13F filings
            filings, filings_reasoning = await self._get_13f_filings(cik)
            requests_made += 1
            reasoning_parts.append(filings_reasoning)
            
            if not filings:
                return self._create_result(
                    success=False,
                    error_message="No 13F filings found for this investor",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            # Step 3: Download and parse the most recent 13F
            most_recent = filings[0]
            holdings, parse_reasoning = await self._parse_13f_filing(cik, most_recent)
            requests_made += 2  # Primary doc + info table
            reasoning_parts.append(parse_reasoning)
            
            if not holdings:
                return self._create_result(
                    success=False,
                    error_message="Failed to parse 13F holdings",
                    reasoning="\n".join(reasoning_parts),
                    requests_made=requests_made
                )
            
            # Step 4: Convert holdings to standard format
            for holding in holdings:
                company = {
                    "company_name": holding.get("nameOfIssuer", "Unknown"),
                    "company_ticker": holding.get("cusip"),  # CUSIP, not ticker
                    "company_cusip": holding.get("cusip"),
                    "investment_type": "public_equity",
                    "shares_held": str(holding.get("shrsOrPrnAmt", {}).get("sshPrnamt", 0)),
                    "market_value_usd": str(holding.get("value", 0) * 1000),  # 13F reports in thousands
                    "investment_date": most_recent.get("filingDate"),
                    "source_type": self.source_type,
                    "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F",
                    "confidence_level": self.default_confidence,
                    "current_holding": 1,
                }
                companies.append(company)
            
            reasoning_parts.append(f"Found {len(companies)} holdings from 13F filing dated {most_recent.get('filingDate')}")
            
            result = self._create_result(
                success=True,
                companies=companies,
                reasoning="\n".join(reasoning_parts),
                requests_made=requests_made
            )
            result.started_at = started_at
            return result
        
        except Exception as e:
            logger.error(f"Error executing SEC 13F strategy: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning="\n".join(reasoning_parts) + f"\nError: {str(e)}",
                requests_made=requests_made
            )
        
        finally:
            await self.close()
    
    async def _find_cik(self, investor_name: str) -> Tuple[Optional[str], str]:
        """
        Find the CIK (Central Index Key) for an investor.
        
        Uses SEC EDGAR company search.
        """
        try:
            # Try the full-text search first
            # Format: GET https://www.sec.gov/cgi-bin/browse-edgar?company=name&type=13F&action=getcompany
            
            # Clean up the name for search
            search_name = investor_name.replace("'", "").replace("&", "and")
            
            url = f"{self.EDGAR_FILINGS_URL}?company={search_name}&type=13F-HR&action=getcompany&output=atom"
            
            response = await self._rate_limited_request("GET", url)
            
            if response.status_code != 200:
                return None, f"EDGAR search returned status {response.status_code}"
            
            # Parse the Atom feed to find CIK
            content = response.text
            
            # Extract CIK from response
            # The feed contains entries with CIK in the id field
            cik_match = re.search(r'CIK=(\d{10})', content)
            if cik_match:
                cik = cik_match.group(1).lstrip('0')  # Remove leading zeros
                return cik, f"Found CIK {cik} for '{investor_name}'"
            
            # Try alternative: search in company search
            url2 = f"https://www.sec.gov/cgi-bin/browse-edgar?company={search_name}&CIK=&type=13F&owner=include&count=10&action=getcompany"
            response2 = await self._rate_limited_request("GET", url2)
            
            if response2.status_code == 200:
                cik_match2 = re.search(r'CIK=(\d{10})', response2.text)
                if cik_match2:
                    cik = cik_match2.group(1).lstrip('0')
                    return cik, f"Found CIK {cik} via company search for '{investor_name}'"
            
            return None, f"No CIK found for '{investor_name}' in SEC EDGAR"
        
        except Exception as e:
            logger.error(f"Error finding CIK: {e}")
            return None, f"Error searching for CIK: {str(e)}"
    
    async def _get_13f_filings(self, cik: str) -> Tuple[List[Dict[str, Any]], str]:
        """
        Get list of 13F filings for a CIK.
        """
        try:
            # Pad CIK to 10 digits
            cik_padded = cik.zfill(10)
            
            # Use the submissions API
            url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
            
            response = await self._rate_limited_request("GET", url)
            
            if response.status_code != 200:
                return [], f"Failed to get filings: status {response.status_code}"
            
            data = response.json()
            
            # Extract recent 13F filings
            filings = []
            recent_filings = data.get("filings", {}).get("recent", {})
            
            if not recent_filings:
                return [], "No recent filings found in submissions data"
            
            forms = recent_filings.get("form", [])
            dates = recent_filings.get("filingDate", [])
            accession_numbers = recent_filings.get("accessionNumber", [])
            primary_docs = recent_filings.get("primaryDocument", [])
            
            for i, form in enumerate(forms):
                if "13F" in form.upper():
                    filings.append({
                        "form": form,
                        "filingDate": dates[i] if i < len(dates) else None,
                        "accessionNumber": accession_numbers[i] if i < len(accession_numbers) else None,
                        "primaryDocument": primary_docs[i] if i < len(primary_docs) else None,
                    })
            
            # Sort by date descending
            filings.sort(key=lambda x: x.get("filingDate", ""), reverse=True)
            
            if filings:
                return filings[:5], f"Found {len(filings)} 13F filings, most recent: {filings[0].get('filingDate')}"
            else:
                return [], "No 13F-HR filings found for this CIK"
        
        except Exception as e:
            logger.error(f"Error getting 13F filings: {e}")
            return [], f"Error retrieving filings: {str(e)}"
    
    async def _parse_13f_filing(
        self, 
        cik: str, 
        filing: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Parse a 13F filing to extract holdings.
        """
        try:
            cik_padded = cik.zfill(10)
            accession = filing.get("accessionNumber", "").replace("-", "")
            
            if not accession:
                return [], "No accession number for filing"
            
            # The information table is usually in a file like infotable.xml or primary_doc.xml
            # First try to get the filing index
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession}/index.json"
            
            response = await self._rate_limited_request("GET", index_url)
            
            if response.status_code != 200:
                return [], f"Failed to get filing index: status {response.status_code}"
            
            index_data = response.json()
            
            # Find the information table file
            info_table_file = None
            directory = index_data.get("directory", {})
            items = directory.get("item", [])
            
            for item in items:
                name = item.get("name", "").lower()
                if "infotable" in name or ("13f" in name and ".xml" in name):
                    info_table_file = item.get("name")
                    break
            
            if not info_table_file:
                # Try to find any XML file
                for item in items:
                    name = item.get("name", "").lower()
                    if name.endswith(".xml") and "primary" not in name:
                        info_table_file = item.get("name")
                        break
            
            if not info_table_file:
                return [], "Could not find information table XML file in filing"
            
            # Download the information table
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession}/{info_table_file}"
            
            xml_response = await self._rate_limited_request("GET", xml_url)
            
            if xml_response.status_code != 200:
                return [], f"Failed to download info table: status {xml_response.status_code}"
            
            # Parse the XML
            holdings = self._parse_13f_xml(xml_response.text)
            
            return holdings, f"Parsed {len(holdings)} holdings from {info_table_file}"
        
        except Exception as e:
            logger.error(f"Error parsing 13F filing: {e}")
            return [], f"Error parsing filing: {str(e)}"
    
    def _parse_13f_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse 13F information table XML content.
        """
        holdings = []
        
        try:
            # Handle namespace
            # 13F XML typically uses namespace: http://www.sec.gov/edgar/document/thirteenf/informationtable
            
            # Remove namespace for easier parsing (hacky but works)
            xml_content = re.sub(r'\s+xmlns\s*=\s*"[^"]+"', '', xml_content)
            xml_content = re.sub(r'<ns\d+:', '<', xml_content)
            xml_content = re.sub(r'</ns\d+:', '</', xml_content)
            
            root = ET.fromstring(xml_content)
            
            # Find all infoTable entries
            for info_table in root.iter('infoTable'):
                holding = {}
                
                # Extract fields
                name_elem = info_table.find('.//nameOfIssuer')
                if name_elem is not None:
                    holding['nameOfIssuer'] = name_elem.text
                
                cusip_elem = info_table.find('.//cusip')
                if cusip_elem is not None:
                    holding['cusip'] = cusip_elem.text
                
                value_elem = info_table.find('.//value')
                if value_elem is not None:
                    try:
                        holding['value'] = int(value_elem.text)
                    except (ValueError, TypeError):
                        holding['value'] = 0
                
                # Shares or principal amount
                shrs_elem = info_table.find('.//shrsOrPrnAmt')
                if shrs_elem is not None:
                    holding['shrsOrPrnAmt'] = {}
                    ssh_elem = shrs_elem.find('.//sshPrnamt')
                    if ssh_elem is not None:
                        try:
                            holding['shrsOrPrnAmt']['sshPrnamt'] = int(ssh_elem.text)
                        except (ValueError, TypeError):
                            holding['shrsOrPrnAmt']['sshPrnamt'] = 0
                    
                    type_elem = shrs_elem.find('.//sshPrnamtType')
                    if type_elem is not None:
                        holding['shrsOrPrnAmt']['sshPrnamtType'] = type_elem.text
                
                if holding.get('nameOfIssuer'):
                    holdings.append(holding)
            
            return holdings
        
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return []
        except Exception as e:
            logger.error(f"Error parsing 13F XML: {e}")
            return []
