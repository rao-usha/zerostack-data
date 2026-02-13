"""
People collectors.

Collectors for gathering data about executives and investors:
- LinkedIn profile scraping
- Bio extraction with LLM
- Education and experience parsing
"""

from app.sources.pe_collection.people_collectors.bio_extractor import BioExtractor

# LinkedIn requires paid API / ToS compliance
# from app.sources.pe_collection.people_collectors.linkedin_people_collector import LinkedInPeopleCollector

__all__ = [
    "BioExtractor",
]
