# Data Directory

This directory contains data files, outputs, and cached metadata for the External Data Ingestion Service.

## Contents

### Metadata Files

- `census_variable_metadata.csv` - Census variable metadata (original)
- `census_variable_metadata_updated.csv` - Census variable metadata (updated version)

### Output Data

- `sec_data_output/` - SEC company data output files (JSON format)
  - Contains 224+ company data files

### Other Files

- `api_docs.html` - Generated API documentation

## Usage Notes

- This directory is typically excluded from version control (see `.gitignore`)
- Data files may be large and are generated/downloaded by the ingestion service
- Do not manually edit files in this directory unless you know what you're doing
- The ingestion service will automatically create subdirectories as needed

## Data Safety

⚠️ **Important:** This directory may contain sensitive or large datasets. Always:
- Respect data licensing requirements
- Never commit large data files to version control
- Be mindful of PII and data privacy regulations
- Back up important data before running destructive operations

