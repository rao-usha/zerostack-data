"""
Unit tests for Census metadata parsing.

Uses fixture data - no network requests.
"""
import pytest
from app.sources.census.metadata import (
    clean_column_name,
    map_census_type_to_postgres,
    parse_table_metadata,
    generate_create_table_sql,
    build_column_mapping
)


@pytest.mark.unit
def test_clean_column_name_basic():
    """Test basic column name cleaning."""
    assert clean_column_name("B01001_001E") == "b01001_001e"
    assert clean_column_name("NAME") == "name"
    assert clean_column_name("GEO_ID") == "geo_id"


@pytest.mark.unit
def test_clean_column_name_special_characters():
    """Test column name cleaning with special characters."""
    assert clean_column_name("COLUMN-NAME") == "column_name"
    assert clean_column_name("COLUMN.NAME") == "column_name"
    assert clean_column_name("COLUMN NAME") == "column_name"


@pytest.mark.unit
def test_clean_column_name_starts_with_digit():
    """Test column names starting with digits."""
    assert clean_column_name("123ABC") == "col_123abc"
    assert clean_column_name("1") == "col_1"


@pytest.mark.unit
def test_clean_column_name_reserved_keywords():
    """Test handling of SQL reserved keywords."""
    assert clean_column_name("USER") == "user_col"
    assert clean_column_name("TABLE") == "table_col"
    assert clean_column_name("SELECT") == "select_col"


@pytest.mark.unit
def test_map_census_type_to_postgres():
    """Test Census type to Postgres type mapping."""
    assert map_census_type_to_postgres("int") == "INTEGER"
    assert map_census_type_to_postgres("INT") == "INTEGER"
    assert map_census_type_to_postgres("float") == "NUMERIC"
    assert map_census_type_to_postgres("FLOAT") == "NUMERIC"
    assert map_census_type_to_postgres("string") == "TEXT"
    assert map_census_type_to_postgres("STRING") == "TEXT"
    
    # Unknown type defaults to TEXT
    assert map_census_type_to_postgres("unknown") == "TEXT"


@pytest.mark.unit
def test_parse_table_metadata_filters_by_table(sample_census_metadata):
    """Test that parse_table_metadata filters to specific table."""
    result = parse_table_metadata(sample_census_metadata, "B01001")
    
    # Should include B01001 variables
    assert "B01001_001E" in result
    assert "B01001_002E" in result
    assert "B01001_003E" in result
    assert "B01001_001M" in result
    
    # Should NOT include B02001 variables
    assert "B02001_001E" not in result
    
    # Should NOT include annotation variables
    # (none in our sample, but test won't break if added)


@pytest.mark.unit
def test_parse_table_metadata_structure(sample_census_metadata):
    """Test structure of parsed metadata."""
    result = parse_table_metadata(sample_census_metadata, "B01001")
    
    var = result["B01001_001E"]
    
    assert "label" in var
    assert "concept" in var
    assert "predicate_type" in var
    assert "postgres_type" in var
    assert "column_name" in var
    
    assert var["label"] == "Estimate!!Total:"
    assert var["concept"] == "SEX BY AGE"
    assert var["postgres_type"] == "INTEGER"
    assert var["column_name"] == "b01001_001e"


@pytest.mark.unit
def test_parse_table_metadata_type_mapping(sample_census_metadata):
    """Test that types are correctly mapped."""
    result = parse_table_metadata(sample_census_metadata, "B01001")
    
    # All B01001 variables in sample are integers
    for var_name, var_meta in result.items():
        assert var_meta["postgres_type"] == "INTEGER"


@pytest.mark.unit
def test_generate_create_table_sql_basic(sample_census_metadata):
    """Test SQL generation for table creation."""
    table_vars = parse_table_metadata(sample_census_metadata, "B01001")
    
    sql = generate_create_table_sql("acs5_2023_b01001", table_vars)
    
    # Should be a CREATE TABLE statement
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "acs5_2023_b01001" in sql
    
    # Should have ID column
    assert "id SERIAL PRIMARY KEY" in sql
    
    # Should have geography columns
    assert "geo_name TEXT" in sql
    assert "geo_id TEXT" in sql
    assert "state_fips TEXT" in sql
    
    # Should have data columns
    assert "b01001_001e INTEGER" in sql
    assert "b01001_002e INTEGER" in sql


@pytest.mark.unit
def test_generate_create_table_sql_without_geo(sample_census_metadata):
    """Test SQL generation without geography columns."""
    table_vars = parse_table_metadata(sample_census_metadata, "B01001")
    
    sql = generate_create_table_sql(
        "acs5_2023_b01001",
        table_vars,
        include_geo_columns=False
    )
    
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "id SERIAL PRIMARY KEY" in sql
    
    # Should NOT have geography columns
    assert "geo_name" not in sql
    assert "geo_id" not in sql


@pytest.mark.unit
def test_build_column_mapping(sample_census_metadata):
    """Test column mapping generation."""
    table_vars = parse_table_metadata(sample_census_metadata, "B01001")
    
    mapping = build_column_mapping(table_vars)
    
    # Should map Census variable names to Postgres column names
    assert mapping["B01001_001E"] == "b01001_001e"
    assert mapping["B01001_002E"] == "b01001_002e"
    assert mapping["B01001_003E"] == "b01001_003e"
    
    # All variables should be in mapping
    assert len(mapping) == len(table_vars)


@pytest.mark.unit
def test_parse_empty_metadata():
    """Test parsing with empty metadata."""
    result = parse_table_metadata({"variables": {}}, "B01001")
    
    assert result == {}


@pytest.mark.unit
def test_parse_metadata_missing_variables_key():
    """Test parsing with missing variables key."""
    result = parse_table_metadata({}, "B01001")
    
    assert result == {}





