"""
GraphQL API layer for Nexdata.

Provides flexible querying of investors, portfolio companies, and analytics.
"""
from app.graphql.schema import graphql_app, schema

__all__ = ["graphql_app", "schema"]
