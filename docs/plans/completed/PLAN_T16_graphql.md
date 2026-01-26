# Plan T16: GraphQL API Layer

**Task ID:** T16
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-16

---

## Goal

Provide a flexible GraphQL API layer for complex frontend queries, enabling nested data fetching, selective field queries, and efficient data loading.

---

## Why GraphQL?

1. **Flexible Queries**: Clients request exactly what they need (no over-fetching)
2. **Nested Data**: Fetch investor + portfolio + co-investors in one query
3. **Type Safety**: Strong schema with introspection
4. **Frontend Friendly**: Modern frontends (React/Vue) work well with GraphQL

---

## Design

### Library Choice: Strawberry GraphQL

- Modern Python GraphQL library with excellent FastAPI integration
- Type-safe with dataclasses/Pydantic support
- Built-in DataLoader for N+1 prevention
- Active development and good documentation

### Schema Design

```graphql
# Core investor type (union of LP and FamilyOffice)
union Investor = LPFund | FamilyOffice

type LPFund {
  id: ID!
  name: String!
  formalName: String
  lpType: String!
  jurisdiction: String
  websiteUrl: String
  createdAt: DateTime!

  # Relationships
  portfolioCompanies(limit: Int, industry: String): [PortfolioCompany!]!
  coInvestors(limit: Int): [CoInvestor!]!
  portfolioCount: Int!
}

type FamilyOffice {
  id: ID!
  name: String!
  legalName: String
  region: String
  country: String
  type: String
  city: String
  stateProvince: String
  website: String
  principalFamily: String
  estimatedWealth: String
  investmentFocus: [String]
  sectorsOfInterest: [String]
  status: String
  createdAt: DateTime!

  # Relationships
  portfolioCompanies(limit: Int, industry: String): [PortfolioCompany!]!
  coInvestors(limit: Int): [CoInvestor!]!
  portfolioCount: Int!
}

type PortfolioCompany {
  id: ID!
  companyName: String!
  companyWebsite: String
  companyIndustry: String
  companyStage: String
  companyLocation: String
  companyTicker: String
  investmentType: String
  investmentDate: DateTime
  marketValueUsd: String
  sharesHeld: String
  ownershipPercentage: String
  currentHolding: Boolean!
  confidenceLevel: String
  sourceType: String
  sourceUrl: String
  collectedDate: DateTime!

  # Relationship back to investor
  investor: Investor
}

type CoInvestor {
  id: ID!
  coInvestorName: String!
  coInvestorType: String
  dealName: String
  dealDate: DateTime
  dealSizeUsd: String
  coInvestmentCount: Int!
  sourceType: String
  sourceUrl: String
  collectedDate: DateTime!
}

# Pagination support
type PageInfo {
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
  startCursor: String
  endCursor: String
  totalCount: Int!
}

type PortfolioCompanyConnection {
  edges: [PortfolioCompanyEdge!]!
  pageInfo: PageInfo!
}

type PortfolioCompanyEdge {
  node: PortfolioCompany!
  cursor: String!
}

# Root queries
type Query {
  # Single entity lookups
  lpFund(id: ID!): LPFund
  familyOffice(id: ID!): FamilyOffice
  portfolioCompany(id: ID!): PortfolioCompany

  # List queries with filtering and pagination
  lpFunds(
    limit: Int = 50
    offset: Int = 0
    lpType: String
    jurisdiction: String
  ): [LPFund!]!

  familyOffices(
    limit: Int = 50
    offset: Int = 0
    region: String
    country: String
    type: String
  ): [FamilyOffice!]!

  portfolioCompanies(
    investorId: Int
    investorType: String
    industry: String
    limit: Int = 50
    offset: Int = 0
  ): [PortfolioCompany!]!

  # Search (leverages T12 search engine)
  search(
    query: String!
    type: String
    limit: Int = 20
  ): [SearchResult!]!

  # Analytics (leverages T13 dashboard)
  analyticsOverview: AnalyticsOverview!
}

type SearchResult {
  id: ID!
  type: String!
  name: String!
  description: String
  score: Float!
}

type AnalyticsOverview {
  totalLpFunds: Int!
  totalFamilyOffices: Int!
  totalPortfolioCompanies: Int!
  coveragePercentage: Float!
}
```

---

## Files to Create

### 1. `app/graphql/__init__.py`
Empty init file for package.

### 2. `app/graphql/types.py`
Strawberry type definitions matching the schema above.

### 3. `app/graphql/resolvers.py`
Resolver functions with DataLoader for N+1 prevention.

### 4. `app/graphql/dataloaders.py`
DataLoader classes for batch loading related data.

### 5. `app/graphql/schema.py`
Schema assembly and FastAPI router registration.

---

## Implementation Details

### DataLoader Pattern (N+1 Prevention)

```python
# When fetching portfolioCompanies for multiple investors,
# batch the queries to avoid N+1

class PortfolioLoader:
    async def load_for_investor(self, investor_id: int, investor_type: str) -> List[PortfolioCompany]:
        # Batched query
        pass
```

### Pagination

- Support both simple (limit/offset) and cursor-based pagination
- Default limit: 50, max limit: 100

### Field Selection

- Strawberry automatically handles field selection
- Only requested fields are fetched from database

### Integration Points

- **Search (T12)**: Leverage existing search engine for search query
- **Analytics (T13)**: Leverage existing dashboard service for analyticsOverview

---

## API Endpoint

```
POST /graphql
```

GraphQL Playground available at `/graphql` (GET) in development.

---

## Example Queries

### 1. Get LP with portfolio companies

```graphql
query {
  lpFund(id: 1) {
    name
    lpType
    jurisdiction
    portfolioCompanies(limit: 10) {
      companyName
      companyIndustry
      marketValueUsd
    }
    portfolioCount
  }
}
```

### 2. Search investors

```graphql
query {
  search(query: "calpers", type: "investor", limit: 5) {
    id
    name
    type
    score
  }
}
```

### 3. List family offices with filters

```graphql
query {
  familyOffices(region: "US", limit: 20) {
    name
    principalFamily
    estimatedWealth
    investmentFocus
    portfolioCount
  }
}
```

---

## Dependencies (requirements.txt additions)

```
strawberry-graphql[fastapi]>=0.220.0
```

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Open GraphQL Playground: http://localhost:8001/graphql
3. Test queries:
   - Single LP lookup
   - List queries with filters
   - Nested portfolio data
   - Search integration
   - Analytics overview

---

## Success Criteria

- [ ] GraphQL endpoint functional at POST /graphql
- [ ] All entity types queryable (LP, FamilyOffice, PortfolioCompany)
- [ ] Nested relationships resolve correctly
- [ ] DataLoaders prevent N+1 queries
- [ ] Search query integrates with T12 search engine
- [ ] Analytics query integrates with T13 dashboard

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Used Strawberry GraphQL with FastAPI integration
- Synchronous database access to match existing codebase patterns
- Portfolio limits controlled at query level (portfolioLimit parameter)
- Integrates with T12 search and T13 analytics

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
