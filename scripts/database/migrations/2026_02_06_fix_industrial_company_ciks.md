# Migration: Fix Industrial Company CIKs

**Date:** 2026-02-06
**Issue:** Incorrect SEC CIK mappings preventing people extraction

## Problem

Two industrial companies had incorrect CIK (Central Index Key) mappings that prevented SEC proxy statement extraction:

| Company | Old CIK | Pointed To | Correct CIK |
|---------|---------|------------|-------------|
| Illinois Tool Works | 0000052795 | Anixter International Inc (deregistered 2020) | 0000049826 |
| Beacon Building Products | 0001333188 | Cambridge Logistics LLC | 0001124941 |

## Solution

Run the following SQL to correct the CIK mappings:

```sql
-- Illinois Tool Works
-- Old CIK 0000052795 was actually Anixter International Inc (deregistered in 2020)
-- Correct CIK is 0000049826 for Illinois Tool Works Inc
UPDATE industrial_companies
SET cik = '0000049826'
WHERE id = 31 AND name = 'Illinois Tool Works';

-- Beacon Building Products (actually Beacon Roofing Supply Inc)
-- Old CIK 0001333188 was actually Cambridge Logistics LLC
-- Correct CIK is 0001124941 for Beacon Roofing Supply Inc
UPDATE industrial_companies
SET cik = '0001124941'
WHERE id = 97 AND name = 'Beacon Building Products';
```

## Verification

After running the migration, verify with:

```sql
SELECT id, name, cik
FROM industrial_companies
WHERE id IN (31, 97);
```

Expected output:
```
 id |           name           |    cik
----+--------------------------+------------
 31 | Illinois Tool Works      | 0000049826
 97 | Beacon Building Products | 0001124941
```

## Result

After applying this fix:
- Illinois Tool Works: 5 people extracted
- Beacon Building Products: 11 people extracted

## How CIKs Were Found

Used SEC's company tickers API to find correct CIKs:
- ITW ticker → CIK 0000049826
- BECN ticker (Beacon Roofing Supply) → CIK 0001124941
