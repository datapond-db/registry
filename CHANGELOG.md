# Datapond Changelog

## 2026-09-14 — September refresh

Six databases rebuilt from their latest sources with the July audit's repairs
now enforced as regression checks inside every build (a failing check aborts
the publish). Per-database details are in each repo's CHANGELOG.md and on the
HuggingFace dataset pages.

| Database | Rows (old → new) | Data now through | Headline changes |
|----------|------------------|------------------|------------------|
| eoir | 169.2M → 173.3M | Aug 2026 DOJ file | Lookup normalization built in; untyped fallback now fails loudly |
| ice | 22.0M → 23.6M | 2026-08-06 | DDP Aug 2026 release supersedes March; new `arrests.city`; encounters exact-dedup in build; `data_source` = `release_2026_08` |
| clinicaltrials | 58.0M → 59.0M | AACT 2026-09-14 | AACT moved its download page; phantom `search_results` table dropped by the builder |
| dol-visas | 8.5M → 8.8M | FY2026 Q3 | **Fix:** date columns were 100% NULL since July (Excel serials); new-column guard |
| fec | 347.2M → 354.9M | 2026 cycle (bulk files as of 2026-09-14) | 12-cycle coverage, date-parse rates, view semantics and conduit exclusion enforced at build; file 37.7 → 35.2 GB |

| ipeds-db | 26.7M → 27.9M | 2025-26 provisional (fall) / 2024-25 (winter, spring) | NCES moved its files to `/ipeds/complete-data-files/`; rebuilt from a datapond fork of Paul Goldsmith-Pinkham's pipeline (github.com/ian-nason/ipeds-database) with a streaming loader; now hosted at `Nason/ipeds-database` |

Not rebuilt: cms-medicare (CY2024 already included) and openpayments (PY2025
already included; next refresh January 2027).

Also this round: **datapond 0.1.3** (Python) fixes the README examples, the
Hugging Face download filename for hyphenated ids, and makes `describe()`
tolerant of contributed dictionaries; **datapond-r 0.1.0** is a new R client
(github.com/datapond-db/datapond-r).

## 2026-07-06 — Full refresh + data-quality audit

All five existing Nason-maintained databases were rebuilt from their latest
sources, then independently audited from a researcher's perspective (every
finding SQL-verified), repaired, and republished. Two new databases were added
the next day. Per-database details live in each repo's CHANGELOG.md and on the
HuggingFace dataset pages.

| Database | Rows (old → new) | Data now through | Headline changes |
|----------|------------------|------------------|------------------|
| eoir | 164.6M → 169.2M | 2026 | Values normalized: charge-lookup joins 77% → 100% coverage |
| ice | 17.8M → 22.0M | 2026-03-10 | New `encounters` table; 2026 FOIA release; views rewritten (were producing negative durations); cross-release double counting removed |
| fec | 339.5M → 347.2M | 2026 cycle | Standalone-table dates fixed (were 100% NULL); IE amendments deduplicated; views rebuilt with conduit/refund-correct money math |
| clinicaltrials | 56.4M → 58.0M | June 2026 | ACTUAL/ESTIMATED date-type columns restored; docs corrected |
| cms-medicare | 121.7M → 132.9M | CY2024 | CY2024 added; corrupted geography HCPCS codes and NPI/ZIP typing repaired; suppression undercount documented |
| openpayments | new → 172.5M | PY2025 | **New** (2026-07-07): CMS Open Payments (Sunshine Act) general, research, and ownership payments, PY2013–PY2025 |
| dol-visas | new → 8.5M | FY2026 Q2 | **New** (2026-07-07): DOL OFLC H-1B/H-1B1/E-3 LCA and PERM disclosure data, FY2015–present |

Also released: **datapond 0.1.2** on PyPI — fixes `connect()`/CLI for
hyphenated database IDs (`cms-medicare`, `ipeds-db`).

Every database README now carries a researcher-caveats section covering the
semantics traps the audit surfaced (suppression undercounts, granularity,
coverage windows, upstream dirty dates, how to sum FEC money correctly).

ipeds-db is maintained upstream by paulgp85 and was health-checked but not
rebuilt.
