# Datapond Changelog

## 2026-09-16 — Response to the September 15 public audit

An independent audit of all 19 public repositories and 14 published databases (5 P1 and
11 P2 findings) was addressed in source and, for the two databases below, in the published
files. Its 2026-09-16 recheck found the catalogs still reconcile and 59/59 README SQL
statements bind, and reported the remaining gaps, which are fixed in the follow-up section
below; the Python client fixes reach users only once 0.1.4 is on PyPI. Data rows did not
change.

**Clients**
- `datapond` 0.1.4 (Python): downloads stream to a temporary file and replace the
  destination only after the file verifies as DuckDB, so a failed download never damages
  an existing copy; `update()` compares the remote file's ETag/size with the identity
  recorded at download time instead of the file's modification time; `connect()` returns
  the native DuckDB connection (pandas replacement scans work); paths with apostrophes
  are quoted; `--path some/dir/` creates the directory. Offline test suite added.
- `datapond` 0.1.1 (R): a partial download is resumed only while the remote revision is
  unchanged (ETag/size/Last-Modified recorded beside the `.part` file); the finished file
  is validated and a failed replacement is an error; `dp_update()` uses the recorded file
  identity; the registry is decoded as UTF-8 explicitly (works in a C locale).
- `datapond-build` 0.1.3: atomic `os.replace` downloads, `refresh="if-changed"` with a
  sidecar identity, column statistics in batches, HF `license_name`/`license_link`.

**Databases**
- fjc: `v_criminal_by_year` grouped on `FISCALYR` only and put 1.42M 1970-95 defendants in
  a NULL year; it now uses the harmonised `fiscal_year` (era-specific offense codes kept
  apart) and the build asserts the view reconciles with the table. Civil rows are
  documented as case records (reopenings carry `ORIGIN` 4-7), not unique cases.
- ussc: the quick starts averaged raw `TOTPRISN`, whose 9992/9996/9997/9998 are codes
  (under a day / life / no term stated / death); `v_sentence_terms` separates them and
  the examples use it.
- scdb: the README's majority-share example did not run (`AVG(BOOLEAN)`); fixed and its
  population labelled honestly.
- cbp: `refresh_releases.py` regenerates the file manifest from the DDP page; tiling
  counts in the README now come from `_files` (19 base / 23 superseded).
- eoir: README validation numbers current (charge lookup 99.997%); coverage 1990-2026.
- Source refresh vs resume: eoir, fec and dol-visas re-download a cached source file when
  the server's ETag/Last-Modified/size differs from the one recorded at download time
  (eoir discards the old extraction); openpayments reloads a program year whose source
  file changed (`_sources`); `DATAPOND_REFRESH=skip` keeps the old resume behaviour.
- ipeds: a missing required survey year now fails the build; `_columns` is built in the
  pipeline; ZIP/OPEID/EIN/FIPS identifiers are never numeric-typed.
- Download scripts (fjc-judges, scdb, cook-sao, ussc, cbp) create `data/raw`, fail on
  HTTP errors and exit non-zero.
- HF card licenses now match the registry (public domain / CC0), and sub-0.1 GB sizes
  render in MB; `scripts/add_metadata.py` delegates to datapond-build and refreshes
  counts on rerun.
- The quick-start gate (`tools/run_quickstarts.py`) now binds every SQL statement in
  every dataset README against the live schema.

**Follow-up to the 2026-09-16 recheck**
- ussc: `TOTPRISN = 0` means "no prison or under one month", not "no prison";
  `v_sentence_terms` labels it so and adds `prison_imposed` from `PRISDUM`, which the
  incidence example now uses (FY2025: 92.3% with a prison sentence, not 66.8%).
  Republished.
- R client: a resume is bound to a strong validator with `If-Range`, the GET response's
  validator is checked (a file that changes between HEAD and GET is re-downloaded from
  scratch), size alone never proves freshness, and a directory at the destination is
  rejected. Python client: the same validator rules for `update()` and the HEAD/GET check.
- openpayments: a year refresh loads into a staging table, rejects an empty or shrunken
  replacement, and swaps the partition in a transaction, so a bad source cannot erase a
  year.
- `scripts/add_metadata.py` imports from `datapond_build.metadata` (it could not import);
  datapond-build 0.1.4 exports `user_tables`. Exercised twice through the CLI.
- scdb's download script lists the ten files that exist (no 404s on legacy units).

**Follow-up to the 2026-09-16 third-round audit**
- R client: real downloads failed (F17) because curl returns response headers as a
  vector of lines; fixed, together with two resume defects (a 206 response's length is
  only the remaining range; a stale `If-Range` answered with the whole file now restarts
  the transfer). The download, resume and revision-change paths are exercised against a
  real loopback HTTP server in the test suite, and the public fjc-judges download was
  re-run end to end.
- openpayments: a replacement year must also map and parse its required columns before
  it can replace the loaded one: `record_id` (no NULLs) in every table, payment amounts
  (at most 0.1% NULL for general and research payments, 1% for ownership investment
  amounts) and, for general payments, `date_of_payment` (at most 1% NULL). A renamed
  header or unparseable amounts are rejected and the existing year kept.

## 2026-09-15 — Six legal-system databases: fjc, cbp, ussc, cook-sao, scdb, fjc-judges

New databases, all built with [datapond-build](https://github.com/datapond-db/datapond-build)
and the same regression-check discipline as the September refresh. The registry now
lists 14 databases and 1.04B rows:

| Database | Rows | Coverage | What it is |
|----------|-----:|----------|------------|
| fjc | 21.1M | 1970-2026 | Federal Judicial Center Integrated Database: every federal civil case (1988+), criminal defendant (FY1970+) and appeal (1971+). Two file eras per table unioned by column name; invalid source dates audited; 638 malformed civil rows (0.006%) skipped and counted. Bankruptcy is not published in bulk by the FJC. |
| cbp | 51.3M | 2000-2026 | Deportation Data Project CBP FOIA releases: Border Patrol apprehensions, apprehensions with place of birth, encounters, OFO inadmissibility events, Title 42 expulsions, CBP One. 121 heterogeneous xlsx files harmonised through a header crosswalk; overlapping inadmissibles releases tiled into a base table plus `inadmissibles_superseded`; `_files` documents every source file. |
| scdb | 677K | 1791-2026 | The Supreme Court Database (Washington University), modern release 2026_01 plus Legacy 07: all four units of analysis at case and justice level, with the online code book scraped into a `codes` table and a decoded `v_cases` view. CC BY-NC 3.0. |
| fjc-judges | 38K | 1789-2026 | FJC Biographical Directory of Article III judges: six normalised tables (judges, appointments with nomination/confirmation/commission dates, education, career, other service, failed nominations) and a `v_current_judges` view. |
| cook-sao | 3.2M | 2011-2024 | Cook County (Chicago) State's Attorney felony cases, charge by charge: intake/felony review, initiation with statute, class and bond, dispositions with judge and court, sentences, diversion referrals. The Office closed the series on 2024-12-30, so this is a complete, fixed dataset. Typed from the portal's own column metadata; co-defendants share charge ids; dispositions and sentencing reach back to cases received before 2011. |
| ussc | 14.0M | FY2002-FY2025 | U.S. Sentencing Commission individual offender datafiles: 1,720,684 sentenced defendants (per-year counts match the Commission's Sourcebook); the 22,000-variable fixed-width layout reshaped into `sentences` plus long tables per count of conviction, guideline computation, drug type and departure reason. Datapond-build 0.1.1 batches column statistics so 500-column tables fit in a 3 GB build. |

Also: `datapond-python` and `datapond-r` `describe()` now tolerate contributed
dictionaries that lack optional columns; the health check counts only data tables.

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
