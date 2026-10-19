# datapond registry

Central registry of curated DuckDB databases built from public government and research data.

## What is datapond?

datapond is a collection of clean, queryable DuckDB databases built from messy public data sources. Each database is:

- **Reproducible** -- built from public source files with a scripted pipeline
- **Queryable** -- stored as a single `.duckdb` file with documented tables
- **Accessible** -- query over HTTP without downloading the full file first, or download a local copy for repeated disk reads. Transfer volume and runtime depend on the query
- **Documented** -- includes a `_metadata` table and full README

## Quick start

```bash
uv pip install datapond
```

```python
import datapond

# See what's available
datapond.list()

# Connect and query over HTTP (no full download required first)
con = datapond.connect('eoir')
con.sql("SELECT * FROM proceedings LIMIT 5").show()

con.close()

# Optional: download the complete file for local use
datapond.download('eoir')
```

Or from R ([CRAN package](https://CRAN.R-project.org/package=datapond)):

```r
install.packages("datapond")
library(datapond)
dp_list()
con <- dp_connect("eoir")
DBI::dbGetQuery(con, "SELECT * FROM proceedings LIMIT 5")
dp_disconnect(con)
```

## Available databases

Row totals sum stored data tables, which may have different units and overlapping records; they are not counts of unique people or events. File size is disk storage, not required RAM. Source cadence and a build date do not promise an automatic future refresh. See each dataset's README for keys, coverage and query limits.

| Database | Rows | Tables | Size | Coverage | Last rebuilt | Source |
|----------|------|--------|------|----------|--------------|--------|
| [eoir](https://github.com/ian-nason/eoir-database) | 173.3M | 97 | 4.40 GiB | 1990-2026 | 2026-09-14 | DOJ Executive Office for Immigration Review |
| [ice](https://github.com/ian-nason/ice-database) | 23.6M | 6 | 1.80 GiB | 2003-2026 | 2026-09-14 | Deportation Data Project (FOIA litigation) |
| [fec](https://github.com/ian-nason/fec-database) | 356.3M | 10 | 19.69 GiB | 2004-2026 | 2026-10-18 | Federal Election Commission |
| [clinicaltrials](https://github.com/ian-nason/clinicaltrials-database) | 59.0M | 48 | 7.10 GiB | 1999-2026 | 2026-09-14 | AACT / Clinical Trials Transformation Initiative |
| [cms-medicare](https://github.com/ian-nason/cms-medicare-database) | 132.9M | 3 | 13.00 GiB | 2013-2024 | 2026-07-06 | Centers for Medicare & Medicaid Services |
| [openpayments](https://github.com/ian-nason/openpayments-database) | 172.5M | 6 | 17.50 GiB | 2013-2025 | 2026-07-07 | CMS Open Payments (openpaymentsdata.cms.gov) |
| [dol-visas](https://github.com/ian-nason/dol-visas-database) | 8.8M | 2 | 2.80 GiB | FY2015-FY2026 | 2026-09-14 | DOL Office of Foreign Labor Certification disclosure files |
| [ipeds-db](https://github.com/ian-nason/ipeds-database) | 27.9M | 23 | 1.30 GiB | 1997-2025 | 2026-09-14 | National Center for Education Statistics (NCES) |
| [fjc](https://github.com/ian-nason/fjc-database) | 21.1M | 3 | 2.50 GiB | 1970-2026 | 2026-09-16 | Federal Judicial Center (Integrated Database) |
| [cbp](https://github.com/ian-nason/cbp-database) | 51.3M | 8 | 1.60 GiB | 2000-2026 | 2026-09-15 | Deportation Data Project (CBP FOIA releases) |
| [ussc](https://github.com/ian-nason/ussc-database) | 14.0M | 9 | 0.7 GiB | FY2002-FY2025 | 2026-09-16 | U.S. Sentencing Commission |
| [cook-sao](https://github.com/ian-nason/cook-sao-database) | 3.2M | 5 | 0.4 GiB | 2011-2024 | 2026-09-15 | Cook County State's Attorney's Office (county open-data portal) |
| [fjc-judges](https://github.com/ian-nason/fjc-judges-database) | 37.8K | 6 | 3.5 MiB | 1789-2026 | 2026-10-17 | Federal Judicial Center (Biographical Directory of Article III Federal Judges) |
| [scdb](https://github.com/ian-nason/scdb-database) | 677.5K | 9 | 0.1 GiB | 1791-2026 | 2026-09-15 | The Supreme Court Database (Washington University in St. Louis) |
| [open-policing](https://github.com/ian-nason/open-policing-database) | 230.0M | 2 | 8.10 GiB | 2000-2020 | 2026-09-16 | Stanford Open Policing Project |
| [courtlistener](https://github.com/ian-nason/courtlistener-database) | 2.3M | 20 | 3.03 GiB | 2026-09-30 snapshot; disclosure reports 1987-2022; other tables vary | 2026-10-18 | Free Law Project (CourtListener bulk data) |
| [onet](https://github.com/ian-nason/onet-database) | 468.9K | 12 | 10.8 MiB | 31.0 snapshot; rating update months 2005-12–2026-08 | 2026-10-18 | U.S. Department of Labor, Employment and Training Administration (O*NET) |
| [qcew](https://github.com/ian-nason/qcew-database) | 3.1M | 6 | 186.5 MiB | 2025 annual; snapshot acquired 2026-10-18 | 2026-10-18 | U.S. Bureau of Labor Statistics, Quarterly Census of Employment and Wages |
| [fars](https://github.com/ian-nason/fars-database) | 190.8K | 4 | 25.3 MiB | 2024 Annual Report File; first release 2026-04-01 | 2026-10-18 | NHTSA National Center for Statistics and Analysis, Fatality Analysis Reporting System |

See [CHANGELOG.md](CHANGELOG.md) for refresh history.

## Registry format

The [`registry.json`](registry.json) file contains metadata for all databases. Each entry includes:

- `id` -- short identifier used by the client packages (quote it in SQL if it contains a hyphen)
- `name` -- human-readable name
- `description` -- what the database contains
- `rows`, `tables`, `size_gb` (in GiB) -- scale information
- `source`, `source_url` -- original data source
- `github` -- build repository
- `huggingface` -- Hugging Face dataset page
- `attach_url` -- direct URL for DuckDB remote attach
- `dictionary_url` -- link to the database's `DICTIONARY.md`
- `data_date_range` -- period the data covers (e.g. `2004-2026`, `FY2015-FY2026`)
- `last_rebuilt` -- date the `.duckdb` file was last built (`YYYY-MM-DD`)
- `update_frequency` -- source cadence and rebuild policy; this field does not guarantee an automatic refresh
- `maintainer` -- who maintains this database
- `license` -- data license
- `updated` -- registry update date; newer clients also compare remote file identity when updating a local copy

The website renders `dictionary_url`, `data_date_range`, `last_rebuilt`, and `update_frequency` on every card, so please fill them in.

## Contributors

| Who | What |
|-----|------|
| [Ian Nason](https://github.com/ian-nason) | Registry, [Python client](https://github.com/datapond-db/datapond-python), [website](https://github.com/datapond-db/website), and the databases attributed to him in `registry.json` |
| [Paul Goldsmith-Pinkham](https://github.com/paulgp) (`paulgp`, HF `paulgp85`) | Build pipeline and original release of the `ipeds-db` database (the datapond copy is rebuilt from a fork) |

Each database's build repository is linked in the table above; the `maintainer` field in `registry.json` records who owns each entry.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add a new database to the registry.

## Links

- **Python package:** [pypi.org/project/datapond](https://pypi.org/project/datapond/)
- **R package:** [CRAN](https://CRAN.R-project.org/package=datapond) (0.1.0); [development source](https://github.com/datapond-db/datapond-r)
- **Website:** [datapond-db.github.io/website](https://datapond-db.github.io/website)
- **GitHub org:** [github.com/datapond-db](https://github.com/datapond-db)

## License

This registry is licensed under the MIT License. Individual databases have their own licenses as specified in the registry.
