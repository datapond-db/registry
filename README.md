# datapond registry

Central registry of curated DuckDB databases built from public government and research data.

## What is datapond?

datapond is a collection of clean, queryable DuckDB databases built from messy public data sources. Each database is:

- **Reproducible** -- built from public source files with a scripted pipeline
- **Queryable** -- stored as a single `.duckdb` file with documented tables
- **Accessible** -- every database can be queried remotely in seconds with no full download (DuckDB fetches only the byte ranges a query needs), or downloaded locally for full speed
- **Documented** -- includes a `_metadata` table and full README

## Quick start

```bash
uv pip install datapond
```

```python
import datapond

# See what's available
datapond.list()

# Connect and query instantly (attaches over HTTP; no full download)
con = datapond.connect('eoir')
con.sql("SELECT * FROM proceedings LIMIT 5").show()

# Download for local use
datapond.download('eoir')
```

Or from R ([datapond-r](https://github.com/datapond-db/datapond-r)):

```r
pak::pak("datapond-db/datapond-r")
library(datapond)
dp_list()
con <- dp_connect("eoir")
DBI::dbGetQuery(con, "SELECT * FROM proceedings LIMIT 5")
```

## Available databases

| Database | Rows | Tables | Size | Coverage | Last rebuilt | Source |
|----------|------|--------|------|----------|--------------|--------|
| [eoir](https://github.com/ian-nason/eoir-database) | 173.3M | 97 | 4.4 GB | 1990-2026 | 2026-09-14 | DOJ Executive Office for Immigration Review |
| [ice](https://github.com/ian-nason/ice-database) | 23.6M | 6 | 1.8 GB | 2003-2026 | 2026-09-14 | Deportation Data Project (FOIA litigation) |
| [fec](https://github.com/ian-nason/fec-database) | 354.9M | 10 | 35.2 GB | 2004-2026 | 2026-09-14 | Federal Election Commission |
| [clinicaltrials](https://github.com/ian-nason/clinicaltrials-database) | 59.0M | 48 | 7.1 GB | 1999-2026 | 2026-09-14 | AACT / Clinical Trials Transformation Initiative |
| [cms-medicare](https://github.com/ian-nason/cms-medicare-database) | 132.9M | 3 | 13.0 GB | 2013-2024 | 2026-07-06 | Centers for Medicare & Medicaid Services |
| [openpayments](https://github.com/ian-nason/openpayments-database) | 172.5M | 6 | 17.5 GB | 2013-2025 | 2026-07-07 | CMS Open Payments (openpaymentsdata.cms.gov) |
| [dol-visas](https://github.com/ian-nason/dol-visas-database) | 8.8M | 2 | 2.8 GB | FY2015-FY2026 | 2026-09-14 | DOL Office of Foreign Labor Certification disclosure files |
| [ipeds-db](https://github.com/ian-nason/ipeds-database) | 27.9M | 23 | 1.3 GB | 1997-2025 | 2026-09-14 | National Center for Education Statistics (NCES) |
| [fjc](https://github.com/ian-nason/fjc-database) | 21.1M | 3 | 2.5 GB | 1970-2026 | 2026-09-15 | Federal Judicial Center (Integrated Database) |
| [cbp](https://github.com/ian-nason/cbp-database) | 51.3M | 8 | 1.6 GB | 2000-2026 | 2026-09-15 | Deportation Data Project (CBP FOIA releases) |
| [ussc](https://github.com/ian-nason/ussc-database) | 14.0M | 9 | 0.7 GB | FY2002-FY2025 | 2026-09-15 | U.S. Sentencing Commission |
| [cook-sao](https://github.com/ian-nason/cook-sao-database) | 3.2M | 5 | 0.4 GB | 2011-2024 | 2026-09-15 | Cook County State's Attorney's Office (county open-data portal) |
| [fjc-judges](https://github.com/ian-nason/fjc-judges-database) | 37.8K | 6 | 0.003 GB | 1789-2026 | 2026-09-15 | Federal Judicial Center (Biographical Directory of Article III Federal Judges) |
| [scdb](https://github.com/ian-nason/scdb-database) | 677.5K | 9 | 0.1 GB | 1791-2026 | 2026-09-15 | The Supreme Court Database (Washington University in St. Louis) |

See [CHANGELOG.md](CHANGELOG.md) for refresh history.

## Registry format

The [`registry.json`](registry.json) file contains metadata for all databases. Each entry includes:

- `id` -- short identifier used by the client packages (quote it in SQL if it contains a hyphen)
- `name` -- human-readable name
- `description` -- what the database contains
- `rows`, `tables`, `size_gb` -- scale information
- `source`, `source_url` -- original data source
- `github` -- build repository
- `huggingface` -- Hugging Face dataset page
- `attach_url` -- direct URL for DuckDB remote attach
- `dictionary_url` -- link to the database's `DICTIONARY.md`
- `data_date_range` -- period the data covers (e.g. `2004-2026`, `FY2015-FY2026`)
- `last_rebuilt` -- date the `.duckdb` file was last built (`YYYY-MM-DD`)
- `update_frequency` -- how often the database is rebuilt (e.g. `Monthly`, `Quarterly`)
- `maintainer` -- who maintains this database
- `license` -- data license
- `updated` -- last update date (clients compare this to a local copy's timestamp)

The website renders `dictionary_url`, `data_date_range`, `last_rebuilt`, and `update_frequency` on every card, so please fill them in.

## Contributors

| Who | What |
|-----|------|
| [Ian Nason](https://github.com/ian-nason) | Registry, [Python client](https://github.com/datapond-db/datapond-python), [website](https://github.com/datapond-db/website), and the `eoir`, `ice`, `fec`, `clinicaltrials`, `cms-medicare`, `openpayments`, and `dol-visas` databases |
| [Paul Goldsmith-Pinkham](https://github.com/paulgp) (`paulgp`, HF `paulgp85`) | Build pipeline and original release of the `ipeds-db` database (the datapond copy is rebuilt from a fork) |

Each database's build repository is linked in the table above; the `maintainer` field in `registry.json` records who owns each entry.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add a new database to the registry.

## Links

- **Python package:** [pypi.org/project/datapond](https://pypi.org/project/datapond/)
- **R package:** [github.com/datapond-db/datapond-r](https://github.com/datapond-db/datapond-r)
- **Website:** [datapond-db.github.io/website](https://datapond-db.github.io/website)
- **GitHub org:** [github.com/datapond-db](https://github.com/datapond-db)

## License

This registry is licensed under the MIT License. Individual databases have their own licenses as specified in the registry.
