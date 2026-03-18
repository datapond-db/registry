# datapond registry

Central registry of curated DuckDB databases built from public government and research data.

## What is datapond?

datapond is a collection of clean, queryable DuckDB databases built from messy public data sources. Each database is:

- **Reproducible** -- built from public source files with a scripted pipeline
- **Queryable** -- stored as a single `.duckdb` file with documented tables
- **Accessible** -- every database can be queried remotely in seconds with no download required, or downloaded locally for full speed
- **Documented** -- includes a `_metadata` table and full README

## Quick start

```bash
uv pip install datapond
```

```python
import datapond

# See what's available
datapond.list()

# Connect and query instantly (streams over HTTP, no download)
con = datapond.connect('eoir')
con.sql("SELECT * FROM proceedings LIMIT 5").show()

# Download for local use
datapond.download('eoir')
```

## Available databases

| Database | Rows | Tables | Size | Source |
|----------|------|--------|------|--------|
| [eoir](https://github.com/ian-nason/eoir-database) | 164.6M | 98 | 6.6 GB | DOJ Executive Office for Immigration Review |
| [ice](https://github.com/ian-nason/ice-database) | 17.8M | 5 | 2.0 GB | Deportation Data Project (FOIA litigation) |
| [fec](https://github.com/ian-nason/fec-database) | 269.0M | 10 | 29.0 GB | Federal Election Commission |
| [clinicaltrials](https://github.com/ian-nason/clinicaltrials-database) | 56.4M | 48 | 5.8 GB | AACT / ClinicalTrials.gov |
| [cms-medicare](https://github.com/ian-nason/cms-medicare-database) | 121.7M | 3 | 9.7 GB | CMS Medicare Physician & Other Practitioners |
| [ipeds-db](https://github.com/paulgp/ipeds-database) | 26.7M | 23 | 1.1 GB | NCES Integrated Postsecondary Education Data System |

## Registry format

The [`registry.json`](registry.json) file contains metadata for all databases. Each entry includes:

- `id` -- short identifier used by the Python package
- `name` -- human-readable name
- `description` -- what the database contains
- `rows`, `tables`, `size_gb` -- scale information
- `source`, `source_url` -- original data source
- `github` -- build repository
- `huggingface` -- Hugging Face dataset page
- `attach_url` -- direct URL for DuckDB remote attach
- `maintainer` -- who maintains this database
- `license` -- data license
- `updated` -- last update date

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add a new database to the registry.

## Links

- **Python package:** [pypi.org/project/datapond](https://pypi.org/project/datapond/)
- **Website:** [datapond-db.github.io/website](https://datapond-db.github.io/website)
- **GitHub org:** [github.com/datapond-db](https://github.com/datapond-db)

## License

This registry is licensed under the MIT License. Individual databases have their own licenses as specified in the registry.
