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

## Available databases

| Database | Rows | Tables | Size | Source |
|----------|------|--------|------|--------|
| [eoir](https://github.com/ian-nason/eoir-database) | 169.2M | 97 | 4.6 GB | DOJ Executive Office for Immigration Review |
| [ice](https://github.com/ian-nason/ice-database) | 22.0M | 6 | 1.4 GB | Deportation Data Project (FOIA litigation) |
| [fec](https://github.com/ian-nason/fec-database) | 347.2M | 10 | 37.7 GB | Federal Election Commission |
| [clinicaltrials](https://github.com/ian-nason/clinicaltrials-database) | 58.0M | 48 | 6.6 GB | AACT / ClinicalTrials.gov |
| [cms-medicare](https://github.com/ian-nason/cms-medicare-database) | 132.9M | 3 | 13.0 GB | CMS Medicare Physician & Other Practitioners |
| [openpayments](https://github.com/ian-nason/openpayments-database) | 172.5M | 6 | 17.5 GB | CMS Open Payments (Sunshine Act) |
| [dol-visas](https://github.com/ian-nason/dol-visas-database) | 8.5M | 2 | 3.2 GB | DOL Office of Foreign Labor Certification |
| [ipeds-db](https://github.com/paulgp/ipeds-database) | 26.7M | 23 | 1.1 GB | NCES Integrated Postsecondary Education Data System |

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
| [Paul Goldsmith-Pinkham](https://github.com/paulgp) (`paulgp`, HF `paulgp85`) | The `ipeds-db` database |

Each database's build repository is linked in the table above; the `maintainer` field in `registry.json` records who owns each entry.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add a new database to the registry.

## Links

- **Python package:** [pypi.org/project/datapond](https://pypi.org/project/datapond/)
- **Website:** [datapond-db.github.io/website](https://datapond-db.github.io/website)
- **GitHub org:** [github.com/datapond-db](https://github.com/datapond-db)

## License

This registry is licensed under the MIT License. Individual databases have their own licenses as specified in the registry.
