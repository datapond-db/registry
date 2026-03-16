# Contributing to datapond

Thank you for your interest in adding a database to the datapond registry. This guide explains what makes a good datapond database, the technical requirements, and the submission process.

## What makes a good datapond database

datapond databases share a few properties:

- **Public data** -- the source data is freely available from a government agency, research institution, or FOIA release
- **Messy source files** -- the raw data is distributed as bulk downloads, fixed-width text, nested CSVs, XML dumps, or other formats that require significant cleaning to be useful
- **Broad research value** -- the data is useful to journalists, researchers, analysts, or the public
- **Large enough to benefit from DuckDB** -- typically millions of rows across multiple related tables

The goal is to take data that is technically public but practically inaccessible and make it instantly queryable.

## Requirements

Every datapond database must meet these requirements:

### 1. Reproducible build

Your GitHub repository must contain a build script (Python, R, shell, or similar) that:

- Downloads the raw source data
- Cleans, transforms, and loads it into DuckDB
- Produces a single `.duckdb` file as output
- Can be re-run to incorporate updates

### 2. DuckDB output

The final artifact is a single `.duckdb` file containing:

- All data tables with appropriate types and constraints
- A `_metadata` table with:
  - `database_id` -- matches the registry ID
  - `database_name` -- human-readable name
  - `source` -- original data source
  - `source_url` -- URL to the source
  - `build_date` -- when this file was built
  - `row_count` -- total rows across all tables
  - `table_count` -- number of data tables
  - `license` -- data license
  - `version` -- build version string
- Clear, descriptive table and column names

### 3. Hosted on Hugging Face

Upload the `.duckdb` file to a Hugging Face dataset repository. This enables remote attach via DuckDB's httpfs extension, local download via `huggingface_hub`, and version tracking.

**Step-by-step upload instructions:**

First, install the Hugging Face CLI and log in:

```bash
pip install huggingface_hub
huggingface-cli login
```

Create a new dataset repository on Hugging Face:

```bash
huggingface-cli repo create your-dataset-name --type dataset
```

Clone the empty repo, copy your `.duckdb` file into it, and push:

```bash
git clone https://huggingface.co/datasets/YOUR_USER/your-dataset-name
cd your-dataset-name
cp /path/to/your-db.duckdb .
git lfs install
git lfs track "*.duckdb"
git add .gitattributes your-db.duckdb
git commit -m "Add database file"
git push
```

Alternatively, upload directly without cloning:

```bash
huggingface-cli upload YOUR_USER/your-dataset-name ./your-db.duckdb your-db.duckdb --repo-type dataset
```

After uploading, verify the file is accessible by constructing the direct URL:

```
https://huggingface.co/datasets/YOUR_USER/your-dataset-name/resolve/main/your-db.duckdb
```

This URL becomes the `attach_url` in your registry entry.

### 4. Documentation

Your GitHub repository must include a README with:

- Description of the data and its source
- Table schema documentation (table names, column descriptions)
- Example queries
- Build instructions
- Data license and citation information

## Submission process

1. **Build your database** following the requirements above
2. **Upload to Hugging Face** using the instructions in the "Hosted on Hugging Face" section above
3. **Verify remote attach works** by running these commands in DuckDB:
   ```sql
   INSTALL httpfs;
   LOAD httpfs;
   ATTACH 'https://huggingface.co/datasets/YOUR_USER/YOUR_DB/resolve/main/YOUR_DB.duckdb' AS db (READ_ONLY);
   SELECT * FROM db.information_schema.tables;
   SELECT * FROM db._metadata;
   ```
   Or verify with the Python client:
   ```python
   import duckdb
   con = duckdb.connect()
   con.install_extension("httpfs")
   con.load_extension("httpfs")
   con.execute("ATTACH 'https://huggingface.co/datasets/YOUR_USER/YOUR_DB/resolve/main/YOUR_DB.duckdb' AS db (READ_ONLY)")
   con.sql("SELECT * FROM db.information_schema.tables").show()
   ```
4. **Open a pull request** on this repository adding an entry to `registry.json`
5. **Fill out the validation checklist** in your PR description (see below)

### Registry entry format

Add your database to the `databases` array in `registry.json`:

```json
{
  "id": "your-db-id",
  "name": "Your Database Name",
  "description": "Brief description of what the database contains",
  "rows": 50000000,
  "tables": 12,
  "size_gb": 3.5,
  "source": "Source Agency or Organization",
  "source_url": "https://source-agency.gov/data",
  "github": "https://github.com/your-user/your-repo",
  "huggingface": "https://huggingface.co/datasets/your-user/your-dataset",
  "attach_url": "https://huggingface.co/datasets/your-user/your-dataset/resolve/main/your-db.duckdb",
  "maintainer": "Your Name",
  "license": "Public domain",
  "updated": "2026-01-15"
}
```

## Validation checklist

Include this checklist in your PR description:

- [ ] `.duckdb` file loads without errors
- [ ] Remote attach via httpfs works from the `attach_url`
- [ ] `_metadata` table exists with all required fields
- [ ] All tables have descriptive column names (no `col1`, `field_a`)
- [ ] Row count in registry matches actual data
- [ ] Build script is included in the GitHub repo and runs end-to-end
- [ ] README includes table documentation and example queries
- [ ] License is specified and accurate
- [ ] Data is sourced from a public, freely available source

## Suggested datasets

The following public datasets would make great additions to the registry. If you're looking for a project, consider building one of these:

- **SEC EDGAR** -- corporate filings, financial statements, insider trading
- **CMS Medicare** -- provider utilization, payment data, drug spending
- **Census ACS** -- American Community Survey microdata and summary tables
- **BLS** -- Bureau of Labor Statistics employment, wages, prices
- **EPA** -- emissions, water quality, facility compliance, Superfund sites
- **USDA** -- crop production, food safety, agricultural census
- **NIH ClinicalTrials** -- clinical trial registry with protocols, results, and sites
- **DOT** -- aviation, highway safety, pipeline incidents, railroad data
- **NOAA** -- weather observations, climate data, severe storm events
- **USPTO** -- patent grants, trademark registrations, assignment records
- **FDIC** -- bank financial data, branch locations, failures
- **HUD** -- housing, fair market rents, public housing authorities
- **NHTSA** -- vehicle recalls, complaints, crash data
- **FBI UCR** -- Uniform Crime Reporting data

## Questions?

Open an issue on this repository or start a discussion. We're happy to help you scope and build a new database.
