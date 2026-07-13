# BSSIR - Basic Survey Structure for Iran

BSSIR is the shared Python infrastructure for Iran-Open-Data survey packages.
It provides the common logic for loading, cleaning, standardizing, documenting,
and enriching Iranian socioeconomic survey data.

Packages such as HBSIR, LFSIR, and CNSIR can depend on BSSIR for the processing
engine while keeping most of their own code focused on metadata: table
definitions, schemas, raw-file mappings, classifications, and package-specific
configuration.

## Why BSSIR exists

Iranian survey datasets often change across years. Raw files may use different
formats, encodings, column names, table names, value codes, and classification
systems. Without a shared base layer, every survey package would need to solve
the same problems repeatedly.

BSSIR centralizes that repeated work:

- configuration loading
- metadata loading and merging
- year and table availability parsing
- raw and cleaned table loading
- schema-driven standardization
- external-data loading
- ID, geography, occupation, industry, and commodity decoding
- documentation/rendering helpers
- utilities shared by downstream `*SIR` packages

## The `*SIR` ecosystem

BSSIR is intended to be the base package in a family of survey packages:

- **BSSIR**: shared logic, metadata system, cleaning/loading infrastructure
- **HBSIR**: Household Budget Survey package built on BSSIR
- **LFSIR**: Labor Force Survey package built on BSSIR
- **CNSIR**: Census package built on BSSIR

The downstream packages should be able to stay relatively small. Ideally, they
define metadata and package-specific rules, then let BSSIR do the common work.

## Installation

For normal development from this repository:

```powershell
uv sync --extra dev
```

Or install the package in editable mode with pip:

```powershell
pip install -e .
```

BSSIR requires Python 3.10 or newer.

## Basic Usage

Create a context for the active package:

```python
from bssir.context import load_context
from bssir.api import API

context = load_context()
api = API(context)
```

Load a table:

```python
table = api.load_table("Weight", years=1400)
```

Load an external table:

```python
cpi = api.load_external_table("cpi", data_source="SCI", frequency="Annual")
```

Add derived metadata to a table:

```python
table = api.add_attribute(table)
table = api.add_classification(table)
table = api.add_weight(table)
```

## Configuration And Metadata

BSSIR builds runtime context from three layers:

1. **Base package metadata** shipped with BSSIR
2. **Package metadata** shipped with a downstream package such as HBSIR
3. **Local metadata** supplied by a project or user

This lets downstream packages override or extend BSSIR defaults without copying
the engine.

Important metadata files include:

- `metadata/source_tables.yaml`: table list, table descriptions, availability, settings
- `metadata/schema.yaml`: column definitions and schema information
- `metadata/resources.yaml`: raw file mappings and patterns
- `metadata/id_schema.yaml`: ID structure and geographic attributes
- `metadata/maps.yaml`: map/geographic metadata
- `metadata/commodities.yaml`: commodity classifications
- `metadata/industries.yaml`: industry classifications
- `metadata/occupations.yaml`: occupation classifications

## Repository Layout

```text
src/bssir/
  api.py                  Public API wrapper around the runtime context
  context/                Configuration and metadata loading
  data_engine.py          Cleaned/normalized table handling
  data_cleaner.py         Raw table cleaning helpers
  decoder.py              Attribute and classification decoding
  external_data/          External-data readers and cleaners
  metadata/               Base BSSIR metadata
  rendering/              HTML/static rendering helpers
  utils/                  Shared utilities

tests/
  test_*.py               Unit tests for core logic
  package/                Fake downstream package used for contract tests
```

## Testing

Run the test suite:

```powershell
uv run --extra dev pytest
```

If your environment has restricted temp/cache folders, point pytest to writable
folders inside the repository:

```powershell
uv run --extra dev pytest -q --basetemp .pytest-tmp -o cache_dir=.pytest-cache-test
```

The current tests cover two important levels:

- **Unit tests** for core logic such as year parsing, metadata resolution, and
  helper utilities.
- **Contract tests** using `tests/package`, a minimal fake downstream package.
  These tests check that BSSIR can load metadata from another package shaped like
  HBSIR/LFSIR/CNSIR.

The contract tests are especially important because BSSIR behaves like a
framework. A change is not safe just because BSSIR's own metadata still loads;
it should also preserve the downstream package contract.

## Testing Downstream Packages

Before releasing a BSSIR change, test BSSIR itself:

```powershell
uv run --extra dev pytest
```

Then test downstream packages against the local BSSIR checkout. For example, in
an HBSIR checkout:

```powershell
pip install -e ../BSSIR
pytest
```

This catches integration problems that only appear when real downstream metadata
is used.

## Writing Good Tests For BSSIR

Prefer small tests with small fixtures.

Good BSSIR tests usually fit one of these categories:

- **Pure logic tests**: no real files, no network, no full survey data
- **Metadata tests**: tiny YAML examples that prove merging/resolution behavior
- **Contract tests**: a fake downstream package with minimal config and metadata
- **Small fixture tests**: 2-5 rows of fake raw data for cleaning/loading behavior
- **Golden output tests**: expected output files for stable, high-value transforms

Avoid tests that require large private datasets unless they are explicitly marked
as integration or maintenance tests.

## License

See `LICENCE`.
