# Keboola CLI Developer-Friendly Structure Specification

**Version:** 1.0  
**Date:** January 2026  
**Status:** Draft

---

## 1. Executive Summary

This specification defines a developer-friendly file representation for Keboola transformations and orchestrations. The current CLI structure mirrors the UI's block/code hierarchy, which creates unnecessary complexity for local development and code review.

The proposed structure consolidates transformation code into single files (`.sql` or `.py`) with comment-based block/code markers, generates proper Python dependency files (`pyproject.toml`), and provides declarative YAML-based orchestration definitions.

**Migration:** This is the new default structure. Existing projects will be automatically migrated when users upgrade the CLI and run `kbc pull`. The old block/code structure will be overwritten.

### Key Changes

| Component | Current State | Proposed State |
|-----------|---------------|----------------|
| SQL Transformations | Multiple files in `blocks/*/codes/*/code.sql` | Single `transform.sql` with comment markers |
| Python Transformations | Multiple files in `blocks/*/codes/*/code.py` | Single `transform.py` with comment markers |
| Python Dependencies | Embedded in `config.json` | Standalone `pyproject.toml` |
| Python Local Dev | No local execution support | `data/` folder with Common Interface structure |
| Orchestrations | Nested `phases/*/tasks/*/task.json` | Single `pipeline.yml` |
| Configuration & Metadata | Separate `config.json`, `meta.json`, `description.md` | Single `_config.yml` with all metadata |

---

## 2. Problem Statement

### Current Issues

1. **Fragmented Code Structure**: Transformation code is split across multiple directories and files. The block/code hierarchy is a UI abstraction that doesn't reflect execution reality—all SQL/Python code within a transformation executes as a single unit.

2. **Poor Code Review Experience**: Changes to a single logical transformation create diffs across many files. Git history is cluttered with structural changes.

3. **Missing Standard Tooling Support**: No `pyproject.toml` for Python transformations. Cannot use standard Python tools (pytest, mypy, pip-tools) easily.

4. **Complex Orchestration Definition**: Orchestrations are defined as nested directories with JSON files. Difficult to review pipeline changes.

5. **No Local Execution Support**: Python transformations cannot be easily executed locally for development and testing.

### Goals

1. Single-file transformations with comment-based block/code markers
2. Standard Python packaging with `pyproject.toml`
3. Declarative YAML-based orchestrations
4. Local execution support for Python transformations via `data/` folder
5. Bidirectional sync between developer format and Keboola API format

---

## 3. Proposed File Structure

```
📂 [branch-name]/
┣ 🟦 meta.json
┣ 🟩 description.md
┃
┣ 📂 _shared/                              # Shared code (structure unchanged)
┃ ┗ 📂 [target-component]/
┃   ┗ 📂 codes/
┃     ┗ 📂 [code-name]/
┃       ┣ 🟫 code.[ext]
┃       ┣ 🟦 config.json
┃       ┣ 🟦 meta.json
┃       ┗ 🟩 description.md
┃
┣ 📂 transformation/
┃ ┣ 📂 keboola.snowflake-transformation/   # SQL transformations
┃ ┃ ┗ 📂 [transformation-name]/
┃ ┃   ┣ 🟫 transform.sql                   # Single SQL file (blocks/codes merged)
┃ ┃   ┣ 🟦 _config.yml                     # Config + metadata (name, description, tags, mappings)
┃ ┃   ┗ 📂 variables/                      # Optional
┃ ┃     ┗ 🟦 _variables.yml
┃ ┃
┃ ┗ 📂 keboola.python-transformation-v2/   # Python transformations
┃   ┗ 📂 [transformation-name]/
┃     ┣ 🟫 transform.py                    # Single Python file
┃     ┣ 🟦 pyproject.toml                  # Python dependencies
┃     ┣ 🟦 _config.yml                     # Config + metadata
┃     ┗ 📂 data/                           # Local execution data (git-ignored)
┃       ┣ 🟦 config.json                   # Generated from _config.yml by `kbc local data`
┃       ┣ 📂 in/
┃       ┃ ┣ 📂 tables/                     # Input tables (CSV files)
┃       ┃ ┃ ┣ 🟫 <table-name>.csv
┃       ┃ ┃ ┗ 🟫 <table-name>.csv.manifest
┃       ┃ ┗ 📂 files/                      # Input files
┃       ┃   ┣ 🟫 <file-id>_<file-name>
┃       ┃   ┗ 🟫 <file-id>_<file-name>.manifest
┃       ┗ 📂 out/
┃         ┣ 📂 tables/                     # Output tables (written by transformation)
┃         ┗ 📂 files/                      # Output files (written by transformation)
┃
┣ 📂 application/
┃ ┗ 📂 kds-team.app-custom-python/         # Custom Python applications
┃   ┗ 📂 [application-name]/
┃     ┣ 🟫 code.py                         # Application code
┃     ┣ 🟦 pyproject.toml                  # Python dependencies (from packages)
┃     ┣ 🟦 _config.yml                     # Config + metadata (name, description, mappings, user_properties)
┃     ┗ 📂 data/                           # Local execution data (git-ignored, fully generated)
┃       ┣ 🟦 config.json                   # Generated from _config.yml by `kbc local data`
┃       ┣ 📂 in/
┃       ┃ ┣ 📂 tables/
┃       ┃ ┗ 📂 files/
┃       ┗ 📂 out/
┃         ┣ 📂 tables/
┃         ┗ 📂 files/
┃
┣ 📂 flow/
┃ ┗ 📂 keboola.orchestrator/               # Orchestrations
┃   ┗ 📂 [orchestration-name]/
┃     ┗ 🟦 pipeline.yml                    # Single file: metadata, phases, tasks, schedules, variables
┃
┣ 📂 extractors/                           # Other components (unchanged)
┣ 📂 writers/
┣ 📂 apps/
┗ 📂 other/
```

### Directory Mapping

The parent directory structure remains unchanged. Only the internal structure of each configuration changes:

| Component Type | Path (unchanged) |
|----------------|------------------|
| SQL Transformations | `transformation/keboola.snowflake-transformation/[name]/` |
| Python Transformations | `transformation/keboola.python-transformation-v2/[name]/` |
| Custom Python Apps | `application/kds-team.app-custom-python/[name]/` |
| Orchestrations | `flow/keboola.orchestrator/[name]/` |

### File Naming Conventions

| File | Purpose |
|------|---------|
| `transform.sql` / `transform.py` | Transformation code (fixed names) |
| `code.py` | Custom Python application code |
| `_config.yml` | Configuration + metadata (name, description, tags, mappings, parameters) |
| `pyproject.toml` | Python dependencies |
| `pipeline.yml` | Complete orchestration: metadata, phases, tasks, schedules, variables |
| `_variables.yml` | Variable definitions (for transformations) |
| `data/` | Local execution data directory (git-ignored, fully generated) |

### Git Ignore Rules

The following should be added to `.gitignore` at the project root:

```gitignore
# Local execution data folders (prevent accidental data commits)
**/transformation/**/data/
**/application/**/data/
```

---

## 4. Transformation File Formats

### 4.1 SQL Transformation: `transform.sql`

The single SQL file contains all blocks and codes from the original structure, separated by comment markers. The format matches the existing UI implementation.

**Structure:**
```sql
/* ===== BLOCK: <block-name> ===== */

/* ===== CODE: <code-name> ===== */
<sql-statements>

/* ===== CODE: <another-code-name> ===== */
<sql-statements>

/* ===== BLOCK: <another-block-name> ===== */

/* ===== CODE: <code-name> ===== */
<sql-statements>
```

### 4.2 Python Transformation: `transform.py`

**Structure:**
```python
# ===== BLOCK: <block-name> =====

# ===== CODE: <code-name> =====
<python-code>

# ===== CODE: <another-code-name> =====
<python-code>

# ===== BLOCK: <another-block-name> =====

# ===== CODE: <code-name> =====
<python-code>
```

### 4.3 Conversion Logic Reference

The block/code ↔ single file conversion logic is already implemented in the Keboola UI repository:

**Location:** `apps/kbc-ui/src/scripts/modules/components/react/components/generic/code-blocks/helpers.js`

**Key Functions:**
| Function | Lines | Description |
|----------|-------|-------------|
| `getBlocksAsString()` | 170-216 | Converts blocks/codes structure to single file string |
| `prepareMultipleBlocks()` | 218-279 | Parses single file string back to blocks/codes structure |
| `getDelimiter()` | 97-125 | Returns comment delimiter based on component type |

The CLI implementation **MUST** use the same comment format and parsing logic as the UI to ensure consistency.

---

## 5. Python Dependency Management

### 5.1 `pyproject.toml` Structure

```toml
[project]
name = "<transformation-name-slugified>"
version = "1.0.0"
description = "<from-_config.yml>"
requires-python = ">=3.11"

dependencies = [
    # Explicit packages from transformation config.json parameters.packages
    "<package-name>>=<version>",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "mypy>=1.0.0",
]

[tool.keboola]
# Keboola-specific metadata for CLI sync
component_id = "<component-id>"
config_id = "<config-id>"
backend = "<backend-type>"

[tool.keboola.packages]
# Original package specifications from config.json
explicit = [
    { name = "<package>", version = "<version>" },
]
```

### 5.2 Package Sources

**Explicit packages:** Defined in transformation `config.json` under `parameters.packages`

**Default preinstalled packages:** Available in Keboola Python runtime. Reference the Dockerfile for the current list:
- https://github.com/keboola/docker-custom-python/blob/master/python-3.10/Dockerfile

The `pyproject.toml` should document default packages in comments or `[tool.keboola]` section for developer reference, but they don't need to be installed locally as they're available in the Keboola runtime.

### 5.3 CLI Conversion on Push

Users edit `pyproject.toml` to manage dependencies and `transform.py` for code. The CLI handles conversion back to the platform's config structure on `kbc push`:
- `pyproject.toml` dependencies → `config.json` `parameters.packages`
- `transform.py` content → block/code structure (using UI conversion logic)

---

## 6. Custom Python Applications (`kds-team.app-custom-python`)

### 6.1 Overview

Custom Python applications have a similar structure to Python transformations but include user-configurable parameters. The key difference is that user properties need to be available at runtime via `data/config.json`.

### 6.2 Current vs New Structure

**Current structure:**
```
application/kds-team.app-custom-python/[name]/
├── code.py
├── config.json      # Contains: storage, parameters.packages, parameters.user_properties, parameters.code
├── meta.json
└── description.md
```

**New structure:**
```
application/kds-team.app-custom-python/[name]/
├── code.py          # Application code (user editable)
├── pyproject.toml   # Dependencies from parameters.packages (user editable)
├── _config.yml      # Config + metadata (name, description, mappings, user_properties)
└── data/            # Git-ignored, fully generated by `kbc local data`
    ├── config.json  # Generated from _config.yml
    ├── in/
    │   ├── tables/
    │   └── files/
    └── out/
        ├── tables/
        └── files/
```

### 6.3 Configuration Mapping

| Original field | New location |
|----------------|--------------|
| `meta.json` → `name` | `_config.yml` → `name` |
| `description.md` | `_config.yml` → `description` |
| `meta.json` → `isDisabled` | `_config.yml` → `disabled` |
| `parameters.code` | `code.py` (file content) |
| `parameters.packages` | `pyproject.toml` dependencies |
| `parameters.user_properties` | `_config.yml` → `user_properties` |
| `storage.input/output` | `_config.yml` → `input/output` |
| `parameters.venv` | `_config.yml` → `parameters.venv` |
| `parameters.source` | `_config.yml` → `parameters.source` |

### 6.4 `_config.yml` for Applications

```yaml
version: 2

# Metadata (from meta.json + description.md)
name: customer-downloader
description: |
  Downloads customer data from Google Drive and loads it into Storage.

  Runs daily to sync the latest customer list.
disabled: false
tags:
  - import
  - customers

input:
  tables:
    - source: <bucket>.<table>
      destination: <filename>
  files:
    - tags: [<tag-list>]
      destination: <path>

output:
  tables:
    - source: <filename>
      destination: <bucket>.<table>
  files:
    - source: <pattern>
      tags: [<tag-list>]

parameters:
  venv: "3.13"
  source: code
  # Other non-user parameters

user_properties:
  # User-configurable parameters
  google_drive_url: "https://..."
  destination_bucket: "out.c-customers"
```

### 6.5 Generated `data/config.json`

The `kbc local data` command generates `data/config.json` from `_config.yml` in the format expected by CommonInterface:

```json
{
  "parameters": {
    "google_drive_url": "https://...",
    "destination_bucket": "out.c-customers"
  },
  "storage": {
    "input": {
      "tables": [...],
      "files": [...]
    },
    "output": {
      "tables": [...],
      "files": [...]
    }
  }
}
```

The conversion:
- `_config.yml` `user_properties` → `config.json` `parameters`
- `_config.yml` `input/output` → `config.json` `storage.input/output`

### 6.6 Editable Files

Users edit these files directly:
- `code.py` - Application logic
- `pyproject.toml` - Python dependencies
- `_config.yml` - Input/output mappings and user properties

The CLI handles conversion back to the platform's config structure on `kbc push`:
- `code.py` content → `parameters.code`
- `pyproject.toml` dependencies → `parameters.packages`
- `_config.yml` user_properties → `parameters.user_properties`
- `_config.yml` input/output → `storage.input/output`

---

## 7. Python Local Execution Data Folder

### 7.1 Purpose

The `data/` folder enables local execution of Python transformations by providing the same directory structure that Keboola uses at runtime. This follows the Keboola Common Interface specification.

**Reference:** https://developers.keboola.com/extend/common-interface/folders/

### 7.2 Structure

```
data/
├── config.json                   # Generated from _config.yml (for CommonInterface)
├── in/
│   ├── tables/                    # Input tables as CSV files
│   │   ├── <destination>.csv      # Table data (destination from _config.yml)
│   │   └── <destination>.csv.manifest
│   └── files/                     # Input files
│       ├── <file-id>_<file-name>
│       └── <file-id>_<file-name>.manifest
└── out/
    ├── tables/                    # Output tables (written by transformation)
    └── files/                     # Output files (written by transformation)
```

### 7.3 Git Ignore

The `data/` folder **MUST** be excluded from git to prevent accidental commits of actual data. The CLI should automatically add this rule to `.gitignore` when creating the project structure.

### 7.4 CLI Command: `kbc local data`

The CLI provides a command to populate the `data/` folder with actual data from Keboola Storage based on the input mappings. Works for both Python transformations and custom Python applications.

```bash
# Generate data folder for a transformation
kbc local data <transformation-path>

# Generate data folder for an application
kbc local data <application-path>

# With row limit for large tables
kbc local data <path> --limit=<rows>

# Examples
kbc local data transformation/keboola.python-transformation-v2/sales-forecast
kbc local data application/kds-team.app-custom-python/customer-downloader
kbc local data transformation/keboola.python-transformation-v2/customer-analytics --limit=1000
```

The command should:
1. Read the `_config.yml` to get input table/file mappings
2. Download the specified tables from Keboola Storage as CSV files
3. Download the specified files from Keboola File Storage
4. Generate manifest files for each table/file
5. Create the `out/tables/` and `out/files/` directories (empty)
6. Generate `data/config.json` from `_config.yml` for both Python transformations and applications

The `config.json` generation enables the `keboola.component` library (CommonInterface) to work correctly locally.

### 7.5 Best Practices for Local Development

**Recommended: Use CommonInterface**

The `keboola.component` library handles paths correctly regardless of working directory:

```python
from keboola.component import CommonInterface

ci = CommonInterface()

# Reading input tables - works locally and on platform
input_table = ci.get_input_table_definition_by_name('customers.csv')
df = pd.read_csv(input_table.full_path)

# Writing output tables - works locally and on platform
output_table = ci.create_out_table_definition('result.csv')
df.to_csv(output_table.full_path, index=False)
```

**Not Recommended: Hardcoded Paths**

Code with hardcoded paths only works on the Keboola platform:

```python
# This works on platform (runs from /data/) but breaks locally
with open('in/tables/customers.csv') as f:
    ...

with open('out/tables/result.csv', 'w') as f:
    ...
```

If you must use hardcoded paths, run your transformation from the `data/` directory:

```bash
cd transformation/keboola.python-transformation-v2/my-transform/data
python ../transform.py
```

**Reference:** https://github.com/keboola/python-component

---

## 8. Orchestration File Format

### 8.1 `pipeline.yml` Structure

The `pipeline.yml` is the **only file** for an orchestration. It contains everything: metadata, phases, tasks, schedules, and variables.

```yaml
version: 2

# Metadata
name: Daily Data Pipeline
description: |
  Runs daily to sync customer data from Salesforce,
  transform it with analytics, and load to BigQuery.
disabled: false
tags:
  - production
  - daily

# Schedules (inline, no separate file)
schedules:
  - name: Daily morning run
    cron: "0 6 * * *"
    timezone: Europe/Prague
    enabled: true
    variables:
      full_refresh: false

  - name: Weekly full refresh
    cron: "0 2 * * 0"
    timezone: Europe/Prague
    enabled: true
    variables:
      full_refresh: true

# Variables definition
variables:
  full_refresh:
    type: boolean
    default: false
    description: When true, reload all data instead of incremental

# Pipeline phases and tasks
phases:
  - name: Extract
    description: Load data from external sources
    parallel: true

    tasks:
      - name: Load customers
        component: keboola.ex-salesforce
        config: extractor/keboola.ex-salesforce/salesforce-customers
        enabled: true
        continue_on_failure: false

  - name: Transform
    depends_on:
      - Extract

    tasks:
      - name: Customer analytics
        component: keboola.snowflake-transformation
        config: transformation/keboola.snowflake-transformation/customer-analytics
        enabled: true

      - name: Process orders
        component: keboola.python-transformation-v2
        config: transformation/keboola.python-transformation-v2/order-processing
        enabled: true
        parameters:
          <key>: <value>

  - name: Load
    depends_on:
      - Transform

    tasks:
      - name: Write to BigQuery
        component: keboola.wr-google-bigquery
        config: writer/keboola.wr-google-bigquery/bigquery-output
        enabled: true

# Internal Keboola metadata (managed by CLI)
_keboola:
  component_id: <component-id>
  config_id: <config-id>
```

### 8.2 Task Config Field

The `config` field contains the relative path from the orchestrator directory to the target configuration:

```
<component-type>/<component-id>/<config-name>
```

**Examples:**
- `transformation/keboola.snowflake-transformation/customer-analytics`
- `application/kds-team.app-custom-python/customer-downloader`
- `extractor/keboola.ex-salesforce/salesforce-customers`
- `writer/keboola.wr-google-bigquery/bigquery-output`

**Notes:**
- Path is relative from the orchestrator directory
- CLI resolves the config path to find the referenced configuration
- The `component` field identifies the component type for the task

### 8.3 Conversion Reference

The conversion logic derives from the existing orchestration structure:
- `phases/*/phase.json` → `phases` section in `pipeline.yml`
- `phases/*/tasks/*/task.json` → `tasks` within each phase
- `schedules/*/config.json` → `schedules` section in `pipeline.yml` (inline)
- `meta.json` + `description.md` → `name`, `description`, `tags` in `pipeline.yml`

**Note:** The separate `schedules/` folder is eliminated. All schedules are defined inline in `pipeline.yml`.

---

## 9. Configuration File Formats

### 9.1 `_config.yml` - Unified Configuration

The `_config.yml` file contains both metadata (previously in `meta.json` + `description.md`) and configuration (previously in `config.json`).

**Full Example:**
```yaml
version: 2

# Metadata (from meta.json + description.md)
name: customer-analytics
description: |
  This transformation processes customer data and generates
  daily metrics for the analytics dashboard.

  ## Input Tables
  - customers: Raw customer data
  - orders: Order history

  ## Output
  Generates `daily_metrics` table with aggregated KPIs.
disabled: false
tags:
  - analytics
  - daily

# Configuration
runtime:
  backend:
    type: snowflake | synapse | bigquery | redshift | python
    context: transformation  # optional
  safe: true  # optional, and other runtime fields

input:
  tables:
    - source: in.c-customers.customers
      destination: customers.csv
      columns: [<column-list>]  # optional
      where_column: <column>     # optional
      where_operator: <op>       # optional
      where_values: [<values>]   # optional
      changed_since: <value>     # optional

  files:
    - tags: [<tag-list>]
      destination: <path>

output:
  tables:
    - source: daily_metrics.csv
      destination: out.c-analytics.daily_metrics
      primary_key: [<column-list>]
      incremental: true | false

  files:
    - source: <pattern>
      tags: [<tag-list>]

parameters:
  <key>: <value>

shared_code:
  - name: <shared-code-name>
    codes:
      - <code-name>

# Internal Keboola metadata (managed by CLI)
_keboola:
  component_id: <component-id>
  config_id: <config-id>
```

### 9.2 `_variables.yml` - Variables

```yaml
version: 2

definitions:
  - name: <variable-name>
    type: string | number | boolean
    description: <description>

values:
  default:
    name: Default Values
    values:
      <variable-name>: <value>
```

---

## 10. CLI Commands

### 10.1 Updated Commands

```bash
# Pull configurations (automatically uses new developer-friendly format)
kbc pull

# Push configurations (automatically converts back to API format)
kbc push

# Validate project structure
kbc local validate
```

### 10.2 New Command: `kbc local data`

Generate the `data/` folder for local Python transformation or application execution:

```bash
# Generate data folder for a transformation
kbc local data <transformation-path>

# Generate data folder for an application
kbc local data <application-path>

# With row limit for large tables
kbc local data <path> --limit=<rows>

# Examples
kbc local data transformation/keboola.python-transformation-v2/sales-forecast
kbc local data application/kds-team.app-custom-python/customer-downloader
kbc local data transformation/keboola.python-transformation-v2/customer-analytics --limit=1000
```

### 10.3 Configuration

Project configuration in `.keboola/project.json`:

```json
{
  "developer": {
    "generatePyproject": true,
    "useYamlConfig": true
  }
}
```

---

## 11. Component ID Reference

| Component ID | Type | Language |
|--------------|------|----------|
| `keboola.snowflake-transformation` | SQL | sql |
| `keboola.synapse-transformation` | SQL | sql |
| `keboola.redshift-transformation` | SQL | sql |
| `keboola.exasol-transformation` | SQL | sql |
| `keboola.python-transformation-v2` | Python | py |
| `keboola.python-mlflow` | Python | py |
| `keboola.r-transformation-v2` | R | r |
| `kds-team.app-custom-python` | Application | py |
| `keboola.orchestrator` | Orchestration | - |

---

## 12. Backward Compatibility

### 12.1 Overview

The new developer-friendly format is a breaking change in local file structure, but the CLI must handle the transition gracefully. Users should be able to upgrade without losing work or encountering errors.

### 12.2 Format Detection

The CLI should detect which format is present locally by checking for format indicators:

**Old format indicators:**
- `blocks/` subdirectory present in transformation configs
- `phases/` subdirectory present in orchestration configs
- `config.json` contains `parameters.code` field (for applications)
- Separate `config.json` and `meta.json` files

**New format indicators:**
- `transform.sql` or `transform.py` file present in transformation configs
- `pipeline.yml` file present in orchestration configs
- `_config.yml` file with `user_properties` section (for applications)
- `pyproject.toml` file present (for Python components)

### 12.3 Behavior on `kbc pull`

When user runs `kbc pull` with old format locally:

1. **Detect old format** - CLI detects the legacy structure
2. **Warn user** - Display message: "Legacy format detected. Converting to new developer-friendly format..."
3. **Backup old structure** - Optionally create `.keboola/backup/` with timestamp
4. **Remove old files** - Delete old files (`blocks/`, `phases/`, `config.json`, `meta.json`, `description.md`)
5. **Pull in new format** - Create new files (`transform.sql`, `pipeline.yml`, `_config.yml`, etc.)
6. **Report changes** - List converted configurations

### 12.4 Behavior on `kbc push`

When user runs `kbc push` with old format locally:

1. **Detect old format** - CLI detects the legacy structure
2. **Convert in memory** - Convert old format to API format (this already works)
3. **Push to platform** - Push works as before
4. **Suggest migration** - Display message: "Consider running `kbc pull` to migrate to the new developer-friendly format"

This ensures users can still push their work even before migrating locally.

### 12.5 Behavior on `kbc pull` with Mixed Format

If both old and new format indicators exist within the same configuration (partial migration or manual changes):

1. **Error with guidance** - CLI should error: "Mixed format detected. Found both legacy and new format files in the same configuration."
2. **Suggest resolution** - "Please remove legacy files (`blocks/`, `phases/`, old `config.json`) and run `kbc pull` again, or restore from backup."

### 12.6 Manifest Compatibility

The `.keboola/manifest.json` file tracks object IDs and paths. During migration:

1. **Update paths** - Manifest paths should be updated to reflect new structure
2. **Preserve IDs** - All Keboola object IDs must be preserved for sync to work
3. **Add format version** - Add `"formatVersion": 2` to manifest to track format

```json
{
  "version": 2,
  "formatVersion": 2,
  "project": { ... },
  "branches": [ ... ]
}
```

### 12.7 Migration Path Summary

| Scenario | CLI Behavior |
|----------|--------------|
| Old format + `kbc pull` | Migrate to new format (with warning) |
| Old format + `kbc push` | Push works, suggest migration |
| New format + `kbc pull` | Normal operation |
| New format + `kbc push` | Normal operation (convert to API format) |
| Mixed format + any command | Error with guidance |
| Fresh clone + `kbc pull` | New format (default) |

### 12.8 Rollback

If users encounter issues with the new format:

1. **Restore from backup** - If backup was created, restore from `.keboola/backup/`
2. **Use older CLI version** - Pin to previous CLI version temporarily
3. **Manual conversion** - The old format can be recreated by restructuring files manually

---

## 13. Migration Examples

### 13.1 SQL Transformation

**Before (Classic Format):**
```
transformation/keboola.snowflake-transformation/customer-analytics/
├── config.json
├── meta.json
├── description.md
└── blocks/
    ├── 001-data-preparation/
    │   ├── meta.json
    │   ├── 001-load-customers/
    │   │   ├── meta.json
    │   │   └── code.sql
    │   └── 002-deduplicate/
    │       ├── meta.json
    │       └── code.sql
    └── 002-aggregation/
        ├── meta.json
        └── 001-daily-metrics/
            ├── meta.json
            └── code.sql
```

**After (Developer Format):**
```
transformation/keboola.snowflake-transformation/customer-analytics/
├── transform.sql
└── _config.yml       # Contains name, description, tags, and all config
```

### 13.2 Custom Python Application

**Before (Classic Format):**
```
application/kds-team.app-custom-python/customer-downloader/
├── code.py
├── config.json    # Contains packages, user_properties, code, storage
├── meta.json
└── description.md
```

**After (Developer Format):**
```
application/kds-team.app-custom-python/customer-downloader/
├── code.py           # User editable
├── pyproject.toml    # User editable (dependencies)
└── _config.yml       # Name, description, tags, input/output mappings, user_properties
```

---

## Appendix: External References

| Resource | URL |
|----------|-----|
| Keboola CLI Structure Docs | https://developers.keboola.com/cli/structure/ |
| Keboola Common Interface Folders | https://developers.keboola.com/extend/common-interface/folders/ |
| Python Component Library | https://github.com/keboola/python-component |
| UI Block/Code Helpers | `apps/kbc-ui/src/scripts/modules/components/react/components/generic/code-blocks/helpers.js` |
| Python Runtime Dockerfile | https://github.com/keboola/docker-custom-python/blob/master/python-3.10/Dockerfile |
| dbt Project Structure | https://docs.getdbt.com/docs/build/projects |
| Dagster Project Structure | https://docs.dagster.io/guides/understanding-dagster-project-files |
