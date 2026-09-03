# LLM Export Command

<div class="alert alert-info" role="alert">
<strong>BETA:</strong> The LLM commands are currently in beta. Features and output format may change.
</div>

**Export project data to AI-optimized twin format directory structure.**

```
kbc llm export [flags]
```

The command must be run in a directory initialized with [kbc llm init](init.md).

## Description

The twin format is designed for AI assistants to understand and work with Keboola projects directly from Git repositories. The export includes:

- **Bucket and table metadata** with schema information
- **Transformation configurations** with platform detection
- **Component configurations** organized by type
- **Job execution history** and statistics
- **Lineage graph** showing data flow dependencies
- **Optional data samples** (controlled by flags)

The export creates output files containing JSON with inline documentation (`_comment`, `_purpose`, `_update_frequency` fields) to help AI assistants understand the data structure.

### Security Features

- **Public repository detection** - Automatically detects if the directory is a public Git repository
- **Sample export disabled by default** - Data samples must be explicitly enabled with `--with-samples`
- **Encrypted secrets** - Fields starting with `#` are encrypted in the output

## Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., "connection.keboola.com"

`-t, --storage-api-token <string>`
: Storage API token from your project

`-f, --force`
: Skip confirmation when directory contains existing files

`--with-samples`
: Include table data samples in the export

`--sample-limit <int>`
: Maximum number of rows per table sample (default: 100, max: 1000)

`--max-samples <int>`
: Maximum number of tables to sample (default: 50, max: 100)

[Global Options](../../commands.md#global-options)

## Output Structure

The export creates the following directory structure:

```
.
├── buckets/              # Bucket and table metadata
│   └── index.json
├── transformations/      # Transformation configurations
├── components/           # Component configurations by type
├── jobs/                 # Job execution history
│   ├── recent/
│   └── by-component/
├── indices/              # Query indices and lookups
│   └── queries/
├── ai/                   # AI assistant guides
├── samples/              # Table data samples (if --with-samples)
├── lineage.json          # Data flow dependencies
└── metadata.json         # Project metadata
```

## Examples

### Basic Export

```
➜ kbc llm export

[1/5] Getting default branch...
Using branch: Main (ID: 1234)
[2/5] Fetching project data from APIs...
Fetched: 5 buckets, 23 tables, 150 jobs
[3/5] Processing data (lineage, platforms, sources)...
Processed: 5 buckets, 23 tables, 8 transformations, 45 lineage edges
[4/5] Generating twin format output...
[5/5] Skipping samples (not requested)
Twin format exported to: /path/to/project
Export completed successfully.
```

### Export with Data Samples

```
➜ kbc llm export --with-samples --sample-limit 50 --max-samples 20

[1/5] Getting default branch...
Using branch: Main (ID: 1234)
[2/5] Fetching project data from APIs...
Fetched: 5 buckets, 23 tables, 150 jobs
[3/5] Processing data (lineage, platforms, sources)...
Processed: 5 buckets, 23 tables, 8 transformations, 45 lineage edges
[4/5] Generating twin format output...
[5/5] Fetching and generating table samples...
Twin format exported to: /path/to/project
Export completed successfully.
```

## Next Steps

- [LLM Init](init.md)
- [All Commands](../../commands.md)
