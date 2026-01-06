# AI Assistant Guide

This guide helps AI assistants understand and work with Keboola project exports.

## Quick Start

When starting work on a Keboola project, read these files in order:

1. **`manifest-extended.json`** - Complete project overview in one file
2. **`buckets/index.json`** - All data buckets and tables catalog
3. **`jobs/index.json`** - Job execution statistics and health overview

## Directory Structure

```
project-root/
├── .keboola/manifest.json     # Project configuration
├── .env.local                 # API token (KBC_STORAGE_API_TOKEN)
├── manifest.yaml              # Twin format manifest
├── manifest-extended.json     # Complete project overview (START HERE)
├── README.md                  # Project README
├── buckets/
│   ├── index.json             # All buckets catalog
│   └── {bucket}/tables/{table}/metadata.json
├── transformations/
│   ├── index.json             # All transformations
│   └── {name}/metadata.json
├── jobs/
│   ├── index.json             # Job statistics
│   ├── recent/*.json          # Recent job details
│   └── by-component/**/*.json # Jobs by component
├── indices/
│   ├── graph.jsonl            # Data lineage graph
│   ├── sources.json           # Data sources summary
│   └── queries/*.json         # Pre-computed queries
├── samples/                   # Table data samples (if exported)
│   └── {table-id}/sample.csv
└── ai/
    ├── README.md              # This file
    └── AGENT_INSTRUCTIONS.md  # Comprehensive agent guide
```

## How to Use This Data

### Understanding Data Flow

Use `indices/graph.jsonl` to understand how data flows through the project:

- `consumed_by` edges: Table -> Transformation (input)
- `produces` edges: Transformation -> Table (output)

### Finding Tables

Tables are organized by bucket in `buckets/{bucket}/tables/{table}/metadata.json`.

Each table metadata includes:
- Column names and types
- Row counts and data sizes
- Dependencies (consumed_by, produced_by)
- Source inference (Shopify, Salesforce, etc.)

### Finding Transformations

Transformations are in `transformations/{name}/metadata.json` with:
- Platform (Snowflake, Python, dbt, etc.)
- Input/output table dependencies
- Job execution status and history

### Checking Job Status

Recent job executions are in:
- `jobs/index.json` - Summary statistics
- `jobs/recent/` - Individual job details
- `jobs/by-component/` - Latest job per configuration

## JSON Documentation Fields

Every JSON file includes self-documenting fields:

```json
{
  "_comment": "How this data was generated",
  "_purpose": "Why this file exists",
  "_update_frequency": "When to regenerate"
}
```

## For More Information

See `AGENT_INSTRUCTIONS.md` for comprehensive workflows, API patterns, and best practices.
