# LLM Agent Instructions for Keboola Projects

> Comprehensive guide for AI assistants working with Keboola project exports. This document provides workflows, API patterns, and best practices for effective project analysis and management.

## Quick Start

### First 3 Files to Read

When starting work on a Keboola project, read these files in order:

1. **`.keboola/manifest.json`** - Project ID, API host, configuration
2. **`buckets/index.json`** - All data buckets and tables catalog
3. **`jobs/index.json`** - Job execution statistics and health overview

```
# Quick health check sequence:
1. manifest.json    -> Get project ID and API endpoint
2. buckets/index.json -> Understand data scope (X buckets, Y tables)
3. jobs/index.json    -> Check success rate (success vs errors)
```

### Understanding Project Scope

After reading the 3 key files, you should know:
- Project ID and API host for live queries
- Total buckets and tables
- Data sources (input vs transformed)
- Job success/failure rates
- Whether transformations are captured

---

## Project Structure

### Directory Layout

```
project-root/
├── .keboola/
│   └── manifest.json           # Project configuration (if CLI project)
├── .env.local                  # (Optional) API token, created separately
├── .env.dist                   # (Optional) Token template, created separately
│
├── buckets/
│   ├── index.json              # All buckets catalog
│   └── {bucket-name}/
│       └── tables/
│           └── {table-name}/
│               └── metadata.json
│
├── transformations/
│   └── index.json              # All transformations (may be empty)
│
├── jobs/
│   ├── index.json              # Job statistics summary
│   ├── recent/
│   │   └── {job-id}.json       # Individual job details
│   └── by-component/
│       └── {component-id}/
│           └── {config-id}/
│               └── latest.json
│
├── samples/                    # (Optional) Present only when --samples flag is used
│   ├── index.json              # Sample data index
│   └── {table-id}/
│       ├── metadata.json       # Column list
│       └── sample.csv          # Actual data sample
│
├── indices/
│   ├── graph.jsonl             # Data lineage graph
│   ├── sources.json            # Data sources summary
│   └── queries/
│       ├── tables-by-source.json
│       ├── transformations-by-platform.json
│       └── most-connected-nodes.json
│
└── ai/
    ├── README.md               # Basic AI guide
    └── AGENT_INSTRUCTIONS.md   # This file
```

### File Naming Conventions

| Pattern | Example | Purpose |
|---------|---------|---------|
| `index.json` | `buckets/index.json` | Catalog of all items in directory |
| `metadata.json` | `tables/tickets/metadata.json` | Details for single item |
| `latest.json` | `by-component/.../latest.json` | Most recent job for config |
| `{id}.json` | `jobs/recent/35216272.json` | Item by unique ID |

### Index Files vs Detail Files

**Index files** provide quick lookups:
- Fast scanning without reading many files
- Summary statistics
- Lists of items with basic info

**Detail files** provide full information:
- Complete metadata for single item
- Dependencies and relationships
- All available fields

---

## Key Files Reference

| File | Purpose | Key Fields |
|------|---------|------------|
| `.keboola/manifest.json` | Project config | `project.id`, `project.apiHost` |
| `.env.local` | API authentication | `KBC_STORAGE_API_TOKEN` |
| `buckets/index.json` | Data catalog | `buckets[]`, `total_buckets`, `by_source` |
| `transformations/index.json` | Transformation catalog | `transformations[]`, `by_platform` |
| `jobs/index.json` | Job statistics | `by_status`, `total_jobs` |
| `indices/graph.jsonl` | Data lineage | Edges between tables/transformations |
| `indices/sources.json` | Source summary | Input vs transformed data |

### JSON Documentation Fields

All JSON files include self-documenting fields:

```json
{
  "_comment": "How this data was generated",
  "_purpose": "Why this file exists",
  "_update_frequency": "When to regenerate"
}
```

---

## Common Tasks

### Task 1: Understanding the Data Model

**Goal**: Learn what data exists and how it's organized

**Steps**:
1. Read `buckets/index.json` for overview
2. Identify input buckets (`source: "unknown"`) vs output buckets (`source: "transformed"`)
3. For each interesting table, read `buckets/{bucket}/tables/{table}/metadata.json`
4. Check `samples/{table-id}/sample.csv` to see actual data

**Example workflow**:
```
buckets/index.json
  -> Find "sales" bucket with 3 tables
  -> Read buckets/sales/tables/orders/metadata.json
  -> Check samples/in.c-sales.orders/sample.csv
  -> Understand: customer order data with timestamps
```

---

### Task 2: Analyzing Errors

**Goal**: Identify and diagnose job failures

**Steps**:
1. Read `jobs/index.json` for status breakdown
2. Calculate error rate: `errors / total_jobs`
3. Read individual error jobs from `jobs/recent/`
4. Group errors by `component_id` to find problematic components
5. Group by `config_id` to find specific failing configurations
6. Analyze `error_message` patterns

**Python analysis pattern**:
```python
import json
import os
from collections import Counter

# Load all jobs
jobs = []
for f in os.listdir('jobs/recent'):
    with open(f'jobs/recent/{f}') as file:
        jobs.append(json.load(file))

# Filter errors
errors = [j for j in jobs if j['status'] == 'error']

# Group by component
by_component = Counter(j['component_id'] for j in errors)
print("Errors by component:", by_component.most_common())

# Get error messages for top component
top_component = by_component.most_common(1)[0][0]
for job in errors:
    if job['component_id'] == top_component:
        print(f"Job {job['id']}: {job['error_message'][:200]}")
```

---

### Task 3: Tracing Data Lineage

**Goal**: Understand how data flows through the project

**Steps**:
1. Read `indices/graph.jsonl` for lineage edges
2. Check `dependencies` in table metadata files
3. Look for `consumed_by` (who uses this table)
4. Look for `produced_by` (where this table comes from)

**Note**: If lineage is empty (0 edges), data flow must be inferred from:
- Job history (which components ran)
- Transformation configurations (if available)
- Table naming patterns (e.g., `out.c-*` = output tables)

---

### Task 4: Connecting to Keboola API

**Goal**: Query live data from Keboola

#### Connection Information

Your Keboola connection credentials are stored in two files:

| Information | File | Key/Field |
|-------------|------|-----------|
| API Host | `.keboola/manifest.json` | `project.apiHost` |
| Project ID | `.keboola/manifest.json` | `project.id` |
| API Token | `.env.local` | `KBC_STORAGE_API_TOKEN` |

**Example `.keboola/manifest.json`**:
```json
{
  "version": 2,
  "project": {
    "id": 12345,
    "apiHost": "connection.us-east4.gcp.keboola.com"
  },
  "allowTargetEnv": true,
  "branches": [...]
}
```

**Example `.env.local`**:
```
KBC_STORAGE_API_TOKEN="1234-567890-abcdef..."
```

> **Note**: The `.env.local` file contains sensitive credentials and should never be committed to version control. A template is provided in `.env.dist`.

#### Authentication

```bash
# Get token from .env.local
TOKEN=$(grep KBC_STORAGE_API_TOKEN .env.local | cut -d'"' -f2)

# Get API host from manifest
HOST=$(python3 -c "import json; print(json.load(open('.keboola/manifest.json'))['project']['apiHost'])")
```

**Common API calls**:

```bash
# List all buckets
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "https://$HOST/v2/storage/buckets"

# Get table details with columns
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "https://$HOST/v2/storage/tables/{table_id}?include=columns,metadata"

# Get transformation configuration
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "https://$HOST/v2/storage/components/{component_id}/configs/{config_id}"

# Search jobs by status
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "https://queue.{region}.{cloud}.keboola.com/search/jobs?status=error&limit=50"

# Check running jobs
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "https://queue.{region}.{cloud}.keboola.com/search/jobs?status=processing"
```

**When to use API vs local files**:

| Use Local Files | Use API |
|-----------------|---------|
| Understanding project structure | Getting current job status |
| Historical job analysis | Fetching transformation code |
| Reading sample data | Checking running jobs |
| Offline analysis | Making changes |

---

## API Quick Reference

### Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v2/storage/buckets` | GET | List all buckets |
| `/v2/storage/buckets/{id}` | GET | Bucket details |
| `/v2/storage/tables/{id}` | GET | Table metadata |
| `/v2/storage/components` | GET | List components with configs |
| `/v2/storage/components/{id}/configs/{id}` | GET | Configuration details |
| `/search/jobs` | GET | Search job history (Queue API) |

### Queue API Host Pattern

The Queue API uses a different host pattern:
```
Storage API: connection.{region}.{cloud}.keboola.com
Queue API:   queue.{region}.{cloud}.keboola.com

Example:
Storage: connection.us-east4.gcp.keboola.com
Queue:   queue.us-east4.gcp.keboola.com
```

### Common Query Parameters

```bash
# Jobs search
?status=error           # Filter by status (error, success, processing, waiting)
&limit=100              # Limit results
&component=keboola.snowflake-transformation  # Filter by component

# Table details
?include=columns,metadata  # Include additional data
```

---

## MCP Servers for Keboola

For AI agents with MCP (Model Context Protocol) support, Keboola provides two MCP servers:

| Server | Purpose | Repository |
|--------|---------|------------|
| **Keboola MCP Server** | Direct interaction with Keboola projects | https://github.com/keboola/mcp-server |
| **API Documentation Server** | Look up API endpoint documentation | https://github.com/keboola/keboola-api-documentation-mcp-server |

---

### Keboola MCP Server (Project Interaction)

Connect your AI agents directly to Keboola. Query data, create transformations, run jobs, and manage your project—no glue code required.

**Repository**: https://github.com/keboola/mcp-server

#### Features

- **Storage**: Query tables directly and manage table/bucket descriptions
- **Components**: Create, list, and inspect extractors, writers, data apps, and transformations
- **SQL**: Create SQL transformations with natural language
- **Jobs**: Run components and transformations, retrieve job execution details
- **Flows**: Build and manage workflow pipelines
- **Data Apps**: Create, deploy, and manage Streamlit Data Apps
- **Metadata**: Search, read, and update project documentation
- **Dev Branches**: Work safely in development branches outside of production

#### Quick Start: Remote MCP Server (Recommended)

The easiest way is through the hosted **Remote MCP Server** on every Keboola stack with OAuth authentication.

**Claude Code** - Install using:
```bash
claude mcp add --transport http keboola https://mcp.<YOUR_REGION>.keboola.com/mcp
```

| Region | Installation Command |
|--------|---------------------|
| US Virginia AWS | `claude mcp add --transport http keboola https://mcp.keboola.com/mcp` |
| US Virginia GCP | `claude mcp add --transport http keboola https://mcp.us-east4.gcp.keboola.com/mcp` |
| EU Frankfurt AWS | `claude mcp add --transport http keboola https://mcp.eu-central-1.keboola.com/mcp` |
| EU Ireland Azure | `claude mcp add --transport http keboola https://mcp.north-europe.azure.keboola.com/mcp` |
| EU Frankfurt GCP | `claude mcp add --transport http keboola https://mcp.europe-west3.gcp.keboola.com/mcp` |

**Claude Desktop / Cursor**: Navigate to your Keboola Project Settings → `MCP Server` tab and copy the server URL.

#### Local Setup (Alternative)

For local development or custom configurations:

```json
{
  "mcpServers": {
    "keboola": {
      "command": "uvx",
      "args": ["keboola_mcp_server"],
      "env": {
        "KBC_STORAGE_API_URL": "https://connection.YOUR_REGION.keboola.com",
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema"
      }
    }
  }
}
```

#### Example Usage

Once configured, ask your AI agent:
- "Query the top 10 rows from the orders table"
- "Create a SQL transformation to aggregate sales by region"
- "Run the daily ETL job and show me the results"
- "What tables are in the sales bucket?"

---

### API Documentation MCP Server

Look up Keboola API endpoint documentation without leaving your environment.

**Repository**: https://github.com/keboola/keboola-api-documentation-mcp-server

#### Available APIs (Indexed)

| API | Endpoints |
|-----|-----------|
| Storage API | 199 |
| Management API | 129 |
| Stream Service | 41 |
| Templates Service | 23 |
| Orchestrator API | 15 |
| Queue Service | Jobs search and management |

#### Installation

**Claude Desktop / Claude Code** (using uvx):
```json
{
  "mcpServers": {
    "keboola-docs": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/keboola/keboola-api-documentation-mcp-server", "keboola-docs-mcp"]
    }
  }
}
```

#### Available Tools

| Tool | Description |
|------|-------------|
| `list_apis()` | List all available Keboola APIs with endpoint counts |
| `search_endpoints(query, api_filter?, method_filter?)` | Search for endpoints by keyword |
| `get_endpoint_details(api_name, path, method)` | Get full endpoint documentation |
| `get_api_section(api_name, section_name?)` | Get all endpoints in a section |
| `get_request_example(api_name, path, method)` | Generate curl example |

#### Example Usage

Once configured, ask your AI agent:
- "Find endpoints for creating tables in Storage API"
- "Get details for POST /v2/storage/buckets"
- "Show me a curl example for listing jobs"

---

## Error Analysis Workflow

### Step-by-Step Process

```
┌─────────────────────────────────────────────────────────────┐
│ 1. READ jobs/index.json                                     │
│    -> Get total jobs, success/error counts                  │
│    -> Calculate error rate                                  │
└─────────────────────────────────────────────────────────────┘
                              |
┌─────────────────────────────────────────────────────────────┐
│ 2. ANALYZE jobs/recent/*.json                               │
│    -> Filter status == "error"                              │
│    -> Group by component_id                                 │
│    -> Identify top error source                             │
└─────────────────────────────────────────────────────────────┘
                              |
┌─────────────────────────────────────────────────────────────┐
│ 3. GROUP errors by config_id                                │
│    -> Find specific failing configurations                  │
│    -> Check if errors are recurring                         │
└─────────────────────────────────────────────────────────────┘
                              |
┌─────────────────────────────────────────────────────────────┐
│ 4. ANALYZE error_message patterns                           │
│    -> SQL errors: Look for syntax, invalid identifiers      │
│    -> Schema errors: Column mismatches                      │
│    -> Data errors: Type conversion failures                 │
└─────────────────────────────────────────────────────────────┘
                              |
┌─────────────────────────────────────────────────────────────┐
│ 5. FETCH transformation config (via API)                    │
│    -> Get actual SQL/code                                   │
│    -> Compare with error message                            │
│    -> Identify root cause                                   │
└─────────────────────────────────────────────────────────────┘
                              |
┌─────────────────────────────────────────────────────────────┐
│ 6. SUGGEST fix                                              │
│    -> Provide corrected code                                │
│    -> Explain the issue                                     │
│    -> Recommend prevention                                  │
└─────────────────────────────────────────────────────────────┘
```

### Common Error Patterns

| Error Type | Pattern | Common Fix |
|------------|---------|------------|
| Column mismatch | "Missing columns: X" | Align SQL output with table schema |
| Invalid identifier | "invalid identifier 'X'" | Check column exists, fix case sensitivity |
| SQL syntax | "syntax error line X" | Fix SQL syntax (missing comma, parenthesis) |
| Date parsing | "Can't parse '' as date" | Use TRY_TO_DATE, handle empty strings |
| Table not found | "does not exist" | Check table path, verify permissions |

---

## Best Practices

### For Understanding Projects

1. **Always start with index files** - They provide overview without reading many files
2. **Check samples early** - Actual data helps understand schema better than metadata
3. **Note the source field** - `unknown` = input data, `transformed` = derived data
4. **Look for naming patterns** - `in.c-*` = input, `out.c-*` = output

### For Analyzing Issues

1. **Quantify first** - Get error rates before diving into details
2. **Group errors** - Same config_id often means same root cause
3. **Read full error messages** - They often contain the exact SQL that failed
4. **Check recent vs old** - Recent errors may be regressions

### For API Interactions

1. **Cache responses** - Avoid repeated API calls for same data
2. **Use local files when possible** - Faster and works offline
3. **Check both Storage and Queue APIs** - Different data available
4. **Handle rate limits** - Add delays if making many requests

### For Providing Recommendations

1. **Be specific** - Include exact code changes, not just descriptions
2. **Explain the "why"** - Help users understand root cause
3. **Consider side effects** - Changes may affect downstream tables
4. **Suggest testing** - Recommend validation steps

---

## Troubleshooting

### Problem: Empty lineage graph

**Symptom**: `indices/graph.jsonl` shows 0 edges

**Cause**: Transformations not captured in export or no transformations exist

**Workaround**:
- Infer lineage from job history (component_id + config_id)
- Use API to fetch transformation configs
- Look at table naming patterns

### Problem: Cannot find transformation code

**Symptom**: `transformations/index.json` is empty

**Cause**: Export doesn't include transformation configurations

**Workaround**:
- Use API: `GET /v2/storage/components/{component}/configs/{config_id}`
- Check job error messages (often contain the failed SQL)

### Problem: API returns 401 Unauthorized

**Symptom**: `"error": "Access token must be set"`

**Cause**: Token not being passed correctly

**Fix**:
```bash
# Ensure token is read correctly
cat .env.local  # Check format: KBC_STORAGE_API_TOKEN="..."

# Use single quotes in curl to prevent shell expansion
curl -H 'X-StorageApi-Token: YOUR_TOKEN' ...
```

### Problem: Column metadata missing

**Symptom**: Table metadata has no column information

**Cause**: Column details not included in export

**Workaround**:
- Read sample CSV headers: `head -1 samples/{table}/sample.csv`
- Use API: `GET /v2/storage/tables/{id}?include=columns`

---

## Version History

| Date | Version | Changes |
|------|---------|---------|
| 2025-01-07 | 1.0 | Initial comprehensive guide |
