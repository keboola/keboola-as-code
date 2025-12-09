# Keboola Project AI Guide

## 🎯 Quick Start for AI Agents

This is a Keboola data platform project containing transformations, tables, and data pipelines. This guide helps AI systems understand the project structure and provide better assistance.

## 📊 Project Overview

- **Project ID:** 1255
- **Project Name:** Playground
- **Region:** eu-west-1 (Europe West)
- **Backend:** Snowflake (primary data warehouse)
- **Total Tables:** 458
- **Total Transformations:** 307
- **Data Sources:** 10 (Shopify, HubSpot, Google Analytics, etc.)

## 🏗️ Project Structure

### Core Files (Start Here)
1. **manifest-extended.json** - Complete project overview in one file
   - Statistics (tables, transformations, sources)
   - Source registry
   - Platform breakdown

2. **buckets/index.json** - All storage buckets and tables
   - 135 buckets organized by source
   - Table counts and metadata

3. **transformations/index.json** - All data transformations
   - 307 transformations
   - Grouped by platform (Snowflake, Python, dbt, etc.)

4. **indices/graph.jsonl** - Complete data lineage
   - 741 edges showing data flow
   - From sources → tables → transformations → outputs

### Data Directories

```
├── buckets/                    # Storage layer
│   ├── index.json             # Bucket catalog
│   └── {bucket}/tables/{table}/
│       └── metadata.json      # Table structure, columns, types
│
├── transformations/            # Transformation layer
│   ├── index.json             # Transformation catalog
│   └── {transform}/
│       └── metadata.json      # SQL/Python code, dependencies
│
├── storage/samples/            # Data samples
│   ├── index.json             # Sample catalog
│   └── {bucket}/{table}/
│       ├── sample.csv         # First 100 rows
│       └── metadata.json      # Sample metadata
│
├── jobs/                       # Execution history
│   ├── index.json             # Job statistics
│   ├── recent/                # Last 100 jobs
│   └── by-component/          # Latest per component
│
└── indices/                    # Project-wide indices
    ├── graph.jsonl            # Lineage graph
    ├── sources.json           # Data sources
    └── queries/               # Pre-computed queries
```

## 🔍 How to Analyze This Project

### 1. Understanding the Data Flow

```
Sources → Buckets → Transformations → Output Tables
   ↓         ↓            ↓              ↓
indices/  buckets/  transformations/  buckets/
sources.json  (in)                     (out)
```

**Example Query Path:**
1. Check `indices/sources.json` - Find "shopify" source
2. Check `indices/queries/tables-by-source.json` - Find all Shopify tables
3. Check `indices/graph.jsonl` - Find transformations consuming those tables
4. Check `transformations/{name}/metadata.json` - See transformation logic

### 2. Tracing Table Lineage

To find where data comes from and where it goes:

```bash
# 1. Find table in buckets/index.json
# 2. Check table metadata: buckets/{bucket}/tables/{table}/metadata.json
# 3. Look at dependencies.consumed_by and dependencies.produced_by
# 4. Or search indices/graph.jsonl for the table UID
```

### 3. Understanding Transformations

Transformations process data using different platforms:
- **Snowflake SQL** (224 transformations) - Most common
- **Python** (61 transformations) - Complex logic
- **dbt** (9 transformations) - Modern data modeling
- **Others:** R, DuckDB, Redshift, Synapse

Check `transformations/index.json` for the full list grouped by platform.

### 4. Analyzing Job Executions

```
jobs/
├── recent/{job_id}.json       # Individual job details
└── by-component/{id}/latest.json  # Latest status per component
```

Each job includes:
- Status (success/error/cancelled)
- Duration
- Input/output tables
- Metrics (bytes processed)
- Error messages (if failed)

## 🔗 Keboola API Quick Reference

### Base URL
```
https://connection.north-europe.azure.keboola.com
```

### Authentication
```bash
X-StorageApi-Token: {token}
```

### Common Endpoints

#### Get Project Info
```http
GET /v2/storage/tokens/verify
```

#### List Tables
```http
GET /v2/storage/buckets/{bucket_id}/tables?include=columns,metadata
```

#### Get Table Preview
```http
GET /v2/storage/tables/{table_id}/data-preview?limit=100
```

#### Get Recent Jobs
```http
GET /v2/storage/jobs?limit=50
```

#### Get Components
```http
GET /v2/storage?exclude=componentDetails
```

### Documentation
- **API Docs:** https://keboola.docs.apiary.io/
- **Developer Docs:** https://developers.keboola.com/
- **User Docs:** https://help.keboola.com/

## 🤖 Best Practices for AI Analysis

### Do's ✅
1. **Start with manifest-extended.json** for project overview
2. **Use pre-computed queries** in `indices/queries/` for common questions
3. **Check platform field** in transformations for SQL syntax
4. **Use graph.jsonl** to understand data dependencies
5. **Check job history** before suggesting changes
6. **Respect sample data** - it may be stale (check metadata.sample_date)

### Don'ts ❌
1. **Don't assume live data** - samples may be days old
2. **Don't mix SQL dialects** - Snowflake ≠ BigQuery ≠ Redshift
3. **Don't ignore dependencies** - check graph before suggesting table changes
4. **Don't skip job history** - failed jobs indicate problems
5. **Don't expose secrets** - configurations may contain encrypted tokens

## 🔧 Platform-Specific Notes

### Snowflake (Primary Backend)
- Standard SQL dialect
- Case-insensitive identifiers
- Supports clustering, partitioning
- Warehouse sizes: XS, S, M, L, XL

### Python Transformations
- Python 3.9+
- Pandas, NumPy available
- Input/output via CSV files
- Check dependencies in metadata

### dbt Transformations
- Modern data modeling
- Jinja templating
- Incremental models
- Tests and documentation

## 🚨 Common Issues & Solutions

### Issue: Transformation fails with "table not found"
**Solution:** Check `indices/graph.jsonl` to ensure upstream tables exist and are being produced by previous transformations.

### Issue: Data seems outdated
**Solution:** Check `jobs/by-component/` to see when the component last ran successfully. Check for recent errors.

### Issue: Different platforms need different SQL
**Solution:** Check `platform` field in transformation metadata. Use platform-specific syntax (e.g., Snowflake's `||` for concatenation vs BigQuery's `CONCAT()`).

### Issue: Can't find a table
**Solution:**
1. Check `buckets/index.json` for all buckets
2. Use `indices/queries/tables-by-source.json` to filter by source
3. Search `indices/graph.jsonl` for partial table names

## 📝 Analysis Examples

### Example 1: Find All Shopify Tables
```bash
# Read indices/queries/tables-by-source.json
# Look for "shopify" key
# Returns: 78 Shopify tables with UIDs
```

### Example 2: Trace Orders Table
```bash
# 1. Find: buckets/raw/tables/orders/metadata.json
# 2. Check: metadata.dependencies.consumed_by
# 3. Result: ["transform:orders_clean"]
# 4. Find: transformations/orders_clean/metadata.json
# 5. See: SQL logic, output tables
```

### Example 3: Check Pipeline Health
```bash
# 1. Read: jobs/index.json (overall stats)
# 2. Check: jobs/recent/ (last 100 jobs)
# 3. Filter: status == "error"
# 4. Analyze: error messages, affected components
```

## 🔐 Security & Privacy

- **Secrets:** Encrypted values marked as `***ENCRYPTED***`
- **Samples:** Disabled if repository is public
- **PII:** Sensitive columns may be excluded from samples
- **Tokens:** Never stored in plaintext

## 📚 Additional Resources

### MCP Integration
This project can be queried via Keboola MCP server for real-time data:
- Server: `@keboola/mcp-server-storage`
- Capabilities: Table queries, job monitoring, lineage tracing

### Component Documentation
Each component has specific documentation:
- Transformations: SQL/Python code with comments
- Extractors: Source connection details
- Writers: Destination configuration

### Version History
Git commits show:
- Configuration changes over time
- Who made changes (creator token)
- What changed (diff in commit)

---

## 🎯 Quick Commands for AI Agents

### Get Project Statistics
```bash
cat manifest-extended.json | jq '.statistics'
```

### Find Snowflake Transformations
```bash
cat indices/queries/transformations-by-platform.json | jq '.snowflake | length'
```

### List All Sources
```bash
cat indices/sources.json | jq '.sources[].name'
```

### Find Most Connected Tables
```bash
cat indices/queries/most-connected-nodes.json | jq '.nodes[0:10]'
```

### Check Recent Job Failures
```bash
cat jobs/index.json | jq '.by_status.error'
```

---

**Need Help?** Check the API documentation or review the specific component's metadata.json file for detailed configuration.
