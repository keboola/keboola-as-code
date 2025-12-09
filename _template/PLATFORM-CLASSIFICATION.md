# Platform Classification Guide

## Why Platform Classification Matters

Proper platform classification in transformation metadata is **critical** for:

1. **Cost Estimation** - Different platforms have different compute costs
2. **Performance Optimization** - SQL optimization varies by platform
3. **Maintenance** - Knowing what skills are needed to maintain transformations
4. **Resource Planning** - Understanding platform usage across the project
5. **Migration Planning** - Identifying dependencies on specific platforms

## ⚠️ Common Issue: Unknown Platforms

Transformations with `platform: "unknown"` create problems:
- Cannot estimate compute costs
- Cannot optimize SQL for specific engine
- Unclear what skills needed to maintain
- Poor visibility in analytics

## Supported Platforms

### SQL-Based Platforms
- `snowflake` - Snowflake Data Warehouse
- `redshift` - Amazon Redshift
- `bigquery` - Google BigQuery
- `synapse` - Azure Synapse Analytics
- `mssql` - Microsoft SQL Server
- `mysql` - MySQL
- `postgresql` - PostgreSQL
- `oracle` - Oracle Database
- `exasol` - Exasol
- `duckdb` - DuckDB
- `sql` - Generic SQL (when specific platform unknown)

### Programming Language Platforms
- `python` - Python transformations
- `r` - R transformations
- `julia` - Julia transformations

### Framework Platforms
- `dbt` - dbt (data build tool)
- `spark` - Apache Spark

## Metadata Requirements

Every transformation **MUST** have:

```json
{
  "uid": "transform:my_transformation",
  "name": "My Transformation",
  "type": "transformation",
  "platform": "snowflake",        // ← REQUIRED - never "unknown"
  "component_id": "keboola.snowflake-transformation",
  "description": "...",
  "dependencies": {
    "consumes": [...],
    "produces": [...]
  }
}
```

## Detection Rules

The transformation script detects platforms from `component_id`:

| Component ID Pattern | Detected Platform |
|---------------------|-------------------|
| `keboola.snowflake-transformation` | `snowflake` |
| `keboola.redshift-transformation` | `redshift` |
| `keboola.bigquery-transformation` | `bigquery` |
| `keboola.synapse-transformation` | `synapse` |
| `keboola.duckdb-transformation` | `duckdb` |
| `keboola.python-transformation-v2` | `python` |
| `keboola.r-transformation-v2` | `r` |
| `keboola.dbt-transformation` | `dbt` |
| `keboola.oracle-transformation` | `oracle` |

## Manual Review Checklist

Before finalizing transformation metadata:

1. ✅ Check `platform` field is NOT "unknown"
2. ✅ Verify platform matches actual execution engine
3. ✅ Ensure `component_id` is accurate
4. ✅ Validate platform is in supported list
5. ✅ Update manifest-extended.json statistics

## Example: Fixing Unknown Platforms

### ❌ Before (Unknown)
```json
{
  "uid": "transform:my-duckdb-test",
  "name": "First DuckDB test",
  "platform": "unknown",
  "component_id": "keboola.duckdb-transformation"
}
```

### ✅ After (Fixed)
```json
{
  "uid": "transform:my-duckdb-test",
  "name": "First DuckDB test",
  "platform": "duckdb",
  "component_id": "keboola.duckdb-transformation"
}
```

## Impact on AI Agents

Proper platform classification enables AI agents to:
- Provide platform-specific optimization suggestions
- Estimate costs accurately
- Recommend appropriate resources
- Identify migration candidates
- Generate platform-specific documentation

**Target:** 0 transformations with `platform: "unknown"`
