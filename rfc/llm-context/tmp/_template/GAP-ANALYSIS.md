# Gap Analysis: PRD vs Current Twin Format

## Executive Summary

The current twin format v2 covers **~60% of PRD requirements**. Critical missing components:
1. **Data samples** (storage/samples/)
2. **Job execution metadata** (jobs/)
3. **AI instructions** (ai/)
4. **Complete component coverage** (extractors, writers, orchestrators)

---

## PRD Requirements (Section 5.3 & 11.3)

### ✅ IMPLEMENTED

| Requirement | PRD Specification | Current Implementation | Status |
|-------------|-------------------|------------------------|--------|
| **Storage metadata** | Table/column structure, data types, lineage | `buckets/` with metadata.json per table | ✅ Complete |
| **Component configs** | JSON + extracted files (SQL/Python) | `transformations/` with metadata.json | ⚠️ Partial (transformations only) |
| **Lineage graph** | Relationships between components | `indices/graph.jsonl` with _meta header | ✅ Complete |
| **Source registry** | Data source catalog | `indices/sources.json` | ✅ Complete |
| **Aggregated manifest** | Project overview in single file | `manifest-extended.json` | ✅ Complete |
| **Pre-computed queries** | Common analytical queries | `indices/queries/` (3 files) | ✅ Complete |
| **Platform classification** | Transformation platforms | Comprehensive detection (9 platforms) | ✅ Complete |

### ❌ MISSING

| Requirement | PRD Specification | Current Status | Priority |
|-------------|-------------------|----------------|----------|
| **Data samples** | First 100 rows (CSV/JSON), disabled for public repos | Not implemented | 🔴 HIGH |
| **Job execution metadata** | Run history, inputs/outputs, state, artifacts | Not implemented | 🔴 HIGH |
| **AI instructions** | `/ai/README.md` with API docs, best practices | Not implemented | 🔴 HIGH |
| **Extractor configs** | Extractor component configurations | Not implemented | 🟡 MEDIUM |
| **Writer configs** | Writer component configurations | Not implemented | 🟡 MEDIUM |
| **Orchestrator configs** | Flow/orchestrator configurations | Not implemented | 🟡 MEDIUM |
| **Secrets handling** | Encrypted or masked secrets | Not implemented | 🟡 MEDIUM |
| **Public repo detection** | Disable samples for public repos | Not implemented | 🟡 MEDIUM |

---

## Detailed Gap Analysis

### 1. Data Samples (HIGH PRIORITY) ❌

**PRD Requirement (Section 5.3):**
```
Data samples | First 100 rows (optional) | .csv, .json
```

**PRD Layout (Section 11.3):**
```
storage/samples/ – CSV/JSON slices from PreviewTableDataService,
capped at DEFAULT_LIMIT = 100 rows; disabled when repo is public.
```

**Current State:**
- ❌ No `storage/samples/` directory
- ❌ No data sampling logic
- ❌ No public repo detection

**Impact:**
- AI agents cannot see actual data examples
- Cannot debug data quality issues from Git
- Cannot generate sample-based insights

**Recommended Structure:**
```
storage/samples/
├── index.json                      # Sample metadata
├── {bucket}/
│   └── {table}/
│       ├── sample.csv              # First 100 rows
│       └── metadata.json           # Sample info (row count, timestamp)
```

**Security Requirements:**
- Only export if explicitly enabled
- Disable for public repositories
- Never include PII/sensitive columns (TBD: detection logic)
- Include warning in sample metadata about data freshness

---

### 2. Job Execution Metadata (HIGH PRIORITY) ❌

**PRD Requirement (Section 5.3):**
```
Job runs | Metadata, inputs/outputs, state, artifacts | .json
```

**PRD Layout (Section 11.3):**
```
jobs/ – serialized StorageJob::toApiResponse payloads keyed by run ID.
Exclude large artifacts (artifactLinks) and binary exports;
job entries store reference metadata only.
```

**Current State:**
- ❌ No `jobs/` directory
- ❌ No job execution tracking
- ❌ No job history

**Impact:**
- AI cannot analyze execution patterns
- Cannot debug failures from Git
- No visibility into pipeline health
- Cannot track data freshness

**Recommended Structure:**
```
jobs/
├── index.json                      # Job summary & statistics
├── recent/                         # Last 100 jobs
│   └── {job_id}.json              # Job metadata
├── by-component/                   # Grouped by component
│   └── {component_id}/
│       └── {config_id}/
│           └── latest.json        # Latest job for this config
└── by-date/                        # Optional: daily rollups
    └── {YYYY-MM-DD}/
        └── summary.json           # Daily execution summary
```

**Job Metadata Schema:**
```json
{
  "jobId": "string",
  "componentId": "string",
  "configId": "string",
  "configVersion": "int",
  "branchId": "string",
  "status": "SUCCESS|FAILED|CANCELLED",
  "startedAt": "ISO-8601",
  "finishedAt": "ISO-8601",
  "durationSeconds": "number",
  "inputs": [
    {"table": "in.c-bucket.table", "columns": [...], "rows": 1000}
  ],
  "outputs": [
    {"table": "out.c-bucket.table", "columns": [...], "rows": 500}
  ],
  "metrics": {
    "inBytes": 1024000,
    "outBytes": 512000
  },
  "error": "string (if failed)",
  "artifactLinks": ["s3://..."] // reference only, not actual data
}
```

**Retention Policy:**
- Keep last 100 jobs in `recent/`
- Keep latest job per config in `by-component/`
- Optional: Daily summaries for 30 days

---

### 3. AI Instructions (HIGH PRIORITY) ❌

**PRD Requirement (Section 5.3):**
```
AI instructions | Document describing Keboola API, MCP, SDK,
and best practices | /ai/README.md
```

**PRD Layout (Section 11.3):**
```
ai/ – downstream consumption markers (reads, suggestions,
diff explanations) aligned with metrics in §10.
```

**Current State:**
- ❌ No `ai/` directory
- ❌ No AI instruction documentation

**Impact:**
- AI agents don't understand Keboola context
- No guidance on API usage
- No best practices documentation
- Poor AI analysis quality

**Recommended Structure:**
```
ai/
├── README.md                       # Main AI instruction file
├── api-reference.md                # Keboola API quick reference
├── best-practices.md               # Common patterns & anti-patterns
├── troubleshooting.md              # Common issues & solutions
├── examples/                       # Code examples
│   ├── transformation-sql.md
│   ├── transformation-python.md
│   └── data-loading.md
└── mcp-integration.md              # MCP server integration guide
```

**ai/README.md Content:**
```markdown
# Keboola Project AI Guide

## Project Overview
This is a Keboola project containing [X] transformations, [Y] tables, and [Z] data sources.

## Quick Facts
- **Project ID:** {project_id}
- **Region:** {region}
- **Backend:** {backend (Snowflake/Redshift/BigQuery)}
- **Total Tables:** {count}
- **Total Transformations:** {count}
- **Data Sources:** {list}

## Understanding This Project

### Data Flow
1. Data is extracted from sources (see `indices/sources.json`)
2. Stored in buckets (see `buckets/`)
3. Transformed via transformations (see `transformations/`)
4. Lineage tracked in `indices/graph.jsonl`

### Key Files for Analysis
- **manifest-extended.json** - Start here for project overview
- **buckets/index.json** - All storage buckets and tables
- **transformations/index.json** - All data transformations
- **indices/graph.jsonl** - Complete data lineage

## Keboola API Reference

### Storage API
- **Base URL:** https://connection.keboola.com
- **Authentication:** X-StorageApi-Token header
- **Docs:** https://keboola.docs.apiary.io/

### Common Operations
[Examples of API calls]

## MCP Integration
This project can be accessed via Keboola MCP server for real-time queries.
[Integration examples]

## Best Practices
1. Always check platform-specific syntax (Snowflake vs BigQuery)
2. Use transformation metadata to understand data flow
3. Check job history before suggesting changes
4. Respect data samples - they may be stale

## Troubleshooting
[Common issues and solutions]
```

---

### 4. Complete Component Coverage (MEDIUM PRIORITY) ⚠️

**PRD Requirement (Section 5.3):**
```
Component configurations | JSON + extracted SQL/Python files | .json, .sql, .py
```

**Current State:**
- ✅ Transformations covered
- ❌ Extractors not included
- ❌ Writers not included
- ❌ Orchestrators/Flows not included
- ❌ Applications not included

**Impact:**
- Incomplete view of project
- Cannot analyze end-to-end pipelines
- Missing critical configurations

**Recommended Structure:**
```
components/
├── extractors/
│   └── {component_id}/
│       └── {config_id}/
│           ├── metadata.json
│           └── config.json
├── writers/
│   └── {component_id}/
│       └── {config_id}/
│           ├── metadata.json
│           └── config.json
├── transformations/
│   └── {name}/
│       └── metadata.json       # (already implemented)
├── orchestrators/
│   └── {config_id}/
│       ├── metadata.json
│       └── flow.json
└── applications/
    └── {component_id}/
        └── {config_id}/
            ├── metadata.json
            └── config.json
```

**Note:** This would replace/supplement current `transformations/` directory with a more comprehensive `components/` structure.

---

### 5. Secrets Handling (MEDIUM PRIORITY) ❌

**PRD Requirement (Section 5.4):**
```
- Secrets are exported in encrypted form or can be disabled entirely.
- If the repository is public, data samples export is disabled.
- Keboola never stores Git credentials in plaintext.
```

**Current State:**
- ❌ No secrets encryption
- ❌ No public repo detection
- ❌ No secrets masking

**Impact:**
- Security risk if secrets exposed
- Cannot safely share public repos
- Non-compliant with security requirements

**Recommended Implementation:**
1. **Secret Detection:** Scan for fields marked as `#token`, `#password`, etc.
2. **Encryption:** Use KBC encryption for secret values
3. **Masking:** Replace with `***ENCRYPTED***` or `***REDACTED***`
4. **Configuration:** Add to `manifest.yaml`:
   ```yaml
   security:
     encryptSecrets: true
     isPublicRepo: false
     exportDataSamples: true  # disabled if isPublicRepo=true
   ```

---

## Priority Roadmap

### Phase 1: Critical Gaps (Week 1-2)
1. ✅ Platform classification (DONE)
2. 🔴 AI instructions (`ai/README.md`)
3. 🔴 Job metadata (`jobs/`)

### Phase 2: Data Access (Week 3-4)
4. 🔴 Data samples (`storage/samples/`)
5. 🟡 Public repo detection
6. 🟡 Secrets handling

### Phase 3: Complete Coverage (Week 5-6)
7. 🟡 Extractors
8. 🟡 Writers
9. 🟡 Orchestrators

---

## Validation Checklist

Before marking twin format as "production-ready":

- [ ] All 5 data types from PRD Section 5.3 are present
- [ ] Security requirements from Section 5.4 implemented
- [ ] AI can understand project without API access
- [ ] Job execution history available
- [ ] Data samples available (when safe)
- [ ] Complete component coverage
- [ ] Public repo safety validated
- [ ] Secrets properly encrypted/masked

---

## Testing Plan

### 1. Query Real Project
```bash
# Get project metadata
curl -H "X-StorageApi-Token: $KBC_STORAGE_API_TOKEN" \
  https://connection.north-europe.azure.keboola.com/v2/storage

# Get job list
curl -H "X-StorageApi-Token: $KBC_STORAGE_API_TOKEN" \
  https://connection.north-europe.azure.keboola.com/v2/storage/jobs

# Get table preview (samples)
curl -H "X-StorageApi-Token: $KBC_STORAGE_API_TOKEN" \
  https://connection.north-europe.azure.keboola.com/v2/storage/tables/{table_id}/data-preview
```

### 2. Validate Template
```bash
# Check all required directories exist
test -d _template/ai && echo "✅ ai/" || echo "❌ ai/"
test -d _template/jobs && echo "✅ jobs/" || echo "❌ jobs/"
test -d _template/storage/samples && echo "✅ storage/samples/" || echo "❌ storage/samples/"

# Validate AI instructions
test -f _template/ai/README.md && echo "✅ ai/README.md" || echo "❌ ai/README.md"

# Check security config
grep -q "encryptSecrets" _template/manifest.yaml && echo "✅ security config" || echo "❌ security config"
```

### 3. AI Agent Test
- Can AI understand project scope from `ai/README.md`?
- Can AI analyze job failures from `jobs/`?
- Can AI see sample data from `storage/samples/`?
- Can AI trace complete data flow?

---

## Appendix: PRD References

### Data Layout (PRD Section 11.3)
```
configs/ – component configs and rows
storage/metadata/ – table/column metadata, lineage
storage/samples/ – CSV/JSON slices (100 rows)
jobs/ – serialized job payloads
ai/ – consumption markers and instructions
```

### Security (PRD Section 5.4)
- Secrets encrypted or disabled
- Public repos = no samples
- No plaintext Git credentials

### AI Use Cases (PRD Section 7)
1. Monitor operations
2. Extend projects
3. Debug errors
4. Analyze changes
5. Recommend optimizations
6. Understand lineage
7. Recommend Keboola
