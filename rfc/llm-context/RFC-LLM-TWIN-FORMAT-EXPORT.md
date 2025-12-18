# RFC: LLM Twin Format Export for `kbc` CLI

## Executive Summary

Implement `kbc llm export` command that transforms Keboola project data into an AI-optimized "twin format" directory structure. This enables AI assistants (like Devin) to understand and work with Keboola projects directly from Git repositories.

**Status**: Ready for implementation
**Target**: MVP with full regeneration, samples support, security controls
**Future**: Incremental sync (`kbc llm sync`)

---

## Prerequisites

### Branch Workflow

1. **This RFC branch** (`vb/llm-context/rfc`) will be merged to `main` first
2. **Rebase `jt-llm-init`** on top of `main` after merge
3. **Finish `kbc llm init`** - Complete the `jt-llm-init` branch (PR #1)
4. **Implement `kbc llm export`** - Create PRs in a PR-train on top of `jt-llm-init`

### Dependencies

- `kbc llm init` command must exist before `kbc llm export` (provides project context)
- `keboola-sdk-go` - If SDK lacks required endpoints, implement them in the SDK repo first
  - SDK Repository: `github.com/keboola/keboola-sdk-go`
  - Create SDK PR, get it merged, then use in this codebase

### Reference Materials

- `rfc/llm-context/output-template/` - Canonical output format specification (all JSON files have inline documentation)
- `rfc/llm-context/DATA-SOURCE-MAPPING.md` - Source inference patterns

---

## 1. Problem Statement

Current state:
- Keboola project configurations accessible only through internal APIs
- AI assistants have no easy access to project information
- No unified, machine-readable source of truth outside Keboola platform

Goal:
- Automatically export project to Git-friendly, AI-optimized format
- Include: buckets, tables, transformations, jobs, lineage, samples
- Every JSON self-documenting with `_comment`, `_purpose`, `_update_frequency`
- Security-aware (encrypt secrets, respect public repo settings)

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    kbc llm export                               │
│                                                                 │
│  CLI Command → Dialog → Operation → Twin Format Generator       │
└─────────────────────────────────────────────────────────────────┘

Flow:
1. CLI Command - Parse flags, validate environment
2. Dialog - Interactive prompts (samples, limits, branches)
3. Operation - Orchestrate data pipeline
4. Fetcher - Retrieve data from Storage API
5. Processor - Transform data, build lineage graph
6. Generator - Write twin_format/ directory structure
```

---

## 3. Output Structure

```
twin_format/
├── manifest.yaml                          # Project config + security
├── manifest-extended.json                 # Single-file project overview
├── README.md                              # Auto-generated documentation
│
├── buckets/
│   ├── index.json                         # Catalog of all buckets
│   └── {bucket}/tables/{table}/
│       └── metadata.json                  # Schema + lineage
│
├── transformations/
│   ├── index.json                         # All transformations by platform
│   └── {name}/
│       └── metadata.json                  # Config + dependencies + job status
│
├── components/
│   ├── index.json                         # All components by type
│   └── {type}/{component_id}/{config_id}/
│       └── metadata.json
│
├── storage/samples/                       # Optional, security-controlled
│   ├── index.json
│   └── {bucket}/{table}/
│       ├── sample.csv                     # First N rows
│       └── metadata.json
│
├── jobs/
│   ├── index.json                         # Statistics + recent jobs
│   ├── recent/{job_id}.json
│   └── by-component/{comp_id}/{cfg_id}/latest.json
│
├── indices/
│   ├── graph.jsonl                        # JSONL lineage graph
│   ├── sources.json                       # Source registry
│   └── queries/                           # Pre-computed queries
│       ├── tables-by-source.json
│       ├── transformations-by-platform.json
│       └── most-connected-nodes.json
│
└── ai/
    └── README.md                          # AI guide with project data
```

---

## 4. Jobs Architecture: Queue vs Storage

### CRITICAL DISTINCTION

There are **two separate job systems** in Keboola:

#### 1. Jobs Queue API (PRIMARY - USER-DEFINED JOBS)

**Purpose**: Tracks user-initiated component runs (transformations, extractors, writers)

**Endpoint**: `GET https://queue.{STACK}/search/jobs?branchId={branchId}&limit={limit}`

**Example**: `GET https://queue.us-east4.gcp.keboola.com/search/jobs?branchId=10336&limit=100`

**What it tracks**:
- Transformation executions (Snowflake, Python, dbt, etc.)
- Extractor runs (data ingestion)
- Writer runs (data export)
- Orchestration runs
- Component configuration executions

**Why it's critical for twin format**:
- ✅ Shows actual data flow (what transformations ran)
- ✅ Contains transformation failures and errors
- ✅ Provides execution metadata (duration, status, inputs/outputs)
- ✅ Essential for understanding "what happened with my data"

**Job metadata includes**:
```json
{
  "id": 55789432,
  "status": "success",
  "operationName": "transformationRun",
  "operationParams": {
    "componentId": "keboola.snowflake-transformation",
    "configurationId": "orders_clean"
  },
  "startTime": "2025-11-28T14:31:32+0100",
  "endTime": "2025-11-28T14:32:15+0100",
  "durationSeconds": 45,
  "error": null
}
```

#### 2. Storage API Jobs (SECONDARY - INTERNAL SYSTEM JOBS)

**Purpose**: Tracks internal Storage API operations

**Endpoint**: `GET https://connection.STACK.com/v2/storage/jobs?limit=100`

**What it tracks**:
- Table imports/exports
- Workspace operations
- File uploads
- Bucket operations
- Internal system tasks

**Why it's less important**:
- Less relevant for understanding data transformations
- More focused on infrastructure operations
- Doesn't show transformation execution details

### Implementation Strategy

**For MVP**:
1. **Fetch Jobs Queue jobs** from `queue.STACK.com/search/jobs` (user-defined transformations) - **PRIMARY DATA SOURCE**
2. Link these jobs to transformations in `transformations/{name}/metadata.json`
3. Store job details in `jobs/recent/{job_id}.json`
4. Optionally fetch Storage API jobs from `connection.STACK.com/v2/storage/jobs` for completeness (but lower priority)

**Job linking algorithm**:
```go
// For each job from Jobs Queue:
for job in jobsQueue {
    // Extract component + config
    key := fmt.Sprintf("%s:%s", job.ComponentID, job.ConfigID)

    // Find matching transformation
    if transform, ok := transformations[key]; ok {
        transform.LastJob = JobInfo{
            LastRunTime:   job.EndTime,
            LastRunStatus: job.Status,
            JobReference:  fmt.Sprintf("jobs/recent/%d.json", job.ID),
            DurationSeconds: job.DurationSeconds,
            LastError:     job.Error,
        }
    }
}
```

---

## 5. Command Design

### New Commands

**`kbc llm export`** - Full twin format export (MVP)
- Exports to **current directory** by default (not `twin_format/` subdirectory)
- Allows existing `.keboola/manifest.json` and `.env*` files (project already initialized)
- Warns and prompts for confirmation if other files exist
- Flags: `--output-dir`, `--with-samples`, `--without-samples`, `--sample-limit`, `--max-samples`, `--force`

**Directory validation logic**:
```go
// Allowed files (ignore these)
allowed := []string{
    ".keboola/",
    ".env",
    ".env.local",
    ".env.dist",
    ".gitignore",
}

// Check for conflicting files
conflicts := []string{}
for _, file := range listFiles(currentDir) {
    if !isAllowed(file, allowed) {
        conflicts = append(conflicts, file)
    }
}

// If conflicts exist
if len(conflicts) > 0 {
    if !flags.Force {
        // Interactive: ask for confirmation
        confirmed := dialogs.Confirm("Directory not empty. Continue?")
        if !confirmed {
            return errors.New("export cancelled")
        }
    }
    // Non-interactive or --force: proceed with warning
    logger.Warnf("Directory contains %d files, proceeding...", len(conflicts))
}
```

**`kbc llm sync`** - Incremental updates (Future)
- Detects changes since last export
- Updates only modified files
- Efficient for continuous development

### Existing Command

**`kbc llm init`** - Project initialization (Keep as-is)
- Creates `.env.local`, `.keboola/manifest.json`
- Separate concern from export
- Typically followed by `kbc llm export` to generate twin format

---

## 6. Implementation Plan (PR Train)

Implementation follows a PR-train approach. Each PR should include tests for the work done.

---

### PR 1: Complete `kbc llm init` (branch: `jt-llm-init`)

**Prerequisite PR** - Complete the existing `jt-llm-init` branch.

**Scope**:
- Finish any remaining work on `kbc llm init` command
- Ensure project initialization creates `.keboola/manifest.json` and `.env.local`
- Merge to `main`

**Tests**: E2E test for `kbc llm init` happy path

---

### PR 2: CLI Scaffolding (+ SDK PR if needed)

**Goal**: Command structure ready, SDK has required endpoints

**Tasks**:
1. **SDK Extensions** (separate PR to `github.com/keboola/keboola-sdk-go` if needed):
   - Check if Jobs Queue API exists in SDK
   - If not, create PR to add: `GET https://queue.{STACK}/search/jobs?branchId={branchId}&limit={limit}`
   - Get SDK PR merged before continuing with this PR

2. **CLI Command Skeleton**:
   - `internal/pkg/service/cli/cmd/llm/export/cmd.go` - Command definition
   - `internal/pkg/service/cli/cmd/llm/export/flags.go` - Flag definitions
   - Register in `cmd/llm/cmd.go`
   - Help messages (`helpmsg/msg/llm/export/short.txt`, `long.txt`)

3. **Operation Skeleton**:
   - `pkg/lib/operation/llm/export/operation.go` - Main operation
   - `pkg/lib/operation/llm/export/options.go` - Options struct

4. **Directory Validation**:
   - Check if current directory is suitable for export
   - Allow `.keboola/`, `.env*`, `.gitignore` (ignore these)
   - Warn if other files exist, prompt for confirmation
   - `--force` flag to skip confirmation

**Files**:
```
internal/pkg/service/cli/cmd/llm/export/cmd.go (NEW)
internal/pkg/service/cli/cmd/llm/export/flags.go (NEW)
internal/pkg/service/cli/cmd/llm/cmd.go (MODIFY - add export subcommand)
internal/pkg/service/cli/helpmsg/msg/llm/export/short.txt (NEW)
internal/pkg/service/cli/helpmsg/msg/llm/export/long.txt (NEW)
pkg/lib/operation/llm/export/operation.go (NEW)
pkg/lib/operation/llm/export/options.go (NEW)
```

**Tests**:
- Unit tests for directory validation logic
- E2E test: `kbc llm export --help` works

---

### PR 3: Fetcher + Core Types

**Goal**: Fetch all required data from Keboola APIs

**Tasks**:
1. **Core Types**:
   - `internal/pkg/llm/twinformat/types.go` - Data structures (TwinTable, TwinTransformation, etc.)

2. **Fetcher Implementation**:
   - `internal/pkg/llm/twinformat/fetcher.go`
   - `FetchProjectMetadata()` - `GET /v2/storage/tokens/verify`
   - `FetchBucketsWithTables()` - `GET /v2/storage/buckets?include=tables,columns,metadata`
   - `FetchJobsQueue()` - `GET https://queue.{STACK}/search/jobs?branchId=...&limit=100`
   - Retry logic with exponential backoff (if not in SDK)

3. **Integration with Operation**:
   - Wire fetcher into `pkg/lib/operation/llm/export/operation.go`
   - Command now fetches data but doesn't generate files yet

**Files**:
```
internal/pkg/llm/twinformat/types.go (NEW)
internal/pkg/llm/twinformat/fetcher.go (NEW)
pkg/lib/operation/llm/export/operation.go (MODIFY)
```

**Tests**:
- Unit tests for fetcher with mocked HTTP responses
- E2E test: `kbc llm export` fetches data without error (empty output)

---

### PR 4: Processor + Lineage

**Goal**: Transform fetched data, build lineage graph

**Tasks**:
1. **Local Transformation Scanner**:
   - `internal/pkg/llm/twinformat/scanner.go`
   - Scan `main/transformation/` directory
   - Read `config.json`, `meta.json`, `description.md`
   - Extract storage mappings (input/output tables)

2. **Platform Detection**:
   - `internal/pkg/llm/twinformat/platform.go`
   - `DetectPlatform(componentId)` → snowflake, python, dbt, etc.
   - Target: 0 unknown platforms

3. **Source Inference**:
   - `internal/pkg/llm/twinformat/source.go`
   - `InferSourceFromBucket(bucketName)` → shopify, hubspot, etc.

4. **Lineage Builder**:
   - `internal/pkg/llm/twinformat/lineage.go`
   - Build graph edges (consumed_by, produces)
   - Compute bidirectional dependencies

5. **Job Linking**:
   - Map jobs to transformations by componentId + configId
   - Extract last run status, time, duration, error

6. **Processor Orchestration**:
   - `internal/pkg/llm/twinformat/processor.go`
   - Orchestrate all processing steps

**Files**:
```
internal/pkg/llm/twinformat/scanner.go (NEW)
internal/pkg/llm/twinformat/platform.go (NEW)
internal/pkg/llm/twinformat/source.go (NEW)
internal/pkg/llm/twinformat/lineage.go (NEW)
internal/pkg/llm/twinformat/processor.go (NEW)
```

**Tests**:
- Unit tests for platform detection (all platforms)
- Unit tests for source inference (all patterns)
- Unit tests for lineage edge creation
- E2E test: processor builds correct lineage for test project

---

### PR 5: Generator (Core Output)

**Goal**: Generate twin format directory structure (without samples)

**Tasks**:
1. **Documentation Fields Helper**:
   - `internal/pkg/llm/twinformat/docfields.go`
   - `AddDocFields()` - Inject `_comment`, `_purpose`, `_update_frequency`

2. **Writers**:
   - `internal/pkg/llm/twinformat/writer/json.go` - Ordered JSON (use existing encoding/json)
   - `internal/pkg/llm/twinformat/writer/jsonl.go` - JSONL for graph
   - `internal/pkg/llm/twinformat/writer/markdown.go` - README/guides

3. **Generator Implementation**:
   - `internal/pkg/llm/twinformat/generator.go`
   - `generateBuckets()` - buckets/index.json + per-table metadata
   - `generateTransformations()` - transformations/index.json + per-transform metadata
   - `generateComponents()` - components/index.json + per-component metadata
   - `generateJobs()` - jobs/index.json + recent/ + by-component/
   - `generateIndices()` - graph.jsonl, sources.json, queries/
   - `generateRootFiles()` - manifest.yaml, manifest-extended.json, README.md
   - `generateAIGuide()` - ai/README.md with real project data

4. **Wire Everything**:
   - Complete `pkg/lib/operation/llm/export/operation.go`
   - Command now produces full output

**Files**:
```
internal/pkg/llm/twinformat/docfields.go (NEW)
internal/pkg/llm/twinformat/writer/json.go (NEW)
internal/pkg/llm/twinformat/writer/jsonl.go (NEW)
internal/pkg/llm/twinformat/writer/markdown.go (NEW)
internal/pkg/llm/twinformat/generator.go (NEW)
pkg/lib/operation/llm/export/operation.go (MODIFY)
```

**Tests**:
- Unit tests for doc fields injection
- Unit tests for JSONL writer
- E2E test: `kbc llm export` produces correct directory structure
- E2E test: All JSON files have `_comment`, `_purpose`, `_update_frequency`
- E2E test: Lineage graph is correct

---

### PR 6: Samples + Security

**Goal**: Add sample export with security controls

**Tasks**:
1. **Sample Fetcher**:
   - Extend fetcher with `FetchTableSample(tableId, limit)`
   - `GET /v2/storage/tables/{id}/data-preview?limit=N`

2. **CSV Writer**:
   - `internal/pkg/llm/twinformat/writer/csv.go`

3. **Security Layer**:
   - `internal/pkg/llm/twinformat/security.go`
   - `IsPublicRepository()` - Detect public Git repos
   - `EncryptSecrets()` - Mask fields starting with `#` → `***ENCRYPTED***`
   - Auto-disable samples for public repos

4. **Dialog Implementation**:
   - `internal/pkg/service/cli/cmd/llm/export/dialog.go`
   - `AskExportOptions()` - Interactive prompts for samples
   - Auto-detect public repo and suggest samples=off

5. **Sample Generation**:
   - Extend generator with `generateSamples()`
   - `storage/samples/index.json` + per-table sample.csv + metadata.json

6. **Flags**:
   - `--with-samples` / `--without-samples`
   - `--sample-limit=N` (default: 100, max: 1000)
   - `--max-samples=N` (default: 50, total tables to sample)

**Files**:
```
internal/pkg/llm/twinformat/security.go (NEW)
internal/pkg/llm/twinformat/writer/csv.go (NEW)
internal/pkg/service/cli/cmd/llm/export/dialog.go (NEW)
internal/pkg/llm/twinformat/fetcher.go (MODIFY - add sample fetching)
internal/pkg/llm/twinformat/generator.go (MODIFY - add samples generation)
internal/pkg/service/cli/cmd/llm/export/flags.go (MODIFY - add sample flags)
```

**Tests**:
- Unit tests for secret encryption
- Unit tests for public repo detection
- E2E test: `kbc llm export --with-samples` produces samples
- E2E test: `kbc llm export --without-samples` produces no samples
- E2E test: Public repo auto-disables samples
- E2E test: Secrets are encrypted in output

---

### PR 7: Polish + Performance

**Goal**: Production-ready quality

**Tasks**:
1. **Error Handling**:
   - Graceful degradation on API failures
   - User-friendly error messages
   - Partial export support (log warnings, continue)

2. **Performance**:
   - Parallel API calls (semaphore pattern)
   - Progress reporting (spinners for each phase)
   - Memory optimization for large projects

3. **Edge Cases**:
   - Empty project (0 tables, 0 transformations)
   - Large project (1000+ tables)
   - Missing/deleted tables (404 from API)
   - Malformed transformation configs

**Files**:
```
(Various files modified for error handling and performance)
```

**Tests**:
- E2E test: Empty project produces valid output
- E2E test: API error doesn't crash, produces partial output with warnings
- E2E test: Progress is reported during export

---

## 7. Key Technical Requirements

### JSON Documentation Fields (MANDATORY)

Every JSON file MUST have:
```json
{
  "_comment": "GENERATION: [API endpoint or computation]",
  "_purpose": "Why this file exists",
  "_update_frequency": "When to regenerate"
}
```

Optional fields:
- `_security`: Security notes (for sensitive files)
- `_retention`: Retention policy (for jobs, samples)

### Job Execution Tracking (MANDATORY)

Every transformation MUST include:
```json
{
  "job_execution": {
    "last_run_time": "2025-11-28T14:32:15+0100",
    "last_run_status": "success",
    "job_reference": "jobs/recent/55789432.json",
    "duration_seconds": 45,
    "last_error": null
  }
}
```

### UID Format

- Tables: `table:{bucket_clean}/{table_name}`
- Transformations: `transform:{transform_name}`
- Components: `component:{type}:{component_id}:{config_id}`

### JSONL Graph Format

```jsonl
{"_meta":{"total_edges":2,"total_nodes":3,"updated":"2025-01-20T10:00:00Z"}}
{"source":"table:raw/orders","target":"transform:orders_clean","type":"consumed_by"}
{"source":"transform:orders_clean","target":"table:processed/orders_clean","type":"produces"}
```

### Security Rules

1. **Secret Encryption**: Fields starting with `#` → `***ENCRYPTED***`
2. **Public Repo Safety**: Always disable samples for public repos (override any flag)
3. **No Plaintext Tokens**: Never write API tokens to output files

### Platform Detection

Must achieve **0 unknown platforms**. Supported platforms:
- Snowflake, Redshift, BigQuery, Synapse, DuckDB
- Python, R
- dbt
- Oracle, MySQL, PostgreSQL
- SQL (generic fallback)

---

## 8. Codebase Integration

### Follow Existing Patterns

✅ **Use filesystem abstraction**: `filesystem.Fs` (never `os` or `filepath`)
✅ **Error wrapping**: `errors.Errorf()` for stack traces (never `fmt.Errorf`)
✅ **Thin CLI pattern**: Command delegates to operation in `pkg/lib/operation/`
✅ **Dependencies interface**: Define minimal interface, inject via DI
✅ **Telemetry**: Wrap operations with span tracing
✅ **Structured logging**: Use `d.Logger().Infof()` (never `fmt.Print*`)

### Reuse Existing Components

- **API Client**: `keboola.AuthorizedAPI` from keboola-sdk-go
- **JSON Encoding**: `internal/pkg/encoding/json` (preserves order)
- **File Operations**: `internal/pkg/filesystem`
- **Dialogs**: `internal/pkg/service/cli/dialog`
- **Progress Bars**: Existing CLI patterns

---

## 9. Package Structure

**Note**: Detailed file lists per PR are in Section 6 (Implementation Plan).

```
internal/pkg/llm/twinformat/        # Core twin format logic
├── types.go                        # Data structures
├── fetcher.go                      # API data fetching
├── scanner.go                      # Local transformation scan
├── processor.go                    # Data transformation orchestration
├── platform.go                     # Platform detection
├── source.go                       # Source inference
├── lineage.go                      # Lineage graph building
├── generator.go                    # File generation
├── docfields.go                    # Documentation fields helper
├── security.go                     # Security controls
└── writer/
    ├── json.go                     # Ordered JSON
    ├── jsonl.go                    # JSONL graph
    ├── csv.go                      # CSV samples
    └── markdown.go                 # README/guides

pkg/lib/operation/llm/export/       # Operation layer
├── operation.go
└── options.go

internal/pkg/service/cli/cmd/llm/export/  # CLI command
├── cmd.go
├── dialog.go
└── flags.go
```

---

## 10. API Endpoints Required

| Purpose | Endpoint | Priority | Frequency |
|---------|----------|----------|-----------|
| Project metadata | `GET https://connection.{STACK}/v2/storage/tokens/verify` | HIGH | Once per export |
| Buckets & tables | `GET https://connection.{STACK}/v2/storage/buckets?include=tables,columns,metadata` | HIGH | Once per export |
| **Jobs Queue** | `GET https://queue.{STACK}/search/jobs?branchId={branchId}&limit=100` | **PRIMARY** | Once per export |
| Storage API jobs | `GET https://connection.{STACK}/v2/storage/jobs?limit=100` | SECONDARY | Once per export (optional) |
| Table sample | `GET https://connection.{STACK}/v2/storage/tables/{id}/data-preview?limit=N` | MEDIUM | Per table (if enabled) |

**Notes**:
- **STACK** = Your Keboola stack (e.g., `us-east4.gcp.keboola.com`, `north-europe.azure.keboola.com`)
- **Example Jobs Queue**: `https://queue.us-east4.gcp.keboola.com/search/jobs?branchId=10336&limit=100`
- Jobs Queue API (`queue.{STACK}`) is the primary source for transformation execution data
- Storage API jobs (`connection.{STACK}`) are less critical but can be included for completeness
- If `keboola-sdk-go` lacks required methods, implement them in the SDK first (repo: `github.com/keboola/keboola-sdk-go`)

---

## 11. Testing Strategy

**Principle**: Each PR includes tests for the work done. Focus on E2E tests that cover happy paths and edge cases.

### Unit Tests (Per PR)
Tests are included in each PR for:
- Platform detection (all platforms) - PR 4
- Source inference (all patterns) - PR 4
- Documentation fields injection - PR 5
- Lineage edge creation - PR 4
- Secret encryption - PR 6
- Public repo detection - PR 6

### E2E Tests (Critical Path)

**Happy Path Tests**:
- `kbc llm export` produces correct directory structure
- All JSON files have `_comment`, `_purpose`, `_update_frequency`
- Lineage graph contains correct edges
- Jobs are linked to transformations
- `--with-samples` produces sample files
- `--without-samples` omits sample files

**Edge Case Tests**:
- Empty project (0 tables, 0 transformations) → valid output
- API error during fetch → partial output with warnings
- Public repo detected → samples auto-disabled
- Secrets in config → encrypted in output
- Missing/deleted table → graceful skip with warning
- Directory not empty → prompts for confirmation (or `--force`)

### Manual Testing Checklist
- Interactive mode prompts work correctly
- Non-interactive mode works for CI/CD
- Progress reporting visible during export
- Error messages are user-friendly
- Help text is clear and accurate

---

## 12. Migration from Python Reference

### Key Differences

| Aspect | Python v3 (908 lines) | Go Implementation |
|--------|---------------------|-------------------|
| Language | Python 3 | Go 1.21+ |
| API Client | `requests` library | `keboola-sdk-go` |
| File I/O | `pathlib.Path` | `filesystem.Fs` abstraction |
| JSON | `json` module | `internal/pkg/encoding/json` |
| Error handling | Exceptions | Wrapped errors with stack traces |
| Concurrency | Sequential | Parallel with goroutines |
| Testing | None in scripts | Unit + Integration + E2E |

### Function Mapping (Top 10)

| Python | Go Package | Function |
|--------|------------|----------|
| `TwinFormatTransformerV3.__init__()` | `twinformat` | `NewTransformer()` |
| `fetch_project_metadata()` | `fetcher.go` | `FetchProjectMetadata()` |
| `scan_transformations()` | `scanner.go` | `ScanTransformations()` |
| `detect_platform()` | `platform.go` | `DetectPlatform()` |
| `infer_source_from_bucket()` | `source.go` | `InferSourceFromBucket()` |
| `add_doc_fields()` | `docfields.go` | `AddDocFields()` |
| `create_twin_structure()` | `generator.go` | `Generate()` |
| `_create_jobs_structure()` | `generator.go` | `generateJobs()` |
| `_create_indices()` | `generator.go` | `generateIndices()` |
| `_create_ai_guide()` | `generator.go` | `generateAIGuide()` |

---

## 13. Success Criteria

### MVP Acceptance Criteria

✅ **Functional**:
- [ ] Command `kbc llm export` runs successfully
- [ ] Generates complete `twin_format/` directory structure
- [ ] All JSON files have documentation fields
- [ ] Platform detection achieves 0 unknown transformations
- [ ] Job execution tracking works for all transformations
- [ ] Lineage graph built correctly (JSONL format)
- [ ] Samples export controlled by flags and security
- [ ] Public repo detection works correctly
- [ ] Secrets encrypted (`#` fields → `***ENCRYPTED***`)

✅ **Quality**:
- [ ] 80%+ test coverage
- [ ] All E2E tests passing
- [ ] No linter errors
- [ ] Code follows established patterns
- [ ] Help messages clear and accurate

✅ **Performance**:
- [ ] Export completes in <2 minutes for small projects (<100 tables)
- [ ] Export completes in <10 minutes for large projects (<1000 tables)
- [ ] Memory usage reasonable (<500MB for typical project)

✅ **Documentation**:
- [ ] Help text explains all flags
- [ ] README updated with examples
- [ ] Code comments for complex logic

---

## 14. Future Enhancements (Post-MVP)

### v2.0: Incremental Sync

**`kbc llm sync` command**:
- Read existing `manifest-extended.json`
- Compare timestamps
- Fetch only new jobs since last update
- Update only changed files
- Append to jobs history
- Efficient for continuous development

### v3.0: Git Integration

**Auto-commit on changes**:
- Watch project for modifications
- Trigger incremental sync
- Commit to Git automatically
- Push to remote branch

### v4.0: Enhanced Lineage

**SQL parsing**:
- Parse SQL from transformations
- Extract table references from code
- Build more accurate dependency graph
- Support for nested queries

---

## 15. Decisions

### D1: SDK Support for Jobs API
**Decision**: If `keboola-sdk-go` lacks Jobs Queue API support, create a PR to the SDK repo first.
- SDK Repository: `github.com/keboola/keboola-sdk-go`
- Endpoint: `GET https://queue.{STACK}/search/jobs?branchId={branchId}&limit={limit}`

### D2: Retry Logic
**Decision**: Use SDK retry mechanism. SDK should handle retries with exponential backoff.

### D3: Progress Reporting
**Decision**: Use existing CLI patterns. Reference `dbt/generate` command for spinner implementation.

### D4: Job Storage Retention
**Decision**: Keep last 100 jobs in `jobs/recent/`. No auto-cleanup.

### D5: Sample Export Default
**Decision**: Auto-detect (enabled for private repos, disabled for public). User can override with flags.

---

## 16. Risk Assessment

### High Risk
- **API Rate Limits**: Large projects may hit rate limits
  - *Mitigation*: Implement retry with backoff, configurable limits

### Medium Risk
- **Large Projects**: 1000+ tables could be slow
  - *Mitigation*: Parallel fetching, progress reporting, configurable limits

### Low Risk
- **Platform Detection**: May miss some transformation types
  - *Mitigation*: Comprehensive pattern list, log warnings for unknown

- **Public Repo Detection**: Heuristic may be inaccurate
  - *Mitigation*: Explicit `--public-repo` flag as fallback

---

## 17. Next Steps

1. **Merge this RFC** to `main`
2. **Rebase `jt-llm-init`** on `main`
3. **PR 1**: Complete `kbc llm init` (from `jt-llm-init`)
4. **PR 2-7**: Implement in PR train as described in Section 6

---

## Appendix: Command Examples

```bash
# Basic export to current directory
# (requires directory to be empty or only contain .keboola/, .env*)
kbc llm export

# Export to custom subdirectory
kbc llm export --output-dir twin_format

# Force export even if directory is not empty (skip confirmation)
kbc llm export --force

# Export without samples
kbc llm export --without-samples

# Export with custom sample limits
kbc llm export --sample-limit 500 --max-samples 10

# Force sample export (override public repo detection)
kbc llm export --with-samples

# Specify branches
kbc llm export --branches "main,dev"

# Typical workflow after init
cd my-project
kbc llm init
kbc llm export  # Exports to current directory

# Full flags example
kbc llm export \
  --output-dir twin_format \
  --with-samples \
  --sample-limit 100 \
  --max-samples 50 \
  --branches "*" \
  --force \
  --storage-api-host connection.keboola.com \
  --storage-api-token "$KBC_TOKEN"
```

---

**RFC Version**: 1.0
**Author**: Claude (AI Assistant)
**Date**: 2025-12-09
**Status**: Ready for Implementation
