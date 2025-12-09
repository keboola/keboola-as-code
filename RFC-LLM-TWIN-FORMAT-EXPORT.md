# RFC: LLM Twin Format Export for `kbc` CLI

## Executive Summary

Implement `kbc llm export` command that transforms Keboola project data into an AI-optimized "twin format" directory structure. This enables AI assistants (like Devin) to understand and work with Keboola projects directly from Git repositories.

**Status**: Ready for implementation
**Target**: MVP with full regeneration, samples support, security controls
**Future**: Incremental sync (`kbc llm sync`)

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
│                    kbc llm export                                │
│                                                                   │
│  CLI Command → Dialog → Operation → Twin Format Generator        │
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

**Endpoint**: `GET https://queue.STACK.com/search/jobs` (search/list jobs)

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

## 6. Implementation Plan

### Phase 1: Foundation (Week 1)

**Goal**: Basic scaffolding and API integration

**Tasks**:
1. Create package structure:
   - `internal/pkg/llm/twinformat/` - Core logic
   - `pkg/lib/operation/llm/export/` - Operation layer
   - `internal/pkg/service/cli/cmd/llm/export/` - CLI command

2. Implement directory validation:
   - Check if current directory is suitable for export
   - Allow `.keboola/manifest.json` and `.env*` files (ignore these)
   - Warn if other files exist (buckets/, transformations/, etc.)
   - Prompt for confirmation in interactive mode
   - `--force` flag to skip confirmation

3. Implement `Fetcher`:
   - `FetchProjectMetadata()` - `GET https://connection.STACK.com/v2/storage/tokens/verify`
   - `FetchTables()` - `GET https://connection.STACK.com/v2/storage/buckets?include=...`
   - `FetchJobsQueue()` - **PRIMARY**: `GET https://queue.STACK.com/search/jobs` (transformation runs)
   - `FetchStorageJobs()` - **SECONDARY**: `GET https://connection.STACK.com/v2/storage/jobs?limit=100` (optional)
   - Retry logic with exponential backoff

4. CLI command skeleton:
   - Register `kbc llm export` command
   - Define flags (output-dir, samples, limits, force)
   - Basic validation

5. Unit tests for fetcher and validation

**Deliverable**: Can fetch data from API, directory validation works, no file generation yet

**Critical Files**:
- `internal/pkg/llm/twinformat/fetcher.go` (NEW)
- `internal/pkg/llm/twinformat/types.go` (NEW)
- `pkg/lib/operation/llm/export/operation.go` (NEW)
- `internal/pkg/service/cli/cmd/llm/export/cmd.go` (NEW)

---

### Phase 2: Processing & Lineage (Week 2)

**Goal**: Transform API data into twin format structures

**Tasks**:
1. Implement `Processor`:
   - Convert API tables to `TwinTable` structs
   - Scan local transformations from filesystem
   - Build lineage graph (edges: consumed_by, produces)
   - Compute bidirectional dependencies

2. Platform detection:
   - `DetectPlatform()` - Identify Snowflake, Python, dbt, etc.
   - Pattern matching on component IDs
   - Target: 0 unknown platforms

3. Source inference:
   - `InferSourceFromBucket()` - shopify, hubspot, analytics, etc.
   - Pattern matching on bucket names

4. Job linking:
   - Map jobs to transformations by componentId + configId
   - Extract last run status, time, duration
   - Handle missing jobs gracefully

5. Comprehensive unit tests

**Deliverable**: Can process data, build lineage, compute dependencies

**Critical Files**:
- `internal/pkg/llm/twinformat/processor.go` (NEW)
- `internal/pkg/llm/twinformat/lineage.go` (NEW)
- `internal/pkg/llm/twinformat/platform.go` (NEW)
- `internal/pkg/llm/twinformat/source.go` (NEW)
- `internal/pkg/llm/twinformat/scanner.go` (NEW)

---

### Phase 3: File Generation (Week 3)

**Goal**: Generate complete twin format directory

**Tasks**:
1. Implement `Generator`:
   - `generateBuckets()` - buckets/ + index.json
   - `generateTransformations()` - transformations/ + index.json
   - `generateJobs()` - jobs/ + recent/ + by-component/
   - `generateIndices()` - graph.jsonl, sources.json, queries/
   - `generateRootFiles()` - manifest.yaml, manifest-extended.json, README.md
   - `generateAIGuide()` - ai/README.md with real project data

2. Documentation fields helper:
   - `AddDocFields()` - Inject `_comment`, `_purpose`, `_update_frequency`
   - Preserve field order for AI readability

3. File writers:
   - Ordered JSON writer (use existing `internal/pkg/encoding/json`)
   - JSONL writer for graph (first line = metadata, rest = edges)
   - Markdown writer for README/guides

4. Integration tests with mocked API

**Deliverable**: Full twin format generation working (without samples)

**Critical Files**:
- `internal/pkg/llm/twinformat/generator.go` (NEW)
- `internal/pkg/llm/twinformat/docfields.go` (NEW)
- `internal/pkg/llm/twinformat/writer/json.go` (NEW)
- `internal/pkg/llm/twinformat/writer/jsonl.go` (NEW)
- `internal/pkg/llm/twinformat/writer/markdown.go` (NEW)

---

### Phase 4: Samples & Security (Week 4)

**Goal**: Add sample export with security controls

**Tasks**:
1. Sample fetcher:
   - `FetchTableSample()` - `GET /v2/storage/tables/{id}/data-preview?limit=N`
   - CSV writer for samples
   - Sample metadata tracking

2. Security layer:
   - `IsPublicRepository()` - Detect public Git repos
   - `EncryptSecrets()` - Mask fields starting with `#`
   - Auto-disable samples for public repos

3. Dialog implementation:
   - `AskExportOptions()` - Interactive prompts
   - Auto-detect public repo and suggest samples=off
   - Confirm sample export in interactive mode

4. Flags:
   - `--with-samples` / `--without-samples`
   - `--sample-limit=N` (default: 100, max: 1000)
   - `--max-samples=N` (default: 50, total tables to sample)

5. E2E tests with real project

**Deliverable**: Complete MVP with samples and security

**Critical Files**:
- `internal/pkg/llm/twinformat/security.go` (NEW)
- `internal/pkg/llm/twinformat/writer/csv.go` (NEW)
- `internal/pkg/service/cli/cmd/llm/export/dialog.go` (NEW)
- `internal/pkg/service/cli/cmd/llm/export/flags.go` (NEW)

---

### Phase 5: Polish & Documentation (Week 5)

**Goal**: Production-ready

**Tasks**:
1. Error handling:
   - Graceful degradation on API failures
   - User-friendly error messages
   - Partial export support (log warnings, continue)

2. Performance:
   - Parallel API calls (semaphore pattern)
   - Progress reporting (spinners for each phase)
   - Memory optimization for large projects

3. Documentation:
   - Help messages (`helpmsg/msg/llm/export/short.txt`, `long.txt`)
   - Update main README
   - Code examples in help text

4. Code review & testing:
   - Full test coverage (unit, integration, E2E)
   - Manual testing checklist
   - Security audit (secret handling, public repos)

**Deliverable**: Production-ready `kbc llm export` command

**Critical Files**:
- `internal/pkg/service/cli/helpmsg/msg/llm/export/short.txt` (NEW)
- `internal/pkg/service/cli/helpmsg/msg/llm/export/long.txt` (NEW)

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

## 9. Critical Files to Create/Modify

### Create (22 new files)

**Core Logic**:
1. `internal/pkg/llm/twinformat/transformer.go` - Main orchestrator
2. `internal/pkg/llm/twinformat/fetcher.go` - API data fetching
3. `internal/pkg/llm/twinformat/processor.go` - Data transformation
4. `internal/pkg/llm/twinformat/generator.go` - File generation
5. `internal/pkg/llm/twinformat/lineage.go` - Lineage graph building
6. `internal/pkg/llm/twinformat/scanner.go` - Local transformation scan
7. `internal/pkg/llm/twinformat/platform.go` - Platform detection
8. `internal/pkg/llm/twinformat/source.go` - Source inference
9. `internal/pkg/llm/twinformat/security.go` - Security controls
10. `internal/pkg/llm/twinformat/docfields.go` - Documentation fields
11. `internal/pkg/llm/twinformat/types.go` - Core data structures

**Writers**:
12. `internal/pkg/llm/twinformat/writer/json.go` - Ordered JSON
13. `internal/pkg/llm/twinformat/writer/jsonl.go` - JSONL graph
14. `internal/pkg/llm/twinformat/writer/csv.go` - CSV samples
15. `internal/pkg/llm/twinformat/writer/markdown.go` - README/guides

**Operation Layer**:
16. `pkg/lib/operation/llm/export/operation.go` - Main operation
17. `pkg/lib/operation/llm/export/options.go` - Options + validation

**CLI Command**:
18. `internal/pkg/service/cli/cmd/llm/export/cmd.go` - Command definition
19. `internal/pkg/service/cli/cmd/llm/export/dialog.go` - Interactive prompts
20. `internal/pkg/service/cli/cmd/llm/export/flags.go` - Flag definitions

**Help Messages**:
21. `internal/pkg/service/cli/helpmsg/msg/llm/export/short.txt`
22. `internal/pkg/service/cli/helpmsg/msg/llm/export/long.txt`

### Modify (1 file)

23. `internal/pkg/service/cli/cmd/llm/cmd.go` - Add `llmExport.Command(p)` to command list

---

## 10. API Endpoints Required

| Purpose | Endpoint | Priority | Frequency |
|---------|----------|----------|-----------|
| Project metadata | `GET https://connection.STACK.com/v2/storage/tokens/verify` | HIGH | Once per export |
| Buckets & tables | `GET https://connection.STACK.com/v2/storage/buckets?include=tables,columns,metadata` | HIGH | Once per export |
| **Jobs Queue (transformations)** | `GET https://queue.STACK.com/search/jobs` | **PRIMARY** | Once per export |
| Storage API jobs | `GET https://connection.STACK.com/v2/storage/jobs?limit=100` | SECONDARY | Once per export (optional) |
| Table sample | `GET https://connection.STACK.com/v2/storage/tables/{id}/data-preview?limit=N` | MEDIUM | Per table (if enabled) |

**Notes**:
- **STACK** = Your Keboola stack region (e.g., `north-europe.azure`, `us-east-1`, etc.)
- Jobs Queue API (`queue.STACK.com`) is the primary source for transformation execution data
- Storage API jobs (`connection.STACK.com`) are less critical but can be included for completeness
- Check if `keboola-sdk-go` has these methods. If not, implement custom HTTP requests.

---

## 11. Testing Strategy

### Unit Tests (Coverage target: 80%)
- Platform detection (all platforms)
- Source inference (all patterns)
- Documentation fields injection
- Table reference parsing
- Lineage edge creation
- Job linking logic
- Secret encryption
- Public repo detection

### Integration Tests (Mocked API)
- Full export with mocked responses
- Export with samples enabled/disabled
- Export to custom directory
- API error handling
- Empty project handling
- Large project (100+ tables)

### E2E Tests (Real Project)
- Export after `llm init`
- Verify all files created
- Verify JSON structure correctness
- Verify JSONL graph format
- Verify samples CSV format
- Verify AI guide content

### Manual Testing Checklist
- Interactive mode prompts
- Non-interactive mode (CI/CD)
- Public repo (samples auto-disabled)
- Private repo (samples enabled)
- Progress reporting visible
- Error messages user-friendly
- Help text clear and accurate

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

## 15. Open Questions & Decisions

### Q1: SDK Support for Jobs API
**Status**: TBD
**Action**: Check if `keboola-sdk-go` has Jobs Queue and Storage API endpoints. If not, implement custom HTTP requests.

### Q2: Retry Logic Location
**Status**: TBD
**Action**: Check if SDK has built-in retry. If not, implement in fetcher with exponential backoff.

### Q3: Progress Reporting Style
**Status**: TBD
**Decision**: Use existing CLI patterns (spinner or progress bar). Look at `dbt/generate` for reference.

### Q4: Job Storage Retention
**Status**: Decided
**Decision**: Keep last 100 jobs in `jobs/recent/`. No auto-cleanup.

### Q5: Sample Export Default
**Status**: Decided
**Decision**: Auto-detect (enabled for private repos, disabled for public). User can override with flags.

---

## 16. Risk Assessment

### High Risk
- **API Rate Limits**: Large projects may hit rate limits
  - *Mitigation*: Implement retry with backoff, configurable limits

### Medium Risk
- **SDK Missing Endpoints**: Jobs or preview APIs may not exist in SDK
  - *Mitigation*: Implement custom HTTP requests, contribute back to SDK

- **Large Projects**: 1000+ tables could be slow
  - *Mitigation*: Parallel fetching, progress reporting, configurable limits

### Low Risk
- **Platform Detection**: May miss some transformation types
  - *Mitigation*: Comprehensive pattern list, log warnings for unknown

- **Public Repo Detection**: Heuristic may be inaccurate
  - *Mitigation*: Explicit `--public-repo` flag as fallback

---

## 17. Next Steps

1. **Approval**: Review and approve this RFC
2. **Sprint Planning**: Allocate 5 weeks (one phase per week)
3. **Setup**: Create package structure skeleton
4. **Phase 1 Start**: Begin with fetcher implementation
5. **Weekly Reviews**: Check progress against phase deliverables

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
