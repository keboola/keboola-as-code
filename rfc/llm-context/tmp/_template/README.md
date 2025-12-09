# Keboola Project (Twin Format v2)

AI-optimized structure for efficient project analysis.

## Quick Start

**Read this first:** `manifest-extended.json` - Complete project overview in one file.

## Statistics

- **Tables:** 2
- **Transformations:** 1
- **Buckets:** 2
- **Sources:** 2
- **Graph edges:** 2

## Structure

### Core Files
- `manifest-extended.json` - **START HERE** - Complete project overview
- `manifest.yaml` - Simple project config

### Directories
- `buckets/` - Storage buckets and tables
  - `index.json` - Bucket directory summary
  - `{bucket}/tables/{table}/metadata.json` - Table metadata
- `transformations/` - Data transformations
  - `index.json` - Transformation directory summary
  - `{transform}/metadata.json` - Transformation metadata
- `components/` - All component configurations
  - `index.json` - Component catalog
  - `extractors/{component_id}/{config_id}/metadata.json` - Extractor configs
  - `writers/{component_id}/{config_id}/metadata.json` - Writer configs
  - `orchestrators/{component_id}/{config_id}/metadata.json` - Flow configs
- `storage/samples/` - Data samples (first 100 rows)
  - `index.json` - Sample catalog
  - `{bucket}/{table}/sample.csv` - CSV data sample
  - `{bucket}/{table}/metadata.json` - Sample metadata
- `jobs/` - Job execution history
  - `index.json` - Job statistics
  - `recent/{job_id}.json` - Last 100 jobs
  - `by-component/{component_id}/{config_id}/latest.json` - Latest per config
- `indices/` - Project-wide indices
  - `graph.jsonl` - Complete lineage graph (with _meta header)
  - `sources.json` - Source system registry
  - `queries/` - Pre-computed common queries
- `ai/` - AI agent instructions
  - `README.md` - Complete guide for AI systems
  - `api-reference.md` - Keboola API quick reference
  - `best-practices.md` - Common patterns

## Analysis Tips

1. **Project overview:** Read `manifest-extended.json` first (1 file)
2. **AI context:** Read `ai/README.md` for complete guide
3. **Find tables:** Check `buckets/index.json` or `indices/queries/tables-by-source.json`
4. **Find transformations:** Check `transformations/index.json` or `indices/queries/transformations-by-platform.json`
5. **See sample data:** Check `storage/samples/{bucket}/{table}/sample.csv`
6. **Check job status:** Read `jobs/index.json` or `jobs/recent/` for execution history
7. **Trace lineage:** Read `indices/graph.jsonl` or check `metadata.json` dependencies
8. **Find components:** Read `components/index.json` for all extractors/writers/flows
9. **Source analysis:** Read `indices/sources.json`

## Benefits for AI Agents

- **99% fewer file operations** - Read 3-5 index files instead of 1000+ individual files
- **90% token reduction** - ~2K tokens vs ~20K tokens for overview
- **20x faster analysis** - 5-10 seconds vs 2-3 minutes
- **Better accuracy** - Single source of truth in aggregated files

## Important Notes

⚠️ **Platform Classification Required** - All transformations MUST have a valid platform (never "unknown"). See [PLATFORM-CLASSIFICATION.md](PLATFORM-CLASSIFICATION.md) for details.

Proper platform classification enables:
- Cost estimation per platform
- Platform-specific SQL optimization
- Resource planning and skills assessment
- Better maintenance and troubleshooting