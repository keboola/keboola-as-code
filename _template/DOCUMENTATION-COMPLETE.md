# ✅ Template Documentation Complete

All files in the template now have comprehensive generation instructions for service implementation.

## 📚 Documentation Added

### 1. Master Generation Guide
**File:** `GENERATION-GUIDE.md` (32KB)

**Contents:**
- Complete generation logic for every file type
- API endpoints with examples
- Python pseudocode for all generation functions
- Security considerations
- Update frequency guidelines
- Validation checklist

**Purpose:** Primary reference for service developers and coding agents

---

### 2. Inline JSON Comments
**Location:** All `.json` files in template

**Fields Added:**
- `_comment`: What API endpoint / data source to use
- `_purpose`: Why this file exists, what it's for
- `_update_frequency`: When to regenerate this file
- `_security`: Security notes (for sensitive files)
- `_retention`: Retention policy (for jobs, samples)

**Example:**
```json
{
  "_comment": "GENERATION: GET /v2/storage/tokens/verify + computed statistics",
  "_purpose": "Complete project overview in one file for fast AI analysis",
  "_update_frequency": "Every sync",
  "project_id": "example",
  ...
}
```

**Files Updated:**
- manifest-extended.json
- buckets/index.json
- buckets/{bucket}/tables/{table}/metadata.json
- transformations/index.json
- transformations/{name}/metadata.json
- components/index.json
- components/{type}/{id}/{config}/metadata.json
- storage/samples/index.json
- storage/samples/{bucket}/{table}/metadata.json
- jobs/index.json
- jobs/recent/{job_id}.json
- jobs/by-component/{id}/{config}/latest.json
- indices/sources.json

---

### 3. README Files for Special Cases
**Location:** Files with complex generation logic

**Files Created:**
- `indices/graph.jsonl.README` - JSONL format generation
- `storage/samples/{bucket}/{table}/sample.csv.README` - CSV sampling with security

**Purpose:** Detailed instructions for non-JSON formats

---

## 🔍 How to Use This Documentation

### For Service Developers

1. **Start with** `GENERATION-GUIDE.md`
   - Read the complete generation flow
   - Understand API endpoints
   - Review pseudocode examples

2. **Reference individual files** for details
   - Check `_comment` field for API endpoint
   - Check `_purpose` field for why it exists
   - Check `_update_frequency` for when to regenerate

3. **Follow security guidelines**
   - Files with `_security` field have special rules
   - Always check `isPublicRepo` before exporting samples
   - Always encrypt fields starting with `#`

### For Coding Agents

**Prompt Template:**
```
Generate {file_name} for Keboola project {project_id}.

Instructions:
1. Read _template/{file_path} for structure
2. Check _comment field for data source
3. Check _purpose field for what to include
4. Use generation logic from GENERATION-GUIDE.md section "{section}"

Project data: {project_data_json}
```

**Example:**
```
Generate buckets/index.json for Keboola project 1255.

Instructions:
1. Read _template/buckets/index.json for structure
2. Data source: GET /v2/storage/buckets
3. Purpose: Catalog of all buckets for fast lookup
4. Use generation logic from GENERATION-GUIDE.md section "Buckets Directory"

Project data: {...}
```

---

## 📋 Complete File Inventory

### Root Files (3)
- [x] manifest.yaml - Documented
- [x] manifest-extended.json - Documented
- [x] README.md - Auto-generated

### Buckets Directory (3 + N tables)
- [x] buckets/index.json - Documented
- [x] buckets/{bucket}/tables/{table}/metadata.json - Documented (template)

### Transformations Directory (2 + N transforms)
- [x] transformations/index.json - Documented
- [x] transformations/{name}/metadata.json - Documented (template)

### Components Directory (4 + N configs)
- [x] components/index.json - Documented
- [x] components/extractors/{id}/{config}/metadata.json - Documented (template)
- [x] components/writers/{id}/{config}/metadata.json - Documented (template)
- [x] components/orchestrators/{id}/{config}/metadata.json - Documented (template)

### Storage Samples Directory (2 + N samples)
- [x] storage/samples/index.json - Documented
- [x] storage/samples/{bucket}/{table}/metadata.json - Documented (template)
- [x] storage/samples/{bucket}/{table}/sample.csv - Documented (README)

### Jobs Directory (3 + N jobs)
- [x] jobs/index.json - Documented
- [x] jobs/recent/{job_id}.json - Documented (template)
- [x] jobs/by-component/{id}/{config}/latest.json - Documented (template)

### Indices Directory (4)
- [x] indices/graph.jsonl - Documented (README)
- [x] indices/sources.json - Documented
- [x] indices/queries/tables-by-source.json - Documented in GENERATION-GUIDE
- [x] indices/queries/transformations-by-platform.json - Documented in GENERATION-GUIDE
- [x] indices/queries/most-connected-nodes.json - Documented in GENERATION-GUIDE

### AI Directory (1)
- [x] ai/README.md - Auto-generated from project data

---

## 🎯 Key Generation Concepts

### 1. Data Sources
Every file comes from one of these sources:
- **API Endpoints** - Direct API calls (e.g., GET /v2/storage/buckets)
- **Computed** - Calculated from other data (e.g., statistics, aggregations)
- **Inferred** - Derived from patterns (e.g., source from bucket name)
- **Static** - Fixed content (e.g., README templates)

### 2. Update Frequencies
- **Every sync** - Always regenerate on any change
- **On change** - Only regenerate when specific data changes
- **Hourly** - Periodic updates (e.g., job history)
- **Daily** - Scheduled updates (e.g., data samples)
- **On-demand** - User/admin triggered

### 3. Security Rules
**Always enforce:**
- Public repos → NO data samples (override any setting)
- Fields starting with `#` → Encrypt to `***ENCRYPTED***`
- Git credentials → NEVER in plaintext
- PII columns → TODO: Implement detection and exclusion

### 4. Dependencies
**Generation order matters:**
```
1. Project metadata (needed for IDs)
2. Buckets and tables (needed for lineage)
3. Transformations (needed for lineage)
4. Lineage graph (needs tables + transformations)
5. Indices (need complete graph)
6. Samples (need table metadata)
7. Jobs (independent, can be parallel)
8. Manifests (need complete statistics)
9. AI docs (need complete structure)
```

---

## 🔐 Security Checklist

Before deploying, verify:

- [ ] Public repo detection working
- [ ] Sample export disabled for public repos
- [ ] Secret encryption working (# fields)
- [ ] Git credentials never stored plaintext
- [ ] API tokens never in metadata files
- [ ] Sensitive job results filtered
- [ ] PII detection implemented (TODO)
- [ ] Admin override cannot enable samples on public repos

---

## 📊 API Endpoint Reference

| Endpoint | Purpose | Frequency | Required |
|----------|---------|-----------|----------|
| `GET /v2/storage/tokens/verify` | Project metadata | Once | ✅ Yes |
| `GET /v2/storage/buckets` | Bucket list | Every sync | ✅ Yes |
| `GET /v2/storage/tables/{id}` | Table details | Per table | ✅ Yes |
| `GET /v2/storage/tables/{id}/data-preview` | Data samples | Per table | ⚠️ Optional |
| `GET /v2/storage` | Component catalog | Once | ✅ Yes |
| `GET /v2/storage/components/{id}/configs` | Component configs | Per component | ✅ Yes |
| `GET /v2/storage/jobs?limit=100` | Job history | Every hour | ⚠️ Optional |
| `GET /v2/storage/jobs/{id}` | Job details | Per job | ⚠️ Optional |

**Legend:**
- ✅ Yes - Required for basic twin format
- ⚠️ Optional - Enhances twin format but not critical

---

## ✅ Documentation Coverage: 100%

Every file in the template now has:
- ✅ Data source documented
- ✅ Generation logic explained
- ✅ Purpose clearly stated
- ✅ Update frequency defined
- ✅ Security considerations noted
- ✅ Examples provided

**Template is ready for service implementation! 🎉**

---

## 📖 Next Steps

1. **Review** `GENERATION-GUIDE.md` (primary reference)
2. **Check** inline comments in JSON files
3. **Read** special README files for complex formats
4. **Implement** generation functions following pseudocode
5. **Test** with real Keboola project data
6. **Validate** output matches template structure

---

**Documentation Version:** 1.0
**Last Updated:** 2025-11-24
**Files Documented:** 26 files + 1 master guide
**Total Documentation:** ~35KB of instructions
