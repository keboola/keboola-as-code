# 🎯 START HERE - Template Documentation Index

**Welcome, Service Developer / Coding Agent!**

This template is the **complete specification** for the Keboola Git Sync Twin Format.  
Everything you need to implement the service is documented here.

---

## 📖 Documentation Structure

### 1. 🚀 Quick Start
**File:** `README.md`
- Project overview
- Structure explanation
- Analysis tips for AI agents

### 2. 🏗️ Generation Guide (MOST IMPORTANT)
**File:** `GENERATION-GUIDE.md` (32KB)
- **Complete generation logic for every file**
- API endpoints with exact URLs
- Python pseudocode for all functions
- Security requirements
- Update frequency guidelines
- Complete generation flow

👉 **START HERE for implementation**

### 3. 📋 Gap Analysis
**File:** `GAP-ANALYSIS.md`
- PRD requirements comparison
- What's implemented vs missing
- Priority roadmap
- Testing plan

### 4. 🔧 Platform Guide
**File:** `PLATFORM-CLASSIFICATION.md`
- Why platform detection matters
- All supported platforms (Snowflake, Python, dbt, etc.)
- Detection rules
- Common issues

### 5. ✅ Completion Summary
**File:** `TEMPLATE-COMPLETE.md`
- 100% PRD coverage confirmation
- Real data integration proof
- Security implementation
- Statistics

### 6. 📝 Documentation Summary
**File:** `DOCUMENTATION-COMPLETE.md`
- What documentation was added
- How to use it
- Coding agent prompt templates

---

## 🔍 How to Find Information

### "How do I generate file X?"

1. **Check the file itself** - All JSON files have inline comments:
   ```json
   {
     "_comment": "GENERATION: API endpoint here",
     "_purpose": "Why this file exists",
     "_update_frequency": "When to regenerate"
   }
   ```

2. **Check GENERATION-GUIDE.md** - Detailed section for each directory

3. **Check .README files** - For non-JSON formats (graph.jsonl, sample.csv)

### "What API endpoint do I need?"

**Quick Reference:**
```
Project info:     GET /v2/storage/tokens/verify
Buckets:          GET /v2/storage/buckets
Tables:           GET /v2/storage/tables/{id}?include=columns,metadata
Table samples:    GET /v2/storage/tables/{id}/data-preview?limit=100
Components:       GET /v2/storage
Component configs: GET /v2/storage/components/{id}/configs
Jobs:             GET /v2/storage/jobs?limit=100
```

**Full list:** See `GENERATION-GUIDE.md` → "API Endpoints Summary"

### "What's the generation order?"

**See GENERATION-GUIDE.md → "Complete Generation Flow"**

Quick order:
```
1. Project metadata
2. Buckets & tables
3. Transformations
4. Lineage graph
5. Indices
6. Samples (if enabled)
7. Jobs
8. Manifests
9. AI docs
```

### "How do I handle secrets?"

**See GENERATION-GUIDE.md → "Security Considerations"**

Quick rule:
```python
if field.startswith('#'):
    value = '***ENCRYPTED***'
```

### "When do I disable samples?"

```python
if repo.is_public:
    export_samples = False  # ALWAYS
```

**See:** `manifest.yaml` → security section

---

## 📁 Directory-by-Directory Guide

### Root Level
```
manifest.yaml              → See GENERATION-GUIDE.md "Root Files"
manifest-extended.json     → See GENERATION-GUIDE.md "Root Files"  
README.md                  → Auto-generated
```

### buckets/
```
index.json                 → See GENERATION-GUIDE.md "Buckets Directory"
{bucket}/tables/{table}/
  metadata.json            → Check inline _comment field
```

### transformations/
```
index.json                 → Check inline _comment field
{transform}/
  metadata.json            → Check inline _comment field
```

### components/
```
index.json                 → Check inline _comment field
extractors/{id}/{config}/
  metadata.json            → Check inline _comment field
writers/{id}/{config}/
  metadata.json            → Check inline _comment field
orchestrators/{id}/{config}/
  metadata.json            → Check inline _comment field
```

### storage/samples/
```
index.json                 → Check inline _comment field
{bucket}/{table}/
  sample.csv               → See sample.csv.README
  metadata.json            → Check inline _comment field
```

### jobs/
```
index.json                 → Check inline _comment field
recent/{job_id}.json       → Check inline _comment field
by-component/{id}/{config}/
  latest.json              → Check inline _comment field
```

### indices/
```
graph.jsonl                → See graph.jsonl.README
sources.json               → Check inline _comment field
queries/
  *.json                   → See queries/README.md
```

### ai/
```
README.md                  → See GENERATION-GUIDE.md "AI Directory"
```

---

## 🎯 Quick Answers

**Q: I'm a coding agent. Where do I start?**
A: Read `GENERATION-GUIDE.md` → "Complete Generation Flow" section

**Q: What API token do I need?**
A: Storage API token from project admin (X-StorageApi-Token header)

**Q: How do I test my implementation?**
A: See `GAP-ANALYSIS.md` → "Testing Plan" section

**Q: What if I generate unknown platform transformations?**
A: See `PLATFORM-CLASSIFICATION.md` for detection rules

**Q: Do I need to implement everything?**
A: Core = buckets, transformations, graph, manifests  
Optional = samples, jobs (but strongly recommended)

**Q: How do I know if I'm done?**
A: Compare your output with this template structure (all files should exist)

---

## 🔐 Security Reminders

**CRITICAL - Always enforce:**
1. ✅ Public repos → NO samples (override admin)
2. ✅ Fields starting `#` → Encrypt to `***ENCRYPTED***`
3. ✅ Never store Git credentials in files
4. ✅ Filter sensitive data from job results

**See:** `GENERATION-GUIDE.md` → "Security Considerations"

---

## 📊 Template Statistics

- **Total files:** 30+
- **JSON files:** 19 (all documented with inline comments)
- **Markdown files:** 9 (guides, READMEs)
- **Documentation:** 35KB+ of instructions
- **Coverage:** 100% of PRD requirements
- **API queries tested:** 5 endpoints verified with real data

---

## ✅ You're Ready When...

- [ ] You've read `GENERATION-GUIDE.md`
- [ ] You understand the API endpoints
- [ ] You know the generation order
- [ ] You understand security rules
- [ ] You can generate all required files
- [ ] Your output matches this template structure

---

## 🚀 Go Build!

**Everything is documented. Follow the template exactly for PRD compliance.**

Good luck! 🎉

---

**Template Version:** 2.0 (Fully Documented)
**Last Updated:** 2025-11-24
**Documentation Status:** ✅ Complete
**Ready for:** Service implementation
