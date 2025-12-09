# 🤖 Coding Agent Prompt: Generate Keboola Twin Format

## Objective

Generate a complete **twin format** directory structure for a Keboola project by following the specifications in `_template/` and using data from the current repository and Keboola Storage API.

---

## Context

You are implementing a new data format for Keboola projects that makes them easier for AI agents to understand and analyze. This format transforms component-based configurations into a semantic, data-centric structure.

**Key Principles:**
1. **Data-centric** - Organize by buckets, tables, transformations (not by component types)
2. **AI-optimized** - Include aggregated indices for fast lookups
3. **Self-documenting** - Every JSON file has `_comment`, `_purpose`, `_update_frequency` fields
4. **Secure** - Encrypt secrets, respect public repo safety
5. **Complete** - Include lineage, job history, samples, AI guide

---

## Available Resources

### 1. Template Specification (`_template/`)
**START HERE:** Read these files in order:

1. **`_template/00-START-HERE.md`** - Navigation index and quick reference
2. **`_template/GENERATION-GUIDE.md`** - Complete implementation specification (32KB)
   - Generation logic for every file type
   - API endpoints with examples
   - Python pseudocode for all functions
   - Security requirements
   - Update frequencies
   - Complete generation flow

3. **`_template/manifest-extended.json`** - Example with inline `_comment` fields
4. **`_template/buckets/index.json`** - Example bucket catalog
5. **`_template/transformations/index.json`** - Example transformation catalog
6. **`_template/jobs/index.json`** - Example job history
7. **`_template/storage/samples/index.json`** - Example sample catalog
8. **`_template/ai/README.md`** - Example AI guide

**All JSON files in _template/ have `_comment`, `_purpose`, `_update_frequency` fields showing exactly how to generate them.**

### 2. API Documentation
**File:** `keboola-storage-api-docs.md`
- Keboola Storage API reference
- Endpoint documentation
- Authentication details
- Response formats

### 3. API Token
**File:** `.env.local`
- Contains: `KBC_STORAGE_API_TOKEN`
- Use for: Authentication (X-StorageApi-Token header)
- **CRITICAL:** READ ONLY - Do not modify project data

### 4. Current Project Data
**Directory:** `main/`
- Current component-based format
- Transformation configurations in `main/transformation/`
- Component configs in `main/extractor/`, `main/writer/`, etc.
- Branch metadata in `main/meta.json`

### 5. Project Configuration
**File:** `.keboola/project.json`
- Project ID, region, features
- Default branch information

---

## Task: Generate Complete Twin Format

### Output Directory
Create: `twin_format/`

### Required Structure

```
twin_format/
├── manifest.yaml                    # Simple config with security
├── manifest-extended.json           # Complete project overview
├── README.md                        # Project documentation
│
├── buckets/
│   ├── index.json                   # All buckets catalog
│   └── {bucket}/tables/{table}/
│       └── metadata.json            # Table schema + dependencies
│
├── transformations/
│   ├── index.json                   # All transformations catalog
│   └── {transform_name}/
│       └── metadata.json            # Transform config + dependencies
│
├── components/
│   ├── index.json                   # All components catalog
│   ├── extractors/{component_id}/{config_id}/metadata.json
│   ├── writers/{component_id}/{config_id}/metadata.json
│   └── orchestrators/{component_id}/{config_id}/metadata.json
│
├── storage/samples/
│   ├── index.json                   # Sample catalog
│   └── {bucket}/{table}/
│       ├── sample.csv               # First 100 rows
│       └── metadata.json            # Sample timestamp
│
├── jobs/
│   ├── index.json                   # Job statistics
│   ├── recent/{job_id}.json        # Last 100 jobs
│   └── by-component/{comp_id}/{config_id}/latest.json
│
├── indices/
│   ├── graph.jsonl                  # Lineage graph (with _meta)
│   ├── sources.json                 # Source registry
│   └── queries/
│       ├── tables-by-source.json
│       ├── transformations-by-platform.json
│       └── most-connected-nodes.json
│
└── ai/
    └── README.md                    # AI agent guide
```

---

## Step-by-Step Instructions

### Step 1: Read Documentation (30 minutes)

**Must Read (in order):**
1. `_template/00-START-HERE.md` - Overview and navigation
2. `_template/GENERATION-GUIDE.md` - Complete specification
3. `_template/GAP-ANALYSIS.md` - PRD requirements
4. `_template/PLATFORM-CLASSIFICATION.md` - Platform detection rules
5. `keboola-storage-api-docs.md` - API reference

**What to understand:**
- Generation order (project → buckets → transformations → graph → indices)
- API endpoints for each data type
- Security rules (public repos, secret encryption)
- Platform detection patterns
- Inline documentation requirements

### Step 2: Set Up API Access

```python
# Load API token
with open('.env.local') as f:
    for line in f:
        if 'KBC_STORAGE_API_TOKEN' in line:
            api_token = line.split('=')[1].strip().strip('"')

# Load project config
with open('.keboola/project.json') as f:
    project_config = json.load(f)
    project_id = project_config['project']['id']
    api_host = project_config['project']['apiHost']

# Set up API client
api_base = f"https://{api_host}"
headers = {"X-StorageApi-Token": api_token}
```

### Step 3: Fetch API Data

**Required API Calls:**
```python
# 1. Project metadata
project_data = requests.get(
    f"{api_base}/v2/storage/tokens/verify",
    headers=headers
).json()

# 2. Buckets list
buckets = requests.get(
    f"{api_base}/v2/storage/buckets",
    headers=headers
).json()

# 3. Jobs history
jobs = requests.get(
    f"{api_base}/v2/storage/jobs?limit=100",
    headers=headers
).json()

# 4. Components catalog
components = requests.get(
    f"{api_base}/v2/storage?exclude=componentDetails",
    headers=headers
).json()

# 5. Table samples (optional, for selected tables)
sample_csv = requests.get(
    f"{api_base}/v2/storage/tables/{{table_id}}/data-preview?limit=100",
    headers=headers
).text
```

**See:** `_template/GENERATION-GUIDE.md` → "API Endpoints Summary" for complete list

### Step 4: Scan Local Data

**Scan transformations from `main/` directory:**
```python
# Find all transformation configs
for config_file in Path('main/transformation').rglob('config.json'):
    # Skip variables
    if 'variables' in str(config_file.parent):
        continue

    # Read meta.json, config.json, description.md
    # Extract storage mappings (input/output tables)
    # Detect platform from component_id
    # Build transformation metadata
```

**Platform Detection:**
See `_template/PLATFORM-CLASSIFICATION.md` for complete rules.

```python
def detect_platform(component_id):
    cid = component_id.lower()
    if 'snowflake' in cid: return 'snowflake'
    if 'python' in cid: return 'python'
    if 'dbt' in cid: return 'dbt'
    if 'duckdb' in cid: return 'duckdb'
    if '.r-transformation' in cid: return 'r'
    # ... see PLATFORM-CLASSIFICATION.md for full list
    return 'unknown'  # Should be 0 at the end
```

### Step 5: Build Lineage Graph

**From transformation storage mappings:**
```python
# For each transformation:
for inp in storage['input']['tables']:
    source_table = inp['source']  # e.g., "in.c-bucket.table"
    # Create edge: table → transformation
    edges.append({
        'source': 'table:bucket/table',
        'target': 'transform:name',
        'type': 'consumed_by'
    })

for out in storage['output']['tables']:
    dest_table = out['destination']
    # Create edge: transformation → table
    edges.append({
        'source': 'transform:name',
        'target': 'table:bucket/table',
        'type': 'produces'
    })
```

### Step 6: Generate Files with Documentation

**CRITICAL:** Every JSON file must have generation metadata:

```python
def add_doc_fields(data, comment, purpose, update_freq, **kwargs):
    """Add generation documentation to JSON"""
    result = {
        '_comment': f"GENERATION: {comment}",
        '_purpose': purpose,
        '_update_frequency': update_freq
    }
    if 'security' in kwargs:
        result['_security'] = kwargs['security']
    if 'retention' in kwargs:
        result['_retention'] = kwargs['retention']
    result.update(data)
    return result

# Example usage:
manifest = add_doc_fields(
    {'project_id': 1255, 'twin_version': 1, ...},
    comment="GET /v2/storage/tokens/verify + computed statistics",
    purpose="Complete project overview in one file for fast AI analysis",
    update_freq="Every sync"
)
```

**See each file in `_template/` for exact `_comment`, `_purpose`, `_update_frequency` values to use.**

### Step 7: Implement Security

**Critical Security Rules:**

```python
# 1. Secret encryption
def encrypt_secrets(config):
    for key, value in config.items():
        if key.startswith('#'):
            config[key] = '***ENCRYPTED***'
    return config

# 2. Public repo safety
is_public_repo = False  # TODO: Get from Git repo config
if is_public_repo:
    export_samples = False  # ALWAYS override
else:
    export_samples = True  # Or from admin config

# 3. Sample security
if is_public_repo or not export_samples:
    # Don't create storage/samples/ files
    pass
```

**See:** `_template/GENERATION-GUIDE.md` → "Security Considerations"

### Step 8: Generate All Directories

**Follow this order (dependencies matter):**

1. Fetch API data (project, buckets, jobs)
2. Scan local transformations
3. Build lineage graph
4. Generate `buckets/` with all table metadata
5. Generate `transformations/` with all transform metadata
6. Generate `components/` with component configs
7. Generate `storage/samples/` (if security allows)
8. Generate `jobs/` with execution history
9. Generate `indices/` (graph, sources, queries)
10. Generate `ai/README.md` with real project data
11. Generate root files (manifest.yaml, manifest-extended.json, README.md)

**See:** `_template/GENERATION-GUIDE.md` → "Complete Generation Flow"

### Step 9: Validate Output

**Checklist:**
```bash
# Structure validation
test -f twin_format/manifest-extended.json && echo "✅ Manifest"
test -f twin_format/ai/README.md && echo "✅ AI guide"
test -f twin_format/jobs/index.json && echo "✅ Jobs"
test -d twin_format/storage/samples && echo "✅ Samples dir"
test -f twin_format/indices/graph.jsonl && echo "✅ Graph"

# Documentation validation
grep -r "_comment" twin_format/*.json && echo "✅ Inline docs"

# Data quality validation
cat twin_format/transformations/index.json | jq '.transformations[] | select(.platform == "unknown")' | wc -l
# Should output: 0 (zero unknown platforms)

# Security validation
grep "encryptSecrets" twin_format/manifest.yaml && echo "✅ Security config"
```

**See:** `_template/GENERATION-GUIDE.md` → "Validation Checklist"

---

## Success Criteria

### Required Outputs

- [x] `twin_format/` directory created
- [x] All directories from template present
- [x] All JSON files have `_comment`, `_purpose`, `_update_frequency` fields
- [x] manifest-extended.json has real project statistics
- [x] buckets/index.json catalogs all buckets
- [x] transformations/index.json catalogs all transformations
- [x] jobs/ directory with 100 recent jobs
- [x] ai/README.md with real project data (ID 1255, stats, etc.)
- [x] indices/graph.jsonl with _meta header
- [x] Security config in manifest.yaml
- [x] 0 transformations with platform: "unknown"

### Data Quality

- [x] All transformation platforms detected correctly
- [x] All table dependencies computed from storage mappings
- [x] Graph edges show correct relationships (consumed_by/produces)
- [x] Sources inferred from bucket names
- [x] Job statistics aggregated correctly
- [x] Latest job per component computed

### Security Compliance

- [x] Fields starting with `#` encrypted to `***ENCRYPTED***`
- [x] Public repo check prevents sample export
- [x] No plaintext secrets in output
- [x] Security config in manifest.yaml

---

## Important Notes

### Security - READ ONLY

**CRITICAL:** You have READ ONLY access to the Keboola project.
- ✅ You CAN: Query API endpoints (GET requests)
- ❌ You CANNOT: Modify project data, create tables, run jobs
- ✅ You CAN: Read local files in `main/` directory
- ❌ You CANNOT: Modify files in `main/` or `_template/`

### API Rate Limits

**Be efficient:**
- Use `_template/GENERATION-GUIDE.md` to understand what to query
- Don't fetch table samples for all 458 tables (limit to ~10 for testing)
- Batch requests where possible
- Cache API responses

### Error Handling

**Some tables may not exist anymore:**
- Table previews may return 404 (table was deleted)
- Handle gracefully - skip that sample, continue with others
- Log warnings but don't fail entire generation

### Platform Detection

**Target: 0 unknown platforms**
- Use comprehensive detection from `_template/PLATFORM-CLASSIFICATION.md`
- Check for: snowflake, python, dbt, duckdb, r, redshift, synapse, oracle, etc.
- Filter out "variables" directories (not real transformations)

---

## Detailed Instructions

### Phase 1: Setup & Data Collection (30 min)

```python
import json
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter

# 1. Load credentials
with open('.env.local') as f:
    token = [line.split('=')[1].strip().strip('"')
             for line in f if 'KBC_STORAGE_API_TOKEN' in line][0]

# 2. Load project config
with open('.keboola/project.json') as f:
    project = json.load(f)
    project_id = project['project']['id']
    api_host = project['project']['apiHost']

# 3. Setup API client
api_base = f"https://{api_host}"
headers = {"X-StorageApi-Token": token}

# 4. Fetch API data (following GENERATION-GUIDE.md)
project_data = requests.get(f"{api_base}/v2/storage/tokens/verify", headers=headers).json()
buckets = requests.get(f"{api_base}/v2/storage/buckets", headers=headers).json()
jobs = requests.get(f"{api_base}/v2/storage/jobs?limit=100", headers=headers).json()
```

### Phase 2: Scan Local Transformations (20 min)

```python
# Scan main/transformation/ directory
# For each config.json (excluding variables/):
#   - Read meta.json (name, isDisabled)
#   - Read config.json (storage mappings)
#   - Read description.md
#   - Detect platform from component_id
#   - Extract input/output tables
#   - Build transformation metadata

# See: _template/GENERATION-GUIDE.md → "Transformations Directory"
```

### Phase 3: Build Data Structures (30 min)

```python
# 1. Build tables dictionary from transformation references
# 2. Build graph edges (table→transform, transform→table)
# 3. Infer sources from bucket names
# 4. Group buckets, transformations, components
# 5. Compute statistics

# See: _template/GENERATION-GUIDE.md → "Complete Generation Flow"
```

### Phase 4: Generate Files (60 min)

**For each file type, read the corresponding _template/ file to see:**
- `_comment` - What API endpoint or data source
- `_purpose` - What to include
- Structure - What fields are required

**Then use GENERATION-GUIDE.md for the generation function.**

**Example for buckets/index.json:**
1. Read: `_template/buckets/index.json`
2. See: `_comment` says "GET /v2/storage/buckets - aggregate all buckets"
3. Follow: GENERATION-GUIDE.md → "buckets/index.json" section
4. Generate: Using the pseudocode provided
5. Add: Same `_comment`, `_purpose`, `_update_frequency` fields
6. Write: To `twin_format/buckets/index.json`

**Repeat for all file types.**

### Phase 5: Validation (15 min)

```bash
# 1. Structure check
find twin_format -type d | sort > generated_dirs.txt
find _template -type d | sed 's/_template/twin_format/' | sort > expected_dirs.txt
# Compare: all template dirs should be in generated

# 2. Documentation check
find twin_format -name "*.json" -exec grep -L "_comment" {} \;
# Should output: nothing (all JSON files have _comment)

# 3. Data quality check
cat twin_format/transformations/index.json | jq '.transformations[] | select(.platform == "unknown")'
# Should output: nothing (0 unknown platforms)

# 4. Security check
grep "encryptSecrets" twin_format/manifest.yaml
# Should output: encryptSecrets: true
```

---

## Code Template

**Use this as starting point:**

```python
#!/usr/bin/env python3
"""
Generate Keboola Twin Format from template specification.
Follows instructions in _template/GENERATION-GUIDE.md
"""

import json
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter

class TwinFormatGenerator:
    def __init__(self, api_token, api_base, source_branch='main'):
        self.api_token = api_token
        self.api_base = api_base
        self.source_branch = Path(source_branch)
        self.output_dir = Path('twin_format')
        self.headers = {"X-StorageApi-Token": api_token}

        # Data structures
        self.project_data = None
        self.buckets = []
        self.tables = {}
        self.transformations = {}
        self.jobs = []
        self.graph_edges = []

    def add_doc_fields(self, data, comment, purpose, update_freq, **kwargs):
        """Add generation documentation fields (required for all JSON files)"""
        result = {
            '_comment': f"GENERATION: {comment}",
            '_purpose': purpose,
            '_update_frequency': update_freq
        }
        for key in ['security', 'retention']:
            if key in kwargs:
                result[f'_{key}'] = kwargs[key]
        result.update(data)
        return result

    def api_get(self, endpoint):
        """Make API request"""
        response = requests.get(f"{self.api_base}{endpoint}", headers=self.headers)
        response.raise_for_status()
        return response

    def fetch_api_data(self):
        """Fetch all required API data"""
        print("Fetching API data...")
        self.project_data = self.api_get('/v2/storage/tokens/verify').json()
        self.buckets = self.api_get('/v2/storage/buckets').json()
        self.jobs = self.api_get('/v2/storage/jobs?limit=100').json()

    def scan_transformations(self):
        """Scan local transformation configs"""
        print("Scanning transformations...")
        # See: _template/GENERATION-GUIDE.md → "Transformations Directory"
        # TODO: Implement following the guide

    def build_graph(self):
        """Build lineage graph from transformations"""
        print("Building lineage graph...")
        # See: _template/GENERATION-GUIDE.md → "indices/graph.jsonl"
        # TODO: Implement following the guide

    def generate_buckets(self):
        """Generate buckets/ directory"""
        print("Generating buckets...")
        # See: _template/GENERATION-GUIDE.md → "Buckets Directory"
        # TODO: Implement following the guide

    def generate_transformations(self):
        """Generate transformations/ directory"""
        print("Generating transformations...")
        # See: _template/GENERATION-GUIDE.md → "Transformations Directory"
        # TODO: Implement following the guide

    def generate_jobs(self):
        """Generate jobs/ directory"""
        print("Generating jobs...")
        # See: _template/GENERATION-GUIDE.md → "Jobs Directory"
        # TODO: Implement following the guide

    def generate_samples(self):
        """Generate storage/samples/ directory"""
        print("Generating samples...")
        # Security check
        is_public = False  # TODO: Get from repo config
        if is_public:
            return  # Never export samples to public repos
        # See: _template/GENERATION-GUIDE.md → "Storage Samples Directory"
        # TODO: Implement following the guide

    def generate_indices(self):
        """Generate indices/ directory"""
        print("Generating indices...")
        # See: _template/GENERATION-GUIDE.md → "Indices Directory"
        # TODO: Implement following the guide

    def generate_ai_guide(self):
        """Generate ai/README.md"""
        print("Generating AI guide...")
        # See: _template/GENERATION-GUIDE.md → "AI Directory"
        # Use real project data: self.project_data['owner']
        # TODO: Implement following the guide

    def generate_manifests(self):
        """Generate root manifest files"""
        print("Generating manifests...")
        # See: _template/GENERATION-GUIDE.md → "Root Files"
        # TODO: Implement following the guide

    def validate(self):
        """Validate generated output"""
        print("Validating output...")
        # Check all required files exist
        # Check all JSON have _comment fields
        # Check 0 unknown platforms
        # Check security config

    def run(self):
        """Execute complete generation"""
        print("="*60)
        print("GENERATING TWIN FORMAT FROM TEMPLATE SPECIFICATION")
        print("="*60)

        self.fetch_api_data()
        self.scan_transformations()
        self.build_graph()
        self.generate_buckets()
        self.generate_transformations()
        self.generate_jobs()
        self.generate_samples()
        self.generate_indices()
        self.generate_ai_guide()
        self.generate_manifests()
        self.validate()

        print("\n✅ Twin format generation complete!")

if __name__ == "__main__":
    # Load token
    with open('.env.local') as f:
        for line in f:
            if 'KBC_STORAGE_API_TOKEN' in line:
                token = line.split('=')[1].strip().strip('"')

    # Load project config
    with open('.keboola/project.json') as f:
        project = json.load(f)
        api_host = project['project']['apiHost']

    generator = TwinFormatGenerator(
        api_token=token,
        api_base=f"https://{api_host}",
        source_branch='main'
    )
    generator.run()
```

---

## Expected Output

### Metrics
- **Tables:** ~458
- **Transformations:** ~307
- **Buckets:** ~135
- **Sources:** ~9
- **Jobs:** 100
- **Graph edges:** ~741
- **Total files:** ~800-900

### Platform Distribution
- Snowflake: ~224
- Python: ~61
- dbt: ~10
- R: ~4
- DuckDB: ~1
- Others: ~7
- **Unknown: 0** ✅

### Files with Documentation
- **ALL JSON files** must have `_comment`, `_purpose`, `_update_frequency`
- **~800+ files** with inline documentation

---

## Debugging Tips

### If API calls fail:
- Check token in `.env.local`
- Verify API host in `.keboola/project.json`
- Check network connectivity
- Review `keboola-storage-api-docs.md` for endpoint format

### If platform detection has unknowns:
- Read `_template/PLATFORM-CLASSIFICATION.md`
- Check component_id pattern matching
- Verify filters for variables/ directories

### If validation fails:
- Compare your output with `_template/` structure
- Check each `_comment` field matches template
- Review `_template/GENERATION-GUIDE.md` for that file type

### If confused:
- Re-read `_template/00-START-HERE.md`
- Check specific section in `_template/GENERATION-GUIDE.md`
- Look at example in `_template/` for that file type

---

## Resources Summary

**Must Read:**
1. `_template/00-START-HERE.md` (navigation)
2. `_template/GENERATION-GUIDE.md` (specification)
3. `keboola-storage-api-docs.md` (API reference)

**Reference:**
- `_template/*.json` files (examples with inline docs)
- `_template/PLATFORM-CLASSIFICATION.md` (platform detection)
- `_template/GAP-ANALYSIS.md` (PRD requirements)

**Available Data:**
- `.env.local` (API token)
- `.keboola/project.json` (project config)
- `main/` directory (transformation configs)
- Keboola Storage API (read-only access)

---

## Final Checklist

Before reporting completion:

- [ ] Read `_template/00-START-HERE.md`
- [ ] Read `_template/GENERATION-GUIDE.md` completely
- [ ] Understand API endpoints from `keboola-storage-api-docs.md`
- [ ] Implemented all generation functions
- [ ] All JSON files have documentation fields
- [ ] Security rules enforced
- [ ] Validation passed
- [ ] Output matches template structure
- [ ] 0 unknown platforms
- [ ] ai/README.md has real project data

---

## Time Estimate

- Reading documentation: 30 minutes
- Implementation: 2-3 hours
- Testing & validation: 30 minutes
- **Total: ~3-4 hours**

---

## Expected Final Output

```
✅ Twin format generated successfully!

Statistics:
- Project: Playground (ID 1255)
- Tables: 458
- Transformations: 307 (0 unknown platforms)
- Jobs: 100
- Samples: 0-10 (limited)
- Files: 800+

All files documented with:
- _comment (API endpoint)
- _purpose (why it exists)
- _update_frequency (when to regenerate)

Ready for AI agent testing! 🎉
```

---

**Good luck! Follow the template exactly and you'll generate a PRD-compliant twin format.** 🚀
