# 📊 Twin Format Data Source Mapping

**Purpose:** Complete reference showing where every piece of data comes from and how to obtain it.

**For:** Service developers, coding agents, implementation teams

---

## 🗂️ Table of Contents

1. [Project Metadata](#project-metadata)
2. [Storage Buckets & Tables](#storage-buckets--tables)
3. [Transformations](#transformations)
4. [Components](#components)
5. [Jobs & Execution History](#jobs--execution-history)
6. [Data Samples](#data-samples)
7. [Lineage Graph](#lineage-graph)
8. [Computed Indices](#computed-indices)
9. [AI Instructions](#ai-instructions)

---

## 📋 Project Metadata

### Project ID

**Location:** API - Project info
**Endpoint:** `GET /v2/storage/tokens/verify`
**How to Obtain:**
```python
response = requests.get(
    f"{api_base}/v2/storage/tokens/verify",
    headers={"X-StorageApi-Token": token}
)
project_id = response.json()['owner']['id']
```
**Used In:**
- `manifest.yaml` → `project_id`
- `manifest-extended.json` → `project_id`
- `ai/README.md` → Project overview

---

### Project Name

**Location:** API - Project info
**Endpoint:** `GET /v2/storage/tokens/verify`
**How to Obtain:**
```python
project_name = response.json()['owner']['name']
```
**Used In:**
- `ai/README.md` → Project overview

---

### Project Region

**Location:** API - Project info
**Endpoint:** `GET /v2/storage/tokens/verify`
**How to Obtain:**
```python
region = response.json()['owner']['region']
```
**Used In:**
- `ai/README.md` → Project overview

---

### Project Backend

**Location:** API - Project info
**Endpoint:** `GET /v2/storage/tokens/verify`
**How to Obtain:**
```python
features = response.json()['owner']['features']
# Primary backend is typically in features list
# Look for: 'snowflake', 'redshift', 'bigquery'
```
**Used In:**
- `ai/README.md` → Backend information

---

### API Base URL

**Location:** Local file - Project config
**File:** `.keboola/project.json`
**How to Obtain:**
```python
with open('.keboola/project.json') as f:
    project_config = json.load(f)
    api_host = project_config['project']['apiHost']
    api_base = f"https://{api_host}"
```
**Example:** `https://connection.north-europe.azure.keboola.com`
**Used In:**
- API requests
- `ai/README.md` → API reference

---

### API Token

**Location:** Local file - Environment config
**File:** `.env.local`
**How to Obtain:**
```python
with open('.env.local') as f:
    for line in f:
        if 'KBC_STORAGE_API_TOKEN' in line:
            token = line.split('=')[1].strip().strip('"')
```
**Security:** Never write token to output files
**Used In:**
- API authentication (X-StorageApi-Token header)

---

## 🗄️ Storage Buckets & Tables

### Bucket List

**Location:** API - Storage buckets
**Endpoint:** `GET /v2/storage/buckets`
**How to Obtain:**
```python
response = requests.get(
    f"{api_base}/v2/storage/buckets",
    headers={"X-StorageApi-Token": token}
)
buckets = response.json()
```
**Response Structure:**
```json
[
  {
    "id": "in.c-shopify",
    "name": "shopify",
    "displayName": "Shopify Data",
    "stage": "in",
    "backend": "snowflake",
    "tables": ["in.c-shopify.orders", "in.c-shopify.customers"]
  }
]
```
**Used In:**
- `buckets/index.json` → Bucket catalog
- `manifest-extended.json` → Statistics
- Source inference

---

### Bucket Count

**Location:** Computed from bucket list
**How to Obtain:**
```python
total_buckets = len(buckets)
```
**Used In:**
- `manifest-extended.json` → `statistics.total_buckets`
- `buckets/index.json` → `total_buckets`
- `ai/README.md` → Statistics

---

### Bucket Name (Clean)

**Location:** Computed from bucket ID
**How to Obtain:**
```python
bucket_id = "in.c-shopify"
bucket_clean = bucket_id.replace('in.c-', '').replace('out.c-', '')
# Result: "shopify"
```
**Used In:**
- Directory names in `buckets/{bucket_clean}/`
- Bucket references

---

### Bucket Source

**Location:** Inferred from bucket name
**How to Obtain:**
```python
def infer_source_from_bucket(bucket_name):
    bucket_lower = bucket_name.lower()

    if 'shopify' in bucket_lower: return 'shopify'
    if 'hubspot' in bucket_lower: return 'hubspot'
    if 'ga4' in bucket_lower or 'analytics' in bucket_lower: return 'google-analytics'
    if 'drive' in bucket_lower: return 'google-drive'
    if 'mysql' in bucket_lower: return 'mysql'
    if 'facebook' in bucket_lower: return 'facebook'
    if 'jira' in bucket_lower: return 'jira'
    # ... more patterns
    return 'unknown'
```
**Used In:**
- `buckets/index.json` → `buckets[].source`
- `buckets/{bucket}/tables/{table}/metadata.json` → `source`
- `indices/sources.json`

---

### Table List (per bucket)

**Location:** API - Bucket response
**Endpoint:** `GET /v2/storage/buckets` (includes tables)
**How to Obtain:**
```python
for bucket in buckets:
    table_ids = bucket['tables']  # List of table IDs
    # e.g., ["in.c-shopify.orders", "in.c-shopify.customers"]
```
**Used In:**
- `buckets/index.json` → `buckets[].tables`
- Table metadata generation

---

### Table Details

**Location:** API - Table metadata
**Endpoint:** `GET /v2/storage/tables/{table_id}?include=columns,metadata,columnMetadata`
**How to Obtain:**
```python
table_id = "in.c-shopify.orders"
response = requests.get(
    f"{api_base}/v2/storage/tables/{table_id}?include=columns,metadata",
    headers={"X-StorageApi-Token": token}
)
table_data = response.json()
```
**Response Structure:**
```json
{
  "id": "in.c-shopify.orders",
  "name": "orders",
  "columns": ["id", "customer", "amount", "date"],
  "primaryKey": ["id"],
  "rowsCount": 15420,
  "dataSizeBytes": 2048000,
  "created": "2025-01-15T10:00:00+0100",
  "lastImportDate": "2025-11-24T08:00:00+0100",
  "metadata": [...]
}
```
**Used In:**
- `buckets/{bucket}/tables/{table}/metadata.json`
- Statistics
- Sample metadata

---

### Table Name

**Location:** Parse from table ID
**How to Obtain:**
```python
table_id = "in.c-shopify.orders"
table_name = table_id.split('.')[-1]  # "orders"
```
**Used In:**
- `metadata.json` → `name`
- Directory names

---

### Table Count

**Location:** Computed from all buckets
**How to Obtain:**
```python
total_tables = sum(len(bucket['tables']) for bucket in buckets)
```
**Used In:**
- `manifest-extended.json` → `statistics.total_tables`

---

### Table Dependencies (consumed_by / produced_by)

**Location:** Computed from lineage graph
**How to Obtain:**
```python
# After building graph edges:
consumed_by = [edge['target'] for edge in graph_edges
               if edge['source'] == table_uid and edge['type'] == 'consumed_by']
produced_by = [edge['source'] for edge in graph_edges
               if edge['target'] == table_uid and edge['type'] == 'produces']
```
**Used In:**
- `buckets/{bucket}/tables/{table}/metadata.json` → `dependencies`

---

## 🔄 Transformations

### Transformation Configurations

**Location:** Local files - Component configs
**Directory:** `main/transformation/{component_id}/{config_name}/`
**How to Obtain:**
```python
transform_base = Path('main/transformation')
for config_file in transform_base.rglob('config.json'):
    transform_path = config_file.parent
    transform_name = transform_path.name
    component_id = transform_path.parent.name

    # Skip variables
    if transform_name == 'variables' or 'variables/values' in str(transform_path):
        continue

    # Read files
    with open(config_file) as f:
        config = json.load(f)
```
**Used In:**
- `transformations/{name}/metadata.json`
- Graph building
- Platform detection

---

### Transformation Name

**Location:** Local file - meta.json
**File:** `main/transformation/{component_id}/{config_name}/meta.json`
**How to Obtain:**
```python
meta_file = transform_path / "meta.json"
with open(meta_file) as f:
    meta = json.load(f)
    name = meta.get('name', config_name)  # Fallback to directory name
```
**Used In:**
- `transformations/{name}/metadata.json` → `name`
- `transformations/index.json`

---

### Transformation Platform

**Location:** Inferred from component ID
**How to Obtain:**
```python
def detect_platform(component_id):
    cid = component_id.lower()

    # SQL platforms
    if 'snowflake' in cid: return 'snowflake'
    if 'redshift' in cid: return 'redshift'
    if 'bigquery' in cid: return 'bigquery'
    if 'synapse' in cid: return 'synapse'
    if 'duckdb' in cid: return 'duckdb'
    if 'oracle' in cid: return 'oracle'

    # Programming languages
    if 'python' in cid: return 'python'
    if '.r-transformation' in cid: return 'r'

    # Frameworks
    if 'dbt' in cid: return 'dbt'

    return 'sql'  # Generic SQL fallback
```
**Reference:** `_template/PLATFORM-CLASSIFICATION.md`
**Used In:**
- `transformations/{name}/metadata.json` → `platform`
- `transformations/index.json` → `by_platform`
- `manifest-extended.json` → `transformation_platforms`

---

### Transformation Disabled Status

**Location:** Local file - meta.json
**File:** `main/transformation/{component_id}/{config_name}/meta.json`
**How to Obtain:**
```python
is_disabled = meta.get('isDisabled', False)
```
**Used In:**
- `transformations/{name}/metadata.json` → `is_disabled`
- `transformations/index.json`

---

### Transformation Description

**Location:** Local file - description.md
**File:** `main/transformation/{component_id}/{config_name}/description.md`
**How to Obtain:**
```python
desc_file = transform_path / "description.md"
if desc_file.exists():
    with open(desc_file) as f:
        description = f.read().strip()
else:
    description = ""
```
**Used In:**
- `transformations/{name}/metadata.json` → `description`

---

### Transformation Input Tables

**Location:** Local file - config.json
**File:** `main/transformation/{component_id}/{config_name}/config.json`
**How to Obtain:**
```python
with open(config_file) as f:
    config = json.load(f)
    input_tables = config.get('storage', {}).get('input', {}).get('tables', [])

# Each input has:
for inp in input_tables:
    source = inp['source']  # e.g., "in.c-shopify.orders"
    destination = inp['destination']  # Local CSV name
```
**Used In:**
- Graph edges (table → transformation)
- `transformations/{name}/metadata.json` → `dependencies.consumes`

---

### Transformation Output Tables

**Location:** Local file - config.json
**File:** `main/transformation/{component_id}/{config_name}/config.json`
**How to Obtain:**
```python
output_tables = config.get('storage', {}).get('output', {}).get('tables', [])

for out in output_tables:
    destination = out['destination']  # e.g., "out.c-processed.orders_clean"
    source = out['source']  # Local CSV name
```
**Used In:**
- Graph edges (transformation → table)
- `transformations/{name}/metadata.json` → `dependencies.produces`

---

### Transformation Job Execution Status

**Location:** API - Jobs list + computed mapping
**Endpoint:** `GET /v2/storage/jobs?limit=100`
**How to Obtain:**
```python
# Fetch jobs
response = requests.get(
    f"{api_base}/v2/storage/jobs?limit=100",
    headers={"X-StorageApi-Token": token}
)
jobs = response.json()

# Map jobs to transformations
transformation_jobs = {}
for job in jobs:
    if job['operationName'] == 'transformationRun':
        component_id = job['operationParams'].get('componentId')
        config_id = job['operationParams'].get('configurationId')
        key = f"{component_id}:{config_id}"

        # Keep only the latest job per transformation
        if key not in transformation_jobs or job['endTime'] > transformation_jobs[key]['endTime']:
            transformation_jobs[key] = job

# For each transformation, extract job execution info
for transform in transformations:
    job_key = f"{transform['component_id']}:{transform['name']}"
    latest_job = transformation_jobs.get(job_key)

    if latest_job:
        transform['job_execution'] = {
            'last_run_time': latest_job['endTime'],
            'last_run_status': latest_job['status'],
            'job_reference': str(latest_job['id']),
            'duration_seconds': latest_job.get('durationSeconds', 0),
            'last_error': latest_job.get('error')
        }
```
**IMPORTANT:** Every transformation MUST track job execution status.
**Used In:**
- `transformations/{name}/metadata.json` → `job_execution`
- `transformations/index.json` → `last_run_time`, `last_run_status`, `job_reference`
- `jobs/by-component/{component_id}/{config_id}/latest.json`

---

### Transformation Count

**Location:** Computed from scanned transformations
**How to Obtain:**
```python
total_transformations = len(transformations)
```
**Used In:**
- `manifest-extended.json` → `statistics.total_transformations`
- `transformations/index.json` → `total_transformations`

---

## 🧩 Components

### Component Catalog

**Location:** API - Component list
**Endpoint:** `GET /v2/storage?exclude=componentDetails`
**How to Obtain:**
```python
response = requests.get(
    f"{api_base}/v2/storage?exclude=componentDetails",
    headers={"X-StorageApi-Token": token}
)
components = response.json()['components']
```
**Response Structure:**
```json
{
  "components": [
    {
      "id": "keboola.ex-shopify",
      "name": "Shopify",
      "type": "extractor"
    }
  ]
}
```
**Used In:**
- `components/index.json`
- Source inference
- Component metadata

---

### Component Configurations

**Location:** API - Component configs
**Endpoint:** `GET /v2/storage/components/{component_id}/configs`
**How to Obtain:**
```python
component_id = "keboola.ex-shopify"
response = requests.get(
    f"{api_base}/v2/storage/components/{component_id}/configs",
    headers={"X-StorageApi-Token": token}
)
configs = response.json()
```
**Alternative:** Local files in `main/extractor/`, `main/writer/`, etc.
**Used In:**
- `components/{type}/{component_id}/{config_id}/metadata.json`

---

### Component Configuration Details

**Location:** API - Specific config
**Endpoint:** `GET /v2/storage/components/{component_id}/configs/{config_id}`
**How to Obtain:**
```python
config_id = "12345"
response = requests.get(
    f"{api_base}/v2/storage/components/{component_id}/configs/{config_id}",
    headers={"X-StorageApi-Token": token}
)
config_data = response.json()
```
**Response Structure:**
```json
{
  "id": "12345",
  "name": "Shopify Store",
  "componentId": "keboola.ex-shopify",
  "configuration": {
    "parameters": {
      "shop": "mystore.myshopify.com",
      "#api_key": "secret_value_here"
    }
  },
  "isDisabled": false,
  "created": "2025-01-15T10:00:00Z",
  "version": 5
}
```
**Security:** Encrypt fields starting with `#`
**Used In:**
- `components/{type}/{component_id}/{config_id}/metadata.json`

---

### Component Secrets (Encrypted)

**Location:** Component config (needs encryption)
**How to Obtain:**
```python
def encrypt_secrets(config_params):
    encrypted = {}
    for key, value in config_params.items():
        if key.startswith('#'):
            encrypted[key] = '***ENCRYPTED***'
        elif isinstance(value, dict):
            encrypted[key] = encrypt_secrets(value)
        else:
            encrypted[key] = value
    return encrypted
```
**Security:** NEVER write plaintext secrets
**Used In:**
- `components/{type}/{component_id}/{config_id}/metadata.json` → `configuration`

---

## 📝 Jobs & Execution History

### Jobs List

**Location:** API - Recent jobs
**Endpoint:** `GET /v2/storage/jobs?limit=100`
**How to Obtain:**
```python
response = requests.get(
    f"{api_base}/v2/storage/jobs?limit=100",
    headers={"X-StorageApi-Token": token}
)
jobs = response.json()
```
**Response Structure:**
```json
[
  {
    "id": 55422188,
    "runId": "149879288",
    "status": "success",
    "operationName": "workspaceCreate",
    "operationParams": {
      "componentId": "keboola.sandboxes",
      "configurationId": "abc123",
      "branchId": 1236
    },
    "createdTime": "2025-11-24T11:42:03+0100",
    "startTime": "2025-11-24T11:42:03+0100",
    "endTime": "2025-11-24T11:42:20+0100",
    "metrics": {
      "inBytes": 0,
      "outBytes": 0
    }
  }
]
```
**Used In:**
- `jobs/index.json` → Statistics
- `jobs/recent/{job_id}.json` → Individual jobs
- `jobs/by-component/` → Latest per component

---

### Job Duration

**Location:** Computed from job times
**How to Obtain:**
```python
from datetime import datetime

start = datetime.fromisoformat(job['startTime'].replace('+0100', '+01:00'))
end = datetime.fromisoformat(job['endTime'].replace('+0100', '+01:00'))
duration_seconds = (end - start).total_seconds()
```
**Used In:**
- `jobs/recent/{job_id}.json` → `durationSeconds`

---

### Job Statistics (by status)

**Location:** Computed from jobs list
**How to Obtain:**
```python
from collections import Counter

by_status = Counter(job['status'] for job in jobs)
# Result: {"success": 85, "error": 10, "cancelled": 3, "processing": 2}
```
**Used In:**
- `jobs/index.json` → `by_status`

---

### Job Statistics (by operation)

**Location:** Computed from jobs list
**How to Obtain:**
```python
by_operation = Counter(job.get('operationName', 'unknown') for job in jobs)
# Result: {"workspaceCreate": 15, "tableImport": 40, ...}
```
**Used In:**
- `jobs/index.json` → `by_operation`

---

### Latest Job per Component

**Location:** Computed from jobs list
**Endpoint:** None (computed from jobs)
**How to Obtain:**
```python
from collections import defaultdict

component_jobs = defaultdict(list)

for job in jobs:
    comp_id = job.get('operationParams', {}).get('componentId')
    config_id = job.get('operationParams', {}).get('configurationId')
    if comp_id and config_id:
        component_jobs[(comp_id, config_id)].append(job)

# Get latest for each component (including transformations)
for (comp_id, config_id), job_list in component_jobs.items():
    latest = max(job_list, key=lambda j: j.get('endTime', j['createdTime']))
    success_count = sum(1 for j in job_list if j['status'] == 'success')
    error_count = sum(1 for j in job_list if j['status'] == 'error')
```
**Note:** This includes transformations (e.g., `keboola.snowflake-transformation/orders_clean`)
**Used In:**
- `jobs/by-component/{component_id}/{config_id}/latest.json`

---

### Transformation Jobs Statistics

**Location:** Computed from jobs list
**How to Obtain:**
```python
# Extract transformation-specific jobs
transformation_jobs = [
    j for j in jobs
    if j['operationName'] == 'transformationRun'
]

# Group transformation jobs by platform
from collections import Counter
platform_counts = Counter()
recent_transformations = []

for job in transformation_jobs[:20]:  # Recent 20 transformation jobs
    component_id = job['operationParams'].get('componentId', '')
    platform = detect_platform_from_component_id(component_id)
    platform_counts[platform] += 1

    recent_transformations.append({
        'job_id': job['id'],
        'transformation': job['operationParams'].get('configurationId', ''),
        'component_id': component_id,
        'status': job['status'],
        'completed_time': job['endTime'],
        'duration_seconds': job.get('durationSeconds', 0)
    })
```
**Used In:**
- `jobs/index.json` → `transformations.total_runs`
- `jobs/index.json` → `transformations.by_platform`
- `jobs/index.json` → `transformations.recent_transformations`

---

### Job Queue API - Create and Run Job

**Location:** API - Job Queue
**Endpoint:** `POST https://queue.{STACK}/jobs`
**Reference:** See `KEBOOLA_JOBS_API.md` for complete documentation
**How to Use:**
```python
import requests

# Create and run a job (e.g., transformation)
# Replace {STACK} with your stack (e.g., us-east4.gcp.keboola.com)
response = requests.post(
    "https://queue.{STACK}/jobs",
    headers={
        "X-StorageApi-Token": token,
        "Content-Type": "application/json"
    },
    json={
        "mode": "run",
        "component": "keboola.snowflake-transformation",
        "config": "orders_clean"
    }
)

job_result = response.json()
# Returns: {"id": 55789432, "status": "created", ...}
```
**Request Parameters:**
- `mode`: Job execution mode (typically "run")
- `component`: Component ID (e.g., "keboola.snowflake-transformation")
- `config`: Configuration ID (transformation name or config ID)

**Important Notes:**
- Use correct endpoint for your Stack (e.g., `queue.us-east4.gcp.keboola.com`)
- For searching jobs, use `GET /search/jobs` endpoint (see RFC-LLM-TWIN-FORMAT-EXPORT.md)
- Token must have permissions for the component
- Job ID can be used to track execution via Queue API or Storage API

**Used For:**
- Triggering transformation runs
- Manual job execution
- Automated pipeline execution
- Integration with external systems

---

## 💾 Data Samples

### Table Sample Data

**Location:** API - Table preview
**Endpoint:** `GET /v2/storage/tables/{table_id}/data-preview?limit=100`
**How to Obtain:**
```python
table_id = "in.c-shopify.orders"
response = requests.get(
    f"{api_base}/v2/storage/tables/{table_id}/data-preview?limit=100",
    headers={"X-StorageApi-Token": token}
)
sample_csv = response.text  # Already in CSV format
```
**Response Format:** RFC 4180 CSV with UTF-8 encoding
**Security:**
```python
is_public_repo = False  # TODO: Get from repo config
export_samples = True   # Admin setting

if is_public_repo:
    export_samples = False  # ALWAYS override for safety

if not export_samples:
    return None  # Don't fetch samples
```
**Used In:**
- `storage/samples/{bucket}/{table}/sample.csv`
- `storage/samples/index.json` → `samples[]`

---

### Sample Metadata

**Location:** Computed during sampling
**How to Obtain:**
```python
# Count rows in CSV
sample_rows = len(sample_csv.strip().split('\n')) - 1  # Exclude header

# Extract columns from header
columns = sample_csv.split('\n')[0].replace('"', '').split(',')

metadata = {
    'table_id': f"table:{bucket}/{table}",
    'sample_size': sample_rows,
    'sample_date': datetime.now().isoformat(),
    'total_rows_in_table': table_data['rowsCount'],
    'format': 'csv',
    'columns': columns,
    'note': 'First 100 rows. Data may have changed since sample was taken.'
}
```
**Used In:**
- `storage/samples/{bucket}/{table}/metadata.json`

---

### Sample Count

**Location:** Computed from created samples
**How to Obtain:**
```python
total_samples = len(samples_created)
```
**Used In:**
- `storage/samples/index.json` → `total_samples`

---

### Public Repository Status

**Location:** Git repository config (external)
**How to Obtain:**
```python
# Option 1: From admin settings (if available)
# Option 2: Query Git API (GitHub, GitLab, etc.)
# Option 3: Manual configuration

is_public_repo = False  # Default to private (safe)
```
**Security:** When in doubt, assume private but don't export samples
**Used In:**
- `manifest.yaml` → `security.isPublicRepo`
- `storage/samples/index.json` → `isPublicRepo`
- Sample export decision

---

## 📊 Lineage Graph

### Graph Edges

**Location:** Computed from transformation storage mappings
**How to Obtain:**
```python
edges = []

for transform in transformations:
    transform_uid = transform['uid']

    # Input edges: table → transformation
    for inp in transform['input_tables']:
        table_ref = inp['source']  # "in.c-bucket.table"
        stage, bucket, table = parse_table_reference(table_ref)

        edges.append({
            'source': f"table:{bucket}/{table}",
            'target': transform_uid,
            'type': 'consumed_by'
        })

    # Output edges: transformation → table
    for out in transform['output_tables']:
        table_ref = out['destination']
        stage, bucket, table = parse_table_reference(table_ref)

        edges.append({
            'source': transform_uid,
            'target': f"table:{bucket}/{table}",
            'type': 'produces'
        })
```
**Used In:**
- `indices/graph.jsonl` → All edges (one per line)
- Table dependencies computation
- Transformation dependencies computation

---

### Graph Metadata (_meta)

**Location:** Computed from graph edges
**How to Obtain:**
```python
# Count unique nodes
all_nodes = set()
for edge in edges:
    all_nodes.add(edge['source'])
    all_nodes.add(edge['target'])

# Count by type
table_nodes = [n for n in all_nodes if n.startswith('table:')]
transform_nodes = [n for n in all_nodes if n.startswith('transform:')]

meta = {
    '_meta': {
        'total_edges': len(edges),
        'total_nodes': len(all_nodes),
        'total_tables': len(table_nodes),
        'total_transformations': len(transform_nodes),
        'sources': len(sources),
        'updated': datetime.now().isoformat()
    }
}
```
**Format:** First line of `indices/graph.jsonl`
**Used In:**
- `indices/graph.jsonl` → Line 1

---

### Parse Table Reference

**Location:** Utility function
**How to Obtain:**
```python
def parse_table_reference(table_ref):
    """
    Parse: "in.c-shopify.orders" or "out.c-processed.orders_clean"
    Returns: (stage, bucket, table)
    """
    parts = table_ref.split('.')
    if len(parts) >= 3:
        stage = parts[0]        # "in" or "out"
        bucket = parts[1]       # "c-shopify"
        table = '.'.join(parts[2:])  # "orders" (may contain dots)
        return stage, bucket, table
    return None, None, None
```
**Used In:**
- Graph building
- Table tracking
- Dependency computation

---

## 🔍 Computed Indices

### Sources Registry

**Location:** Inferred from buckets + components
**How to Obtain:**
```python
sources = defaultdict(lambda: {
    'type': 'extractor',
    'instances': 0,
    'buckets': set(),
    'table_count': 0
})

# From buckets
for bucket in buckets:
    bucket_clean = bucket['id'].replace('in.c-', '').replace('out.c-', '')
    source = infer_source_from_bucket(bucket_clean)

    sources[source]['buckets'].add(bucket_clean)
    sources[source]['table_count'] += len(bucket['tables'])

# From components (count extractors)
for component in components:
    if component['type'] == 'extractor':
        source = map_component_to_source(component['id'])
        sources[source]['instances'] += 1

# Build output
sources_list = []
for source_id, source_data in sorted(sources.items()):
    sources_list.append({
        'id': source_id,
        'name': source_id.replace('-', ' ').title(),
        'type': source_data['type'],
        'instances': source_data['instances'] or len(source_data['buckets']),
        'total_tables': source_data['table_count'],
        'buckets': sorted(list(source_data['buckets']))
    })
```
**Used In:**
- `indices/sources.json`
- `manifest-extended.json` → `sources`

---

### Tables by Source

**Location:** Computed from table metadata
**How to Obtain:**
```python
from collections import defaultdict

tables_by_source = defaultdict(list)

for table in tables:
    source = table['source']  # From infer_source_from_bucket()
    tables_by_source[source].append({
        'uid': table['uid'],
        'name': table['name'],
        'bucket': table['bucket_clean']
    })

# Sort within each source
for source in tables_by_source:
    tables_by_source[source].sort(key=lambda t: t['name'])
```
**Used In:**
- `indices/queries/tables-by-source.json`

---

### Transformations by Platform

**Location:** Computed from transformation metadata
**How to Obtain:**
```python
transforms_by_platform = defaultdict(list)

for transform in transformations:
    platform = transform['platform']
    transforms_by_platform[platform].append({
        'uid': transform['uid'],
        'name': transform['name'],
        'is_disabled': transform['is_disabled']
    })

# Sort within each platform
for platform in transforms_by_platform:
    transforms_by_platform[platform].sort(key=lambda t: t['name'])
```
**Used In:**
- `indices/queries/transformations-by-platform.json`

---

### Most Connected Nodes

**Location:** Computed from dependencies
**How to Obtain:**
```python
node_connections = []

# Tables
for table in tables:
    connection_count = (
        len(table['dependencies']['consumed_by']) +
        len(table['dependencies']['produced_by'])
    )
    if connection_count > 0:
        node_connections.append({
            'uid': table['uid'],
            'name': table['name'],
            'type': 'table',
            'connections': connection_count,
            'consumed_by_count': len(table['dependencies']['consumed_by']),
            'produced_by_count': len(table['dependencies']['produced_by'])
        })

# Transformations
for transform in transformations:
    connection_count = (
        len(transform['dependencies']['consumes']) +
        len(transform['dependencies']['produces'])
    )
    if connection_count > 0:
        node_connections.append({
            'uid': transform['uid'],
            'name': transform['name'],
            'type': 'transformation',
            'connections': connection_count,
            'input_count': len(transform['dependencies']['consumes']),
            'output_count': len(transform['dependencies']['produces'])
        })

# Sort by connection count
node_connections.sort(key=lambda n: n['connections'], reverse=True)

# Take top 50
top_nodes = node_connections[:50]
```
**Used In:**
- `indices/queries/most-connected-nodes.json`

---

### Platform Statistics

**Location:** Computed from transformations
**How to Obtain:**
```python
from collections import Counter

platform_stats = Counter(t['platform'] for t in transformations)
# Result: {"snowflake": 224, "python": 61, "dbt": 10, ...}
```
**Used In:**
- `manifest-extended.json` → `transformation_platforms`
- `transformations/index.json` → `by_platform`

---

### Bucket Statistics by Source

**Location:** Computed from buckets
**How to Obtain:**
```python
source_stats = defaultdict(lambda: {'count': 0, 'total_tables': 0})

for bucket in buckets:
    bucket_clean = bucket['id'].replace('in.c-', '').replace('out.c-', '')
    source = infer_source_from_bucket(bucket_clean)

    source_stats[source]['count'] += 1
    source_stats[source]['total_tables'] += len(bucket['tables'])
```
**Used In:**
- `buckets/index.json` → `by_source`

---

## 🤖 AI Instructions

### AI README Content

**Location:** Generated from project data + static template
**How to Obtain:**
```python
owner = project_data['owner']
stats = {
    'total_tables': len(tables),
    'total_transformations': len(transformations),
    'total_buckets': len(buckets),
    'total_sources': len(sources)
}

ai_readme = f"""# Keboola Project AI Guide

## 📊 Project Overview
- **Project ID:** {owner['id']}
- **Project Name:** {owner['name']}
- **Region:** {owner['region']}
- **Backend:** Snowflake
- **Total Tables:** {stats['total_tables']}
- **Total Transformations:** {stats['total_transformations']}
- **Data Sources:** {stats['total_sources']}

## 🏗️ Project Structure
[Structure guide]

## 🔍 How to Analyze
[Analysis methods]

## 🔗 API Reference
**Base URL:** {api_base}
[API endpoints]

## 🤖 Best Practices
[Do's and don'ts]
"""
```
**Template:** See `_template/ai/README.md` for complete structure
**Used In:**
- `ai/README.md`

---

## 📅 Timestamps

### Current Timestamp

**Location:** Generated at runtime
**How to Obtain:**
```python
from datetime import datetime

timestamp = datetime.now().isoformat()
# Result: "2025-11-24T11:42:03.123456"
```
**Used In:**
- `manifest-extended.json` → `updated`
- `storage/samples/index.json` → `last_updated`
- `storage/samples/{bucket}/{table}/metadata.json` → `sample_date`
- `indices/graph.jsonl` → `_meta.updated`

---

## 🔒 Security Configuration

### Encrypt Secrets Setting

**Location:** Configuration (hardcoded or from settings)
**How to Obtain:**
```python
encrypt_secrets = True  # Always true for safety
```
**Used In:**
- `manifest.yaml` → `security.encryptSecrets`

---

### Export Data Samples Setting

**Location:** Configuration
**How to Obtain:**
```python
# If public repo
if is_public_repo:
    export_samples = False  # Safety override
else:
    export_samples = True  # Or from admin config
```
**Used In:**
- `manifest.yaml` → `security.exportDataSamples`
- `storage/samples/index.json` → `enabled`

---

### Retention Policies

**Location:** Configuration (fixed values)
**How to Obtain:**
```python
retention = {
    'jobs': 100,        # Keep last 100 jobs
    'sampleRows': 100   # Max 100 rows per sample
}
```
**Used In:**
- `manifest.yaml` → `retention`
- `jobs/index.json` → `retention_policy`

---

## 📋 Quick Reference Table

| Data | Source Type | Location | Endpoint/File |
|------|-------------|----------|---------------|
| **Project ID** | API | Storage API | `GET /v2/storage/tokens/verify` → `owner.id` |
| **Project Name** | API | Storage API | `GET /v2/storage/tokens/verify` → `owner.name` |
| **Project Region** | API | Storage API | `GET /v2/storage/tokens/verify` → `owner.region` |
| **API Base URL** | Local | `.keboola/project.json` | `project.apiHost` |
| **API Token** | Local | `.env.local` | `KBC_STORAGE_API_TOKEN` |
| **Buckets List** | API | Storage API | `GET /v2/storage/buckets` |
| **Table Details** | API | Storage API | `GET /v2/storage/tables/{id}?include=columns,metadata` |
| **Table Samples** | API | Storage API | `GET /v2/storage/tables/{id}/data-preview?limit=100` |
| **Jobs List** | API | Storage API | `GET /v2/storage/jobs?limit=100` |
| **Create Job** | API | Job Queue | `POST https://queue.{STACK}/jobs` |
| **Transformation Jobs** | Computed | Jobs list | Filter by `operationName: "transformationRun"` |
| **Transformation Execution** | Computed | Jobs + Transforms | Map jobs to transformations by component+config |
| **Components** | API | Storage API | `GET /v2/storage?exclude=componentDetails` |
| **Component Configs** | API | Storage API | `GET /v2/storage/components/{id}/configs` |
| **Transform Configs** | Local | `main/transformation/` | Scan `config.json` files |
| **Transform Names** | Local | `main/transformation/` | Read `meta.json` files |
| **Transform Descriptions** | Local | `main/transformation/` | Read `description.md` files |
| **Platform Detection** | Computed | Component ID | Pattern matching (see PLATFORM-CLASSIFICATION.md) |
| **Source Inference** | Computed | Bucket name | Pattern matching |
| **Graph Edges** | Computed | Storage mappings | Parse input/output tables |
| **Table Dependencies** | Computed | Graph edges | Filter by table UID |
| **Statistics** | Computed | All data | Count, aggregate, group |
| **Timestamps** | Generated | Runtime | `datetime.now().isoformat()` |
| **Security Config** | Configuration | Hardcoded/admin | Fixed values or API settings |

---

## 🎯 Generation Order (Critical)

**Dependencies matter - follow this order:**

1. **Load credentials** (.env.local, .keboola/project.json)
2. **Fetch project metadata** (API: tokens/verify)
3. **Fetch buckets** (API: buckets)
4. **Fetch jobs** (API: jobs) - **MUST be done before transformations**
5. **Scan transformations** (Local: main/transformation/)
6. **Link jobs to transformations** (Computed: map transformation jobs by component+config)
7. **Build graph edges** (Computed: from storage mappings)
8. **Compute dependencies** (Computed: from graph edges)
9. **Infer sources** (Computed: from bucket names)
10. **Compute statistics** (Computed: counts and aggregations)
11. **Generate files** (All directories) - **includes job execution data**
12. **Fetch samples** (API: data-preview, if security allows)
13. **Validate output** (Check all requirements met)

**Critical:** Jobs must be fetched (step 4) BEFORE transformation metadata generation (step 11) to include job execution status in transformation files.

---

## ✅ Validation Checklist

After generation, verify:

### Structure
- [ ] All directories from `_template/` exist
- [ ] manifest.yaml has security section
- [ ] manifest-extended.json has real project ID
- [ ] README.md generated
- [ ] ai/README.md has real project data

### Documentation
- [ ] All JSON files have `_comment` field
- [ ] All JSON files have `_purpose` field
- [ ] All JSON files have `_update_frequency` field
- [ ] Security-sensitive files have `_security` field
- [ ] Job files have `_retention` field

### Data Quality
- [ ] Platform detection: 0 unknown transformations
- [ ] All table dependencies computed
- [ ] Graph edges have correct types (consumed_by/produces)
- [ ] Sources inferred (not all "unknown")
- [ ] Statistics match actual counts
- [ ] All transformations have job execution status
- [ ] Transformation job references are valid
- [ ] Jobs/by-component includes transformations

### Security
- [ ] No plaintext secrets (# fields encrypted)
- [ ] Public repo check implemented
- [ ] Sample export respects security config
- [ ] No API tokens in output files

### Content
- [ ] Jobs directory has ~100 jobs
- [ ] Jobs include transformation runs (operationName: "transformationRun")
- [ ] Jobs/index.json has transformations section
- [ ] Jobs/by-component includes transformation components
- [ ] Samples directory exists (even if empty)
- [ ] Components directory has index
- [ ] Graph has _meta header
- [ ] Pre-computed queries present
- [ ] Transformations linked to latest jobs

---

## 📖 Documentation References

**Primary:**
- `_template/GENERATION-GUIDE.md` - Complete specification
- `_template/00-START-HERE.md` - Navigation

**Reference:**
- `keboola-storage-api-docs.md` - Storage API documentation
- `KEBOOLA_JOBS_API.md` - Job Queue API documentation
- `_template/PLATFORM-CLASSIFICATION.md` - Platform detection
- `_template/GAP-ANALYSIS.md` - PRD requirements

**Examples:**
- All `_template/*.json` files - Structure examples
- All `_template/*/*.json` files - Field examples

---

## 🚀 Ready to Generate!

**You now have:**
- ✅ Complete data source mapping (this document)
- ✅ Generation specification (_template/GENERATION-GUIDE.md)
- ✅ Storage API documentation (keboola-storage-api-docs.md)
- ✅ Job Queue API documentation (KEBOOLA_JOBS_API.md)
- ✅ Template examples (_template/)
- ✅ API access (read-only + job queue)
- ✅ Local data (main/)
- ✅ Transformation job execution tracking
- ✅ Job queue integration

**Follow the steps, use the references, and generate the twin format!**

---

**Document Version:** 1.1
**Last Updated:** 2025-12-03
**Total Data Sources:** 45+ mapped
**New in v1.1:**
- Transformation job execution tracking
- Job Queue API integration
- Transformation-to-job linking
- Enhanced job statistics
**Ready for:** Implementation
