# 🏗️ Twin Format Generation Guide

**Purpose:** This document explains how to generate every file and directory in the twin format from Keboola Storage API.

**Audience:** Service developers, coding agents, implementation teams

---

## 📋 Table of Contents

1. [Root Files](#root-files)
2. [Buckets Directory](#buckets-directory)
3. [Transformations Directory](#transformations-directory)
4. [Components Directory](#components-directory)
5. [Storage Samples Directory](#storage-samples-directory)
6. [Jobs Directory](#jobs-directory)
7. [Indices Directory](#indices-directory)
8. [AI Directory](#ai-directory)
9. [Complete Generation Flow](#complete-generation-flow)

---

## 🔧 Root Files

### manifest.yaml

**Purpose:** Simple project configuration with security settings

**Data Source:** Project configuration + Git repository settings

**API Endpoint:** `GET /v2/storage/tokens/verify`

**Generation Logic:**
```python
def generate_manifest_yaml(project_data, git_repo_config):
    """
    Args:
        project_data: Response from /v2/storage/tokens/verify
        git_repo_config: Git repository configuration from admin settings

    Returns:
        YAML content for manifest.yaml
    """
    return {
        'twin_version': 1,
        'project_id': project_data['owner']['id'],
        'security': {
            'encryptSecrets': git_repo_config.get('encrypt_secrets', True),
            'isPublicRepo': git_repo_config.get('is_public', False),
            'exportDataSamples': not git_repo_config.get('is_public', False)
        },
        'retention': {
            'jobs': 100,  # Keep last 100 jobs
            'sampleRows': 100  # Max 100 rows per sample
        }
    }
```

**Required Data:**
- `project_id` from API
- Git repository visibility (public/private)
- Admin preferences for secrets/samples

---

### manifest-extended.json

**Purpose:** Complete project overview in single file for fast AI agent analysis

**Data Sources:**
- Project metadata: `GET /v2/storage/tokens/verify`
- Buckets: `GET /v2/storage/buckets`
- Transformations: Scan `main/transformation/` configs
- Sources: Infer from bucket names

**API Endpoints:**
```
GET /v2/storage/tokens/verify
GET /v2/storage/buckets
```

**Generation Logic:**
```python
def generate_manifest_extended(project_data, buckets, transformations, sources):
    """
    Args:
        project_data: From /v2/storage/tokens/verify
        buckets: List of all buckets from /v2/storage/buckets
        transformations: List of transformation configs
        sources: Inferred data sources

    Returns:
        JSON content for manifest-extended.json
    """
    # Count tables across all buckets
    total_tables = sum(len(b.get('tables', [])) for b in buckets)

    # Group transformations by platform
    platform_counts = Counter(t['platform'] for t in transformations)

    # Build source registry
    source_list = []
    for source_name, source_info in sources.items():
        source_list.append({
            'id': source_name,
            'name': source_name.replace('-', ' ').title(),
            'type': 'extractor',
            'instances': len(source_info['buckets']),
            'total_tables': source_info['table_count'],
            'buckets': sorted(list(source_info['buckets']))
        })

    return {
        'project_id': project_data['owner']['id'],
        'twin_version': 1,
        'format_version': 2,
        'updated': datetime.now().isoformat(),
        'statistics': {
            'total_buckets': len(buckets),
            'total_tables': total_tables,
            'total_transformations': len(transformations),
            'total_edges': count_graph_edges()
        },
        'sources': source_list,
        'transformation_platforms': dict(platform_counts)
    }
```

**Update Frequency:** On every sync (reflects current state)

**Purpose for AI Agents:** Single file read gives complete project overview without scanning directories

---

### README.md

**Purpose:** Human and AI-readable project documentation

**Data Source:** Generated from project statistics and structure

**Generation Logic:**
```python
def generate_readme(manifest_extended):
    """
    Args:
        manifest_extended: Data from manifest-extended.json

    Returns:
        Markdown content with project overview and navigation guide
    """
    stats = manifest_extended['statistics']

    return f"""# Keboola Project (Twin Format v2)

AI-optimized structure for efficient project analysis.

## Quick Start
**Read this first:** `manifest-extended.json` - Complete project overview

## Statistics
- **Tables:** {stats['total_tables']}
- **Transformations:** {stats['total_transformations']}
- **Buckets:** {stats['total_buckets']}
...
"""
```

**Update Frequency:** On structure changes

---

## 📦 Buckets Directory

### buckets/index.json

**Purpose:** Catalog of all storage buckets and tables for fast lookup

**Data Source:** `GET /v2/storage/buckets`

**API Endpoint:**
```
GET /v2/storage/buckets?include=tables
```

**Generation Logic:**
```python
def generate_buckets_index(buckets_response):
    """
    Args:
        buckets_response: Response from /v2/storage/buckets

    Returns:
        JSON content for buckets/index.json
    """
    bucket_summaries = []
    source_stats = defaultdict(lambda: {'count': 0, 'total_tables': 0})

    for bucket in buckets_response:
        # Remove 'c-' prefix for cleaner names
        bucket_clean = bucket['id'].replace('in.c-', '').replace('out.c-', '')

        # Infer source from bucket name
        source = infer_source_from_bucket_name(bucket_clean)

        # Get table names
        table_names = [t.split('.')[-1] for t in bucket.get('tables', [])]

        bucket_summaries.append({
            'name': bucket_clean,
            'source': source,
            'table_count': len(table_names),
            'tables': sorted(table_names)
        })

        source_stats[source]['count'] += 1
        source_stats[source]['total_tables'] += len(table_names)

    return {
        'total_buckets': len(buckets_response),
        'by_source': dict(source_stats),
        'buckets': sorted(bucket_summaries, key=lambda x: x['name'])
    }
```

**Update Frequency:** On every sync

**Purpose for AI Agents:** Quick bucket discovery without scanning subdirectories

---

### buckets/{bucket}/tables/{table}/metadata.json

**Purpose:** Complete table schema and metadata

**Data Source:** `GET /v2/storage/tables/{table_id}?include=columns,metadata`

**API Endpoint:**
```
GET /v2/storage/tables/{table_id}?include=columns,metadata,columnMetadata
```

**Generation Logic:**
```python
def generate_table_metadata(table_data, graph_edges):
    """
    Args:
        table_data: Response from /v2/storage/tables/{table_id}
        graph_edges: Computed lineage edges

    Returns:
        JSON content for table metadata.json
    """
    table_id = table_data['id']
    bucket = table_id.split('.')[1].replace('c-', '')
    table_name = table_id.split('.')[-1]

    # Find what consumes and produces this table from graph
    consumed_by = [e['target'] for e in graph_edges
                   if e['source'] == f"table:{bucket}/{table_name}"]
    produced_by = [e['source'] for e in graph_edges
                   if e['target'] == f"table:{bucket}/{table_name}"]

    return {
        'uid': f"table:{bucket}/{table_name}",
        'name': table_name,
        'type': 'table',
        'bucket': bucket,
        'source': infer_source_from_bucket_name(bucket),
        'columns': table_data.get('columns', []),
        'primaryKey': table_data.get('primaryKey', []),
        'rowsCount': table_data.get('rowsCount', 0),
        'dataSizeBytes': table_data.get('dataSizeBytes', 0),
        'created': table_data.get('created'),
        'lastImportDate': table_data.get('lastImportDate'),
        'description': extract_description_from_metadata(table_data.get('metadata', [])),
        'metadata': table_data.get('metadata', []),
        'dependencies': {
            'consumed_by': consumed_by,
            'produced_by': produced_by
        }
    }
```

**Update Frequency:** On table structure changes

**Directory Structure:**
```
buckets/
  {bucket_clean_name}/
    tables/
      {table_name}/
        metadata.json  # Generated per table
```

**Purpose for AI Agents:** Complete table schema, lineage, and metadata in one file

---

## 🔄 Transformations Directory

### transformations/index.json

**Purpose:** Catalog of all transformations grouped by platform

**Data Source:** Scan transformation configs in project

**Generation Logic:**
```python
def generate_transformations_index(transformations, jobs=None):
    """
    Args:
        transformations: List of transformation configurations
        jobs: Recent jobs list (optional, for job execution tracking)

    Returns:
        JSON content for transformations/index.json
    """
    # Count by platform
    platform_stats = Counter(t['platform'] for t in transformations)

    # Build summaries
    transform_summaries = []
    for transform in transformations:
        summary = {
            'uid': transform['uid'],
            'name': transform['name'],
            'platform': transform['platform'],
            'is_disabled': transform['is_disabled'],
            'input_count': len(transform.get('input_tables', [])),
            'output_count': len(transform.get('output_tables', []))
        }

        # Add job execution summary if available
        if jobs and transform.get('job_execution'):
            summary['last_run_time'] = transform['job_execution']['last_run_time']
            summary['last_run_status'] = transform['job_execution']['last_run_status']
            summary['job_reference'] = transform['job_execution']['job_reference']

        transform_summaries.append(summary)

    return {
        'total_transformations': len(transformations),
        'by_platform': dict(platform_stats),
        'transformations': sorted(transform_summaries, key=lambda x: x['name'])
    }
```

**Update Frequency:** On transformation config changes

---

### transformations/{transform_name}/metadata.json

**Purpose:** Complete transformation configuration and dependencies

**Data Source:** Transformation config from project + job queue status

**API Endpoint:** None (from CLI export or config storage) + `GET /v2/storage/jobs`

**Generation Logic:**
```python
def generate_transformation_metadata(transform_config, graph_edges, latest_job=None):
    """
    Args:
        transform_config: Transformation configuration
        graph_edges: Computed lineage edges
        latest_job: Latest job execution for this transformation (optional)

    Returns:
        JSON content for transformation metadata.json
    """
    transform_id = transform_config['uid']

    # Extract inputs and outputs from graph
    inputs = [e['source'] for e in graph_edges if e['target'] == transform_id]
    outputs = [e['target'] for e in graph_edges if e['source'] == transform_id]

    metadata = {
        'uid': transform_id,
        'name': transform_config['name'],
        'type': 'transformation',
        'platform': detect_platform_from_component_id(transform_config['component_id']),
        'component_id': transform_config['component_id'],
        'is_disabled': transform_config.get('is_disabled', False),
        'description': transform_config.get('description', ''),
        'original_path': transform_config['path'],
        'dependencies': {
            'consumes': inputs,
            'produces': outputs
        }
    }

    # Add job execution information if available
    if latest_job:
        metadata['job_execution'] = {
            'last_run_time': latest_job['endTime'],
            'last_run_status': latest_job['status'],
            'job_reference': str(latest_job['id']),
            'duration_seconds': latest_job.get('durationSeconds', 0),
            'last_error': latest_job.get('error')
        }

    return metadata
```

**IMPORTANT:** Every transformation MUST track its job execution status. This includes:
- `last_run_time`: Timestamp of last execution
- `last_run_status`: Status (success, error, cancelled, processing)
- `job_reference`: Job ID for detailed lookup
- `duration_seconds`: Execution time
- `last_error`: Error message if failed

**Platform Detection:**
```python
def detect_platform_from_component_id(component_id):
    """Detect transformation platform from component ID"""
    component_id_lower = component_id.lower()

    if 'snowflake' in component_id_lower: return 'snowflake'
    elif 'redshift' in component_id_lower: return 'redshift'
    elif 'bigquery' in component_id_lower: return 'bigquery'
    elif 'python' in component_id_lower: return 'python'
    elif 'dbt' in component_id_lower: return 'dbt'
    elif 'duckdb' in component_id_lower: return 'duckdb'
    elif '.r-transformation' in component_id_lower: return 'r'
    # ... more platforms
    else: return 'unknown'
```

**Update Frequency:** On transformation config changes

**Purpose for AI Agents:** Platform, code location, and data flow dependencies

---

## 🧩 Components Directory

### components/index.json

**Purpose:** Catalog of all component configurations (extractors, writers, orchestrators)

**Data Source:** `GET /v2/storage` + component configs

**API Endpoint:**
```
GET /v2/storage?exclude=componentDetails
GET /v2/storage/components/{component_id}/configs  # For each component
```

**Generation Logic:**
```python
def generate_components_index(component_configs):
    """
    Args:
        component_configs: All component configurations from project

    Returns:
        JSON content for components/index.json
    """
    components_by_type = defaultdict(list)

    for config in component_configs:
        comp_type = config['component']['type']  # extractor, writer, etc.

        components_by_type[comp_type].append({
            'type': comp_type,
            'id': config['component']['id'],
            'name': config['component']['name'],
            'configs': [config['id']]
        })

    return {
        'total_components': len(component_configs),
        'by_type': {k: len(v) for k, v in components_by_type.items()},
        'components': merge_duplicate_components(components_by_type)
    }
```

**Update Frequency:** On component config changes

---

### components/{type}/{component_id}/{config_id}/metadata.json

**Purpose:** Complete component configuration with secrets encrypted

**Data Source:** Component configuration from Storage API

**API Endpoint:**
```
GET /v2/storage/components/{component_id}/configs/{config_id}
```

**Generation Logic:**
```python
def generate_component_metadata(config_data, config_type):
    """
    Args:
        config_data: Component configuration from API
        config_type: 'extractor', 'writer', 'orchestrator', etc.

    Returns:
        JSON content for component metadata.json
    """
    # Encrypt secrets in configuration
    safe_config = encrypt_secrets(config_data['configuration'])

    # Build metadata structure
    metadata = {
        'uid': f"component:{config_type}:{config_data['componentId']}:{config_data['id']}",
        'componentId': config_data['componentId'],
        'configurationId': config_data['id'],
        'name': config_data['name'],
        'type': config_type,
        'description': config_data.get('description', ''),
        'isDisabled': config_data.get('isDisabled', False),
        'created': config_data['created'],
        'lastModified': config_data['changeDescription']['created'],
        'version': config_data['version'],
        'configuration': safe_config
    }

    # Add type-specific fields
    if config_type == 'extractor':
        metadata['outputs'] = extract_output_tables(config_data)
    elif config_type == 'writer':
        metadata['inputs'] = extract_input_tables(config_data)
    elif config_type == 'orchestrator':
        metadata['configuration']['tasks'] = extract_orchestrator_tasks(config_data)

    return metadata

def encrypt_secrets(config_params):
    """Encrypt fields starting with # (secrets)"""
    encrypted_config = {}
    for key, value in config_params.items():
        if key.startswith('#'):
            encrypted_config[key] = '***ENCRYPTED***'
        elif isinstance(value, dict):
            encrypted_config[key] = encrypt_secrets(value)
        else:
            encrypted_config[key] = value
    return encrypted_config
```

**Update Frequency:** On component config changes

**Purpose for AI Agents:** Complete component setup, inputs/outputs, schedules

---

## 📊 Storage Samples Directory

### storage/samples/index.json

**Purpose:** Catalog of available data samples with security metadata

**Data Source:** Computed from enabled tables + security settings

**Generation Logic:**
```python
def generate_samples_index(tables, security_config):
    """
    Args:
        tables: List of all tables
        security_config: Security settings from manifest.yaml

    Returns:
        JSON content for storage/samples/index.json
    """
    # Check if samples should be exported
    enabled = security_config['exportDataSamples'] and not security_config['isPublicRepo']

    samples = []
    if enabled:
        for table in tables:
            if should_sample_table(table):  # Check size, privacy, etc.
                samples.append({
                    'bucket': table['bucket'],
                    'table': table['name'],
                    'rows': 100,  # Always 100 (or actual if less)
                    'columns': table['columns'],
                    'sample_file': f"{table['bucket']}/{table['name']}/sample.csv"
                })

    return {
        'enabled': enabled,
        'limit': 100,
        'isPublicRepo': security_config['isPublicRepo'],
        'total_samples': len(samples),
        'last_updated': datetime.now().isoformat(),
        'samples': samples,
        'security': {
            'disabled_for_public_repos': True,
            'pii_columns_excluded': False,  # TODO: Implement PII detection
            'note': 'Samples are first 100 rows. May not reflect current data.'
        }
    }
```

**Update Frequency:** Daily or on-demand

**Security Rules:**
- **Public repos:** `enabled = false` (override admin setting)
- **Private repos:** Respect admin setting
- **PII detection:** TODO - exclude sensitive columns

---

### storage/samples/{bucket}/{table}/sample.csv

**Purpose:** Actual data sample (first 100 rows)

**Data Source:** `GET /v2/storage/tables/{table_id}/data-preview?limit=100`

**API Endpoint:**
```
GET /v2/storage/tables/{table_id}/data-preview?limit=100
```

**Generation Logic:**
```python
def generate_table_sample(table_id, security_config):
    """
    Args:
        table_id: Full table ID (e.g., in.c-bucket.table)
        security_config: Security settings

    Returns:
        CSV content for sample.csv or None if disabled
    """
    # Check if sampling is allowed
    if security_config['isPublicRepo'] or not security_config['exportDataSamples']:
        return None  # Don't create file

    # Fetch data preview
    url = f"{API_BASE}/v2/storage/tables/{table_id}/data-preview?limit=100"
    response = requests.get(url, headers={'X-StorageApi-Token': token})

    if response.status_code == 200:
        return response.text  # Already in CSV format
    else:
        return None  # Table might be empty or inaccessible
```

**Update Frequency:** Daily (samples may become stale)

**File Format:** RFC 4180 CSV with UTF-8 encoding

---

### storage/samples/{bucket}/{table}/metadata.json

**Purpose:** Sample metadata (timestamp, row count, warnings)

**Data Source:** Generated during sampling

**Generation Logic:**
```python
def generate_sample_metadata(table_id, sample_csv, table_data):
    """
    Args:
        table_id: Full table ID
        sample_csv: Generated CSV content
        table_data: Table metadata

    Returns:
        JSON content for sample metadata.json
    """
    # Count rows in sample (excluding header)
    sample_rows = len(sample_csv.split('\n')) - 1 if sample_csv else 0

    return {
        'table_id': f"table:{table_data['bucket']}/{table_data['name']}",
        'sample_size': sample_rows,
        'sample_date': datetime.now().isoformat(),
        'total_rows_in_table': table_data['rowsCount'],
        'format': 'csv',
        'columns': table_data['columns'],
        'note': 'First 100 rows. Data may have changed since sample was taken.'
    }
```

**Update Frequency:** Same as sample.csv

**Purpose:** Timestamp for data freshness checking

---

## 📝 Jobs Directory

### jobs/index.json

**Purpose:** Job execution statistics and summary

**Data Source:** `GET /v2/storage/jobs`

**API Endpoint:**
```
GET /v2/storage/jobs?limit=100&offset=0
```

**Generation Logic:**
```python
def generate_jobs_index(jobs_response):
    """
    Args:
        jobs_response: Response from /v2/storage/jobs?limit=100

    Returns:
        JSON content for jobs/index.json
    """
    # Group jobs by status
    by_status = Counter(job['status'] for job in jobs_response)

    # Group jobs by operation
    by_operation = Counter(job['operationName'] for job in jobs_response)

    # Extract transformation-specific jobs
    transformation_jobs = [
        j for j in jobs_response
        if j['operationName'] == 'transformationRun'
    ]

    # Group transformation jobs by platform
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

    return {
        'total_jobs': len(jobs_response),
        'recent_jobs_count': len(jobs_response),  # Up to 100
        'by_status': dict(by_status),
        'by_operation': dict(by_operation),
        'transformations': {
            '_comment': 'Track transformation executions separately',
            'total_runs': len(transformation_jobs),
            'by_platform': dict(platform_counts),
            'recent_transformations': recent_transformations
        },
        'retention_policy': {
            'recent_jobs': 'Last 100 jobs',
            'by_component': 'Latest job per component configuration'
        }
    }
```

**Update Frequency:** Every hour or on job completion events

**Purpose:** Quick job health check for AI agents

---

### jobs/recent/{job_id}.json

**Purpose:** Complete job execution details for all operations including transformations

**Data Source:** `GET /v2/storage/jobs/{job_id}`

**API Endpoint:**
```
GET /v2/storage/jobs/{job_id}
```

**Note:** This includes jobs for:
- Workspace creation
- Table imports/exports
- **Transformation runs** (operationName: "transformationRun")
- Sandbox operations
- Other component executions

**Generation Logic:**
```python
def generate_job_metadata(job_data):
    """
    Args:
        job_data: Response from /v2/storage/jobs/{job_id}

    Returns:
        JSON content for jobs/recent/{job_id}.json
    """
    # Calculate duration
    start = datetime.fromisoformat(job_data['startTime'])
    end = datetime.fromisoformat(job_data['endTime'])
    duration = (end - start).total_seconds()

    # Extract relevant fields
    return {
        'id': job_data['id'],
        'runId': job_data['runId'],
        'status': job_data['status'],
        'operationName': job_data['operationName'],
        'operationParams': {
            'queue': job_data['operationParams'].get('queue'),
            'backend': job_data['operationParams'].get('backend'),
            'branchId': job_data['operationParams'].get('branchId'),
            'componentId': job_data['operationParams'].get('componentId'),
            'configurationId': job_data['operationParams'].get('configurationId'),
            'backendSize': job_data['operationParams'].get('backendSize')
        },
        'createdTime': job_data['createdTime'],
        'startTime': job_data['startTime'],
        'endTime': job_data['endTime'],
        'durationSeconds': duration,
        'creatorToken': job_data.get('creatorToken'),
        'metrics': job_data.get('metrics', {}),
        'results': simplify_results(job_data.get('results')),  # Remove sensitive data
        'error': job_data.get('error')  # Only present if failed
    }

def simplify_results(results):
    """Remove sensitive data from job results"""
    if not results:
        return None

    # Keep only non-sensitive fields
    safe_results = {
        'id': results.get('id'),
        'type': results.get('type'),
        'backend': results.get('backend')
    }
    # Remove credentials, keys, etc.
    return safe_results
```

**Update Frequency:** On job completion

**Retention:** Keep last 100 jobs

**Purpose:** Job debugging, performance analysis, failure investigation

---

### jobs/by-component/{component_id}/{config_id}/latest.json

**Purpose:** Latest job status per component configuration (includes transformations)

**Data Source:** Aggregated from jobs list

**Important:** Transformations are tracked here with their component ID (e.g., `keboola.snowflake-transformation`) and configuration ID (transformation name)

**Generation Logic:**
```python
def generate_component_latest_job(jobs, component_id, config_id):
    """
    Args:
        jobs: All recent jobs
        component_id: Component ID
        config_id: Configuration ID

    Returns:
        JSON content for latest.json
    """
    # Filter jobs for this component+config
    component_jobs = [
        j for j in jobs
        if j['operationParams'].get('componentId') == component_id
        and j['operationParams'].get('configurationId') == config_id
    ]

    if not component_jobs:
        return None  # No jobs for this config

    # Sort by time, get latest
    latest = sorted(component_jobs, key=lambda j: j['endTime'], reverse=True)[0]

    # Count success/error
    success_count = sum(1 for j in component_jobs if j['status'] == 'success')
    error_count = sum(1 for j in component_jobs if j['status'] == 'error')

    # Find last error
    errors = [j for j in component_jobs if j['status'] == 'error']
    last_error = errors[0]['error'] if errors else None

    return {
        'componentId': component_id,
        'configurationId': config_id,
        'latestJobId': latest['id'],
        'latestJobStatus': latest['status'],
        'latestJobTime': latest['endTime'],
        'successCount': success_count,
        'errorCount': error_count,
        'lastError': last_error
    }
```

**Update Frequency:** On job completion for that component

**Purpose:** Quick component health check

---

## 📈 Indices Directory

### indices/graph.jsonl

**Purpose:** Complete data lineage graph (table → transformation → table)

**Data Source:** Computed from transformation storage mappings

**Generation Logic:**
```python
def generate_graph_jsonl(transformations):
    """
    Args:
        transformations: All transformation configurations

    Returns:
        JSONL content for graph.jsonl (one JSON per line)
    """
    edges = []

    # Iterate through transformations
    for transform in transformations:
        transform_id = transform['uid']

        # Input edges: table -> transformation
        for input_table in transform.get('input_tables', []):
            table_ref = input_table['source']  # e.g., "in.c-bucket.table"
            _, bucket, table = parse_table_reference(table_ref)

            edges.append({
                'source': f"table:{bucket}/{table}",
                'target': transform_id,
                'type': 'consumed_by'
            })

        # Output edges: transformation -> table
        for output_table in transform.get('output_tables', []):
            table_ref = output_table['destination']
            _, bucket, table = parse_table_reference(table_ref)

            edges.append({
                'source': transform_id,
                'target': f"table:{bucket}/{table}",
                'type': 'produces'
            })

    # Build JSONL with meta header
    lines = []

    # First line: metadata
    lines.append(json.dumps({
        '_meta': {
            'total_edges': len(edges),
            'total_nodes': count_unique_nodes(edges),
            'total_tables': count_nodes_by_type(edges, 'table'),
            'total_transformations': count_nodes_by_type(edges, 'transform'),
            'sources': count_sources(),
            'updated': datetime.now().isoformat()
        }
    }))

    # Remaining lines: edges
    for edge in edges:
        lines.append(json.dumps(edge))

    return '\n'.join(lines)
```

**File Format:** JSONL (one JSON object per line)

**Line 1:** Metadata with statistics
**Lines 2+:** Individual edges

**Update Frequency:** On transformation config changes

**Purpose:** Trace data flow, understand dependencies, impact analysis

---

### indices/sources.json

**Purpose:** Registry of data sources with table counts

**Data Source:** Inferred from bucket names + component configs

**Generation Logic:**
```python
def generate_sources_registry(buckets, components):
    """
    Args:
        buckets: All buckets from API
        components: All component configs (especially extractors)

    Returns:
        JSON content for sources.json
    """
    sources = defaultdict(lambda: {
        'type': 'extractor',
        'instances': 0,
        'buckets': set(),
        'table_count': 0
    })

    # Infer from bucket names
    for bucket in buckets:
        bucket_name = bucket['id'].replace('in.c-', '').replace('out.c-', '')
        source = infer_source_from_bucket_name(bucket_name)

        sources[source]['buckets'].add(bucket_name)
        sources[source]['table_count'] += len(bucket.get('tables', []))

    # Count extractor instances
    for component in components:
        if component['component']['type'] == 'extractor':
            source = map_component_to_source(component['component']['id'])
            sources[source]['instances'] += 1

    # Build output
    source_list = []
    for source_id, source_data in sorted(sources.items()):
        source_list.append({
            'id': source_id,
            'name': source_id.replace('-', ' ').title(),
            'type': source_data['type'],
            'instances': source_data['instances'] or len(source_data['buckets']),
            'total_tables': source_data['table_count'],
            'buckets': sorted(list(source_data['buckets']))
        })

    return {'sources': source_list}

def infer_source_from_bucket_name(bucket_name):
    """Infer source system from bucket name"""
    bucket_lower = bucket_name.lower()

    # Pattern matching
    if 'shopify' in bucket_lower: return 'shopify'
    if 'hubspot' in bucket_lower: return 'hubspot'
    if 'ga4' in bucket_lower or 'analytics' in bucket_lower: return 'google-analytics'
    # ... more patterns

    return 'unknown'
```

**Update Frequency:** On bucket/component changes

**Purpose:** Source analysis, data coverage assessment

---

### indices/queries/tables-by-source.json

**Purpose:** Pre-computed query: all tables grouped by source

**Data Source:** Computed from buckets + sources

**Generation Logic:**
```python
def generate_tables_by_source(tables):
    """
    Args:
        tables: All table metadata

    Returns:
        JSON content for tables-by-source.json
    """
    by_source = defaultdict(list)

    for table in tables:
        source = table['source']
        by_source[source].append({
            'uid': table['uid'],
            'name': table['name'],
            'bucket': table['bucket']
        })

    # Sort tables within each source
    for source in by_source:
        by_source[source].sort(key=lambda t: t['name'])

    return dict(by_source)
```

**Update Frequency:** On table changes

**Purpose:** Fast lookup for "show me all Shopify tables"

---

### indices/queries/transformations-by-platform.json

**Purpose:** Pre-computed query: all transformations grouped by platform

**Data Source:** Computed from transformations

**Generation Logic:**
```python
def generate_transformations_by_platform(transformations):
    """
    Args:
        transformations: All transformation metadata

    Returns:
        JSON content for transformations-by-platform.json
    """
    by_platform = defaultdict(list)

    for transform in transformations:
        platform = transform['platform']
        by_platform[platform].append({
            'uid': transform['uid'],
            'name': transform['name'],
            'is_disabled': transform['is_disabled']
        })

    # Sort within each platform
    for platform in by_platform:
        by_platform[platform].sort(key=lambda t: t['name'])

    return dict(by_platform)
```

**Update Frequency:** On transformation changes

**Purpose:** Fast lookup for "show me all Snowflake transformations"

---

### indices/queries/most-connected-nodes.json

**Purpose:** Pre-computed query: most connected tables and transformations

**Data Source:** Computed from graph

**Generation Logic:**
```python
def generate_most_connected_nodes(tables, transformations):
    """
    Args:
        tables: All table metadata with dependencies
        transformations: All transformation metadata with dependencies

    Returns:
        JSON content for most-connected-nodes.json
    """
    nodes = []

    # Score tables by connection count
    for table in tables:
        connection_count = (
            len(table['dependencies']['consumed_by']) +
            len(table['dependencies']['produced_by'])
        )
        if connection_count > 0:
            nodes.append({
                'uid': table['uid'],
                'name': table['name'],
                'type': 'table',
                'connections': connection_count,
                'consumed_by_count': len(table['dependencies']['consumed_by']),
                'produced_by_count': len(table['dependencies']['produced_by'])
            })

    # Score transformations by connection count
    for transform in transformations:
        connection_count = (
            len(transform['dependencies']['consumes']) +
            len(transform['dependencies']['produces'])
        )
        if connection_count > 0:
            nodes.append({
                'uid': transform['uid'],
                'name': transform['name'],
                'type': 'transformation',
                'connections': connection_count,
                'input_count': len(transform['dependencies']['consumes']),
                'output_count': len(transform['dependencies']['produces'])
            })

    # Sort by connection count, take top 50
    nodes.sort(key=lambda n: n['connections'], reverse=True)

    return {'nodes': nodes[:50]}
```

**Update Frequency:** On graph changes

**Purpose:** Identify critical tables/transformations, impact analysis

---

## 🤖 AI Directory

### ai/README.md

**Purpose:** Complete guide for AI agents to understand and analyze the project

**Data Source:** Generated from project metadata + static content

**Generation Logic:**
```python
def generate_ai_readme(manifest_extended, project_data):
    """
    Args:
        manifest_extended: Project statistics
        project_data: From /v2/storage/tokens/verify

    Returns:
        Markdown content for ai/README.md
    """
    stats = manifest_extended['statistics']
    owner = project_data['owner']

    return f"""# Keboola Project AI Guide

## 🎯 Quick Start for AI Agents
This is a Keboola data platform project containing transformations, tables, and data pipelines.

## 📊 Project Overview
- **Project ID:** {owner['id']}
- **Project Name:** {owner['name']}
- **Region:** {owner['region']}
- **Backend:** {detect_primary_backend(owner['features'])}
- **Total Tables:** {stats['total_tables']}
- **Total Transformations:** {stats['total_transformations']}
- **Data Sources:** {stats.get('total_sources', 'N/A')}

## 🏗️ Project Structure
[Generated structure guide]

## 🔍 How to Analyze This Project
[Analysis methods]

## 🔗 Keboola API Quick Reference
[API endpoints and examples]

## 🤖 Best Practices for AI Analysis
[Do's and don'ts]

...
"""
```

**Update Frequency:** On structure changes or project metadata changes

**Content Sections:**
1. Quick start
2. Project overview (real stats)
3. Structure guide
4. Analysis methods
5. API reference
6. Best practices
7. Platform-specific notes
8. Troubleshooting
9. Examples

**Purpose:** Provide complete context for AI agents without API access

---

## 🔄 Complete Generation Flow

### Main Generation Function

```python
def generate_twin_format(project_id, branch_id='main', security_config=None):
    """
    Complete twin format generation from Keboola project

    Args:
        project_id: Keboola project ID
        branch_id: Branch ID (default: 'main')
        security_config: Security settings (encrypt secrets, public repo, etc.)

    Steps:
        1. Initialize API client
        2. Fetch project metadata
        3. Fetch all buckets and tables
        4. Fetch all component configs
        5. Compute lineage graph
        5.5. Fetch job history and link to transformations
        6. Generate all index files (with job execution data)
        7. Generate individual metadata files (with job execution data)
        8. Generate samples (if enabled)
        9. Generate job history
        10. Generate AI documentation

    Output:
        Complete twin format directory structure
    """

    # Step 1: Initialize
    api = KeboolaStorageAPI(token=get_token_for_project(project_id))
    output_dir = Path('twin_format')

    # Step 2: Project metadata
    print("Fetching project metadata...")
    project_data = api.get('/v2/storage/tokens/verify')

    # Step 3: Buckets and tables
    print("Fetching buckets and tables...")
    buckets = api.get('/v2/storage/buckets')
    tables = []
    for bucket in buckets:
        for table_id in bucket['tables']:
            table_data = api.get(f'/v2/storage/tables/{table_id}?include=columns,metadata')
            tables.append(table_data)

    # Step 4: Components
    print("Fetching component configurations...")
    components_index = api.get('/v2/storage?exclude=componentDetails')
    component_configs = fetch_all_component_configs(api, branch_id)

    # Step 5: Compute graph
    print("Computing lineage graph...")
    transformations = extract_transformations(component_configs)
    graph_edges = compute_lineage_graph(transformations)

    # Step 5.5: Fetch job history early for transformation linking
    print("Fetching job history...")
    jobs = api.get('/v2/storage/jobs?limit=100')

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

    # Step 6: Generate indices
    print("Generating index files...")
    write_json(output_dir / 'buckets/index.json',
               generate_buckets_index(buckets))
    write_json(output_dir / 'transformations/index.json',
               generate_transformations_index(transformations, jobs))
    write_json(output_dir / 'components/index.json',
               generate_components_index(component_configs))

    # Step 7: Generate metadata files
    print("Generating metadata files...")
    for table in tables:
        path = get_table_metadata_path(output_dir, table)
        write_json(path, generate_table_metadata(table, graph_edges))

    for transform in transformations:
        # Get latest job for this transformation
        job_key = f"{transform['component_id']}:{transform['name']}"
        latest_job = transformation_jobs.get(job_key)

        path = get_transform_metadata_path(output_dir, transform)
        write_json(path, generate_transformation_metadata(transform, graph_edges, latest_job))

    for config in component_configs:
        path = get_component_metadata_path(output_dir, config)
        write_json(path, generate_component_metadata(config, config['component']['type']))

    # Step 8: Generate samples (if enabled)
    if security_config['exportDataSamples'] and not security_config['isPublicRepo']:
        print("Generating data samples...")
        for table in tables:
            if should_sample_table(table):
                sample_csv = fetch_table_sample(api, table['id'])
                path = get_sample_path(output_dir, table)
                write_file(path / 'sample.csv', sample_csv)
                write_json(path / 'metadata.json',
                          generate_sample_metadata(table['id'], sample_csv, table))

    # Step 9: Generate job history (already fetched in step 5.5)
    print("Generating job history...")
    write_json(output_dir / 'jobs/index.json',
               generate_jobs_index(jobs))

    for job in jobs:
        write_json(output_dir / f'jobs/recent/{job["id"]}.json',
                  generate_job_metadata(job))

    # Generate latest job per component (including transformations)
    generate_component_latest_jobs(output_dir, jobs)

    # Step 10: Generate root files
    print("Generating manifests and documentation...")
    write_yaml(output_dir / 'manifest.yaml',
               generate_manifest_yaml(project_data, security_config))
    write_json(output_dir / 'manifest-extended.json',
               generate_manifest_extended(project_data, buckets, transformations, sources))
    write_file(output_dir / 'README.md',
               generate_readme(manifest_extended))
    write_file(output_dir / 'ai/README.md',
               generate_ai_readme(manifest_extended, project_data))

    # Step 11: Generate graph and queries
    print("Generating lineage graph and queries...")
    write_jsonl(output_dir / 'indices/graph.jsonl',
                generate_graph_jsonl(transformations))
    write_json(output_dir / 'indices/sources.json',
               generate_sources_registry(buckets, component_configs))
    write_json(output_dir / 'indices/queries/tables-by-source.json',
               generate_tables_by_source(tables))
    write_json(output_dir / 'indices/queries/transformations-by-platform.json',
               generate_transformations_by_platform(transformations))
    write_json(output_dir / 'indices/queries/most-connected-nodes.json',
               generate_most_connected_nodes(tables, transformations))

    print(f"✅ Twin format generated successfully in {output_dir}/")
```

---

## 🔑 API Endpoints Summary

| Purpose | Endpoint | Frequency |
|---------|----------|-----------|
| Project info | `GET /v2/storage/tokens/verify` | Once per sync |
| Buckets list | `GET /v2/storage/buckets` | Once per sync |
| Table details | `GET /v2/storage/tables/{id}?include=columns,metadata` | Per table |
| Table sample | `GET /v2/storage/tables/{id}/data-preview?limit=100` | Per table (if enabled) |
| Components | `GET /v2/storage?exclude=componentDetails` | Once per sync |
| Component configs | `GET /v2/storage/components/{id}/configs` | Per component |
| Jobs list | `GET /v2/storage/jobs?limit=100` | Once per sync |
| Job details | `GET /v2/storage/jobs/{id}` | Per job (optional) |

---

## 🔒 Security Considerations

### Secret Encryption
```python
def encrypt_secrets(config):
    """
    Encrypt all secret fields (starting with #)

    Examples:
        #token → ***ENCRYPTED***
        #password → ***ENCRYPTED***
        #api_key → ***ENCRYPTED***
    """
    for key, value in config.items():
        if key.startswith('#'):
            config[key] = '***ENCRYPTED***'
    return config
```

### Public Repo Detection
```python
def should_export_samples(git_repo_config):
    """
    Samples are NEVER exported to public repositories

    Logic:
        if repo.is_public:
            return False  # Override any admin setting
        else:
            return admin_config.export_samples
    """
    if git_repo_config['is_public']:
        return False  # Safety override
    return git_repo_config.get('export_samples', True)
```

---

## 📅 Update Frequency

| Component | Update Trigger | Frequency |
|-----------|---------------|-----------|
| manifest-extended.json | Any change | Every sync |
| buckets/index.json | Bucket/table changes | Every sync |
| buckets/{bucket}/tables/{table}/metadata.json | Table changes | On change |
| transformations/index.json | Transform changes | Every sync |
| transformations/{name}/metadata.json | Transform changes | On change |
| components/index.json | Config changes | Every sync |
| components/{type}/{id}/metadata.json | Config changes | On change |
| storage/samples/ | Manual/scheduled | Daily or on-demand |
| jobs/index.json | Job completion | Hourly |
| jobs/recent/{id}.json | Job completion | On completion |
| indices/graph.jsonl | Transform changes | Every sync |
| indices/sources.json | Bucket/component changes | Every sync |
| indices/queries/ | Data changes | Every sync |
| ai/README.md | Structure changes | On structure change |

---

## ✅ Validation Checklist

Before deploying service, verify:

- [ ] All API endpoints tested and working
- [ ] Secret encryption working correctly
- [ ] Public repo detection prevents sample export
- [ ] Graph edges computed correctly
- [ ] Platform detection working for all transformation types
- [ ] Job metadata includes all required fields
- [ ] Samples respect 100-row limit
- [ ] Metadata files have correct structure
- [ ] Index files are sorted consistently
- [ ] AI guide contains real project data
- [ ] All paths use correct separators (/)
- [ ] JSON files are properly formatted
- [ ] YAML files have correct indentation
- [ ] CSV files follow RFC 4180
- [ ] JSONL files have one JSON per line

---

**This guide is the specification for service implementation. Follow it exactly for PRD compliance.**
