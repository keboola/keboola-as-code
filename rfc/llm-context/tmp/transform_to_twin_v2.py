#!/usr/bin/env python3
"""
Transform Keboola Git Sync to AI-optimized twin format v2.

Improvements based on agent feedback:
1. Central aggregated manifest (manifest-extended.json)
2. Directory-level summaries (index.json)
3. Consolidated metadata (metadata.json instead of model.json + brief.md)
4. Enhanced graph with summary header
5. Source registry (indices/sources.json)
6. Pre-computed queries (indices/queries/)
"""

import json
import os
import re
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple
from datetime import datetime


class TwinFormatTransformerV2:
    def __init__(self, source_branch: str = "main", output_dir: str = "twin_format"):
        self.source_branch = Path(source_branch)
        self.output_dir = Path(output_dir)

        # Data structures
        self.tables: Dict[Tuple[str, str], dict] = {}  # (bucket, table) -> info
        self.transformations: Dict[str, dict] = {}
        self.graph_edges: List[dict] = []
        self.buckets: Dict[str, dict] = defaultdict(lambda: {'tables': [], 'source': None})
        self.sources: Dict[str, dict] = defaultdict(lambda: {
            'type': 'extractor',
            'instances': 0,
            'buckets': set(),
            'table_count': 0
        })

    def parse_table_reference(self, table_ref: str) -> Tuple[str, str, str]:
        """Parse table reference like 'in.c-bucket.table' or 'out.c-bucket.table'"""
        parts = table_ref.split('.')
        if len(parts) >= 3:
            stage = parts[0]
            bucket = parts[1]
            table = '.'.join(parts[2:])
            return stage, bucket, table
        return None, None, None

    def infer_source_from_bucket(self, bucket: str) -> str:
        """Infer source system from bucket name."""
        bucket_lower = bucket.lower()

        # Common patterns
        source_patterns = {
            'shopify': ['shopify', 'ex-shopify'],
            'hubspot': ['hubspot', 'ex-hubspot', 'crm'],
            'google-analytics': ['ga4', 'analytics', 'ex-google-analytics'],
            'google-drive': ['drive', 'ex-google-drive'],
            'salesforce': ['salesforce', 'ex-salesforce'],
            'facebook': ['facebook', 'ex-facebook'],
            'mysql': ['mysql', 'ex-mysql', 'ex-db-mysql'],
            'postgres': ['postgres', 'postgresql', 'ex-db-pgsql'],
            'snowflake': ['snowflake'],
            'bigquery': ['bigquery'],
            'stripe': ['stripe'],
            'jira': ['jira'],
            'slack': ['slack'],
            'zendesk': ['zendesk'],
        }

        for source, patterns in source_patterns.items():
            if any(pattern in bucket_lower for pattern in patterns):
                return source

        # If starts with common prefixes
        if bucket_lower.startswith('in.c-'):
            return 'storage-input'
        if bucket_lower.startswith('out.c-'):
            return 'storage-output'

        return 'unknown'

    def scan_transformations(self):
        """Scan all transformation configs and extract table references."""
        transform_base = self.source_branch / "transformation"

        if not transform_base.exists():
            print(f"⚠️  No transformation directory found at {transform_base}")
            return

        for config_file in transform_base.rglob("config.json"):
            transform_path = config_file.parent
            transform_name = transform_path.name
            component_id = transform_path.parent.name

            # Skip variable definitions and default values (they're not actual transformations)
            if transform_name == 'variables' or transform_name == 'default':
                continue
            # Skip if in variables/values/ path
            if 'variables/values' in str(transform_path):
                continue

            # Extract component type/platform with comprehensive detection
            platform = 'unknown'
            component_id_lower = component_id.lower()

            # SQL-based platforms
            if 'snowflake' in component_id_lower:
                platform = 'snowflake'
            elif 'redshift' in component_id_lower:
                platform = 'redshift'
            elif 'bigquery' in component_id_lower:
                platform = 'bigquery'
            elif 'synapse' in component_id_lower:
                platform = 'synapse'
            elif 'mssql' in component_id_lower or 'sql-server' in component_id_lower:
                platform = 'mssql'
            elif 'mysql' in component_id_lower:
                platform = 'mysql'
            elif 'postgres' in component_id_lower or 'pgsql' in component_id_lower:
                platform = 'postgresql'
            elif 'oracle' in component_id_lower:
                platform = 'oracle'
            elif 'exasol' in component_id_lower:
                platform = 'exasol'
            elif 'duckdb' in component_id_lower:
                platform = 'duckdb'
            # Programming language platforms
            elif 'python' in component_id_lower:
                platform = 'python'
            elif '.r-transformation' in component_id_lower:
                platform = 'r'
            elif 'julia' in component_id_lower:
                platform = 'julia'
            # Framework platforms
            elif 'dbt' in component_id_lower:
                platform = 'dbt'
            elif 'spark' in component_id_lower:
                platform = 'spark'
            # Generic SQL
            elif 'sql' in component_id_lower or 'transformation' in component_id_lower:
                platform = 'sql'

            # Read meta.json
            meta_file = transform_path / "meta.json"
            if meta_file.exists():
                with open(meta_file, 'r') as f:
                    meta = json.load(f)
                    transform_display_name = meta.get('name', transform_name)
                    is_disabled = meta.get('isDisabled', False)
            else:
                transform_display_name = transform_name
                is_disabled = False

            # Read config.json
            with open(config_file, 'r') as f:
                config = json.load(f)

            # Read description.md
            desc_file = transform_path / "description.md"
            description = ""
            if desc_file.exists():
                with open(desc_file, 'r') as f:
                    description = f.read().strip()

            # Extract storage mappings
            storage = config.get('storage', {})
            input_tables = storage.get('input', {}).get('tables', [])
            output_tables = storage.get('output', {}).get('tables', [])

            # Store transformation info
            transform_id = f"transform:{transform_name}"
            self.transformations[transform_id] = {
                'uid': transform_id,
                'name': transform_display_name,
                'type': 'transformation',
                'platform': platform,
                'original_path': str(transform_path.relative_to(self.source_branch)),
                'component_id': component_id,
                'is_disabled': is_disabled,
                'description': description,
                'input_tables': [],
                'output_tables': [],
                'dependencies': {
                    'consumes': [],
                    'produces': []
                }
            }

            # Process input tables
            for inp in input_tables:
                source = inp.get('source', '')
                stage, bucket, table = self.parse_table_reference(source)
                if bucket and table:
                    table_key = (bucket, table)
                    table_id = f"table:{bucket}/{table}"

                    # Track table
                    if table_key not in self.tables:
                        source_system = self.infer_source_from_bucket(bucket)
                        bucket_clean = bucket.replace('c-', '')

                        self.tables[table_key] = {
                            'uid': table_id,
                            'name': table,
                            'type': 'table',
                            'bucket': bucket,
                            'bucket_clean': bucket_clean,
                            'source': source_system,
                            'consumed_by': [],
                            'produced_by': [],
                            'description': f"Table {table} from {source_system}"
                        }

                        self.buckets[bucket_clean]['tables'].append(table)
                        if not self.buckets[bucket_clean]['source']:
                            self.buckets[bucket_clean]['source'] = source_system

                        # Track source
                        self.sources[source_system]['buckets'].add(bucket_clean)
                        self.sources[source_system]['table_count'] += 1

                    self.tables[table_key]['consumed_by'].append(transform_id)
                    self.transformations[transform_id]['input_tables'].append(table_id)
                    self.transformations[transform_id]['dependencies']['consumes'].append(table_id)

                    # Add graph edge
                    self.graph_edges.append({
                        'source': table_id,
                        'target': transform_id,
                        'type': 'consumed_by'
                    })

            # Process output tables
            for out in output_tables:
                dest = out.get('destination', '')
                stage, bucket, table = self.parse_table_reference(dest)
                if bucket and table:
                    table_key = (bucket, table)
                    table_id = f"table:{bucket}/{table}"

                    # Track table
                    if table_key not in self.tables:
                        source_system = self.infer_source_from_bucket(bucket)
                        bucket_clean = bucket.replace('c-', '')

                        self.tables[table_key] = {
                            'uid': table_id,
                            'name': table,
                            'type': 'table',
                            'bucket': bucket,
                            'bucket_clean': bucket_clean,
                            'source': source_system,
                            'consumed_by': [],
                            'produced_by': [],
                            'description': f"Table {table} produced by transformation"
                        }

                        self.buckets[bucket_clean]['tables'].append(table)
                        if not self.buckets[bucket_clean]['source']:
                            self.buckets[bucket_clean]['source'] = source_system

                        # Track source
                        self.sources[source_system]['buckets'].add(bucket_clean)
                        self.sources[source_system]['table_count'] += 1

                    self.tables[table_key]['produced_by'].append(transform_id)
                    self.transformations[transform_id]['output_tables'].append(table_id)
                    self.transformations[transform_id]['dependencies']['produces'].append(table_id)

                    # Add graph edge
                    self.graph_edges.append({
                        'source': transform_id,
                        'target': table_id,
                        'type': 'produces'
                    })

    def create_twin_structure(self):
        """Create the AI-optimized twin format structure."""
        self.output_dir.mkdir(exist_ok=True)

        # 1. Create buckets with consolidated metadata
        buckets_dir = self.output_dir / "buckets"
        buckets_dir.mkdir(exist_ok=True)

        bucket_summaries = []

        for bucket_clean, bucket_info in sorted(self.buckets.items()):
            table_count = len(bucket_info['tables'])
            source = bucket_info['source']

            bucket_summaries.append({
                'name': bucket_clean,
                'source': source,
                'table_count': table_count,
                'tables': sorted(bucket_info['tables'])
            })

            for table in sorted(bucket_info['tables']):
                # Find full table info
                table_data = None
                for (bucket, tbl), data in self.tables.items():
                    if data['bucket_clean'] == bucket_clean and tbl == table:
                        table_data = data
                        break

                if not table_data:
                    continue

                table_dir = buckets_dir / bucket_clean / "tables" / table
                table_dir.mkdir(parents=True, exist_ok=True)

                # Create consolidated metadata.json (replaces model.json + brief.md)
                metadata = {
                    'uid': table_data['uid'],
                    'name': table_data['name'],
                    'type': 'table',
                    'bucket': table_data['bucket'],
                    'source': table_data['source'],
                    'description': table_data['description'],
                    'dependencies': {
                        'consumed_by': table_data['consumed_by'],
                        'produced_by': table_data['produced_by']
                    }
                }

                metadata_file = table_dir / "metadata.json"
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)

        # Create buckets index
        buckets_index = {
            'total_buckets': len(self.buckets),
            'by_source': {},
            'buckets': bucket_summaries
        }

        # Group by source
        source_stats = defaultdict(lambda: {'count': 0, 'total_tables': 0})
        for bucket_summary in bucket_summaries:
            source = bucket_summary['source']
            source_stats[source]['count'] += 1
            source_stats[source]['total_tables'] += bucket_summary['table_count']

        buckets_index['by_source'] = dict(source_stats)

        with open(buckets_dir / "index.json", 'w', encoding='utf-8') as f:
            json.dump(buckets_index, f, indent=2, ensure_ascii=False)

        # 2. Create transformations with consolidated metadata
        transforms_dir = self.output_dir / "transformations"
        transforms_dir.mkdir(exist_ok=True)

        transform_summaries = []

        for transform_id, transform_info in sorted(self.transformations.items()):
            transform_name = transform_info['name']
            transform_dir_name = re.sub(r'[^\w\-]', '_', transform_name.lower())
            transform_dir = transforms_dir / transform_dir_name
            transform_dir.mkdir(exist_ok=True)

            # Create consolidated metadata.json
            metadata = {
                'uid': transform_info['uid'],
                'name': transform_info['name'],
                'type': 'transformation',
                'platform': transform_info['platform'],
                'component_id': transform_info['component_id'],
                'is_disabled': transform_info['is_disabled'],
                'description': transform_info['description'],
                'original_path': transform_info['original_path'],
                'dependencies': transform_info['dependencies']
            }

            metadata_file = transform_dir / "metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            transform_summaries.append({
                'uid': transform_info['uid'],
                'name': transform_info['name'],
                'platform': transform_info['platform'],
                'is_disabled': transform_info['is_disabled'],
                'input_count': len(transform_info['input_tables']),
                'output_count': len(transform_info['output_tables'])
            })

        # Create transformations index
        platform_stats = Counter(t['platform'] for t in transform_summaries)

        transforms_index = {
            'total_transformations': len(self.transformations),
            'by_platform': dict(platform_stats),
            'transformations': transform_summaries
        }

        with open(transforms_dir / "index.json", 'w', encoding='utf-8') as f:
            json.dump(transforms_index, f, indent=2, ensure_ascii=False)

        # 3. Create indices directory
        indices_dir = self.output_dir / "indices"
        indices_dir.mkdir(exist_ok=True)

        # Enhanced graph with meta header
        graph_file = indices_dir / "graph.jsonl"
        with open(graph_file, 'w', encoding='utf-8') as f:
            # Meta header
            meta = {
                '_meta': {
                    'total_edges': len(self.graph_edges),
                    'total_nodes': len(self.tables) + len(self.transformations),
                    'total_tables': len(self.tables),
                    'total_transformations': len(self.transformations),
                    'sources': len(self.sources),
                    'updated': datetime.now().isoformat()
                }
            }
            f.write(json.dumps(meta) + '\n')

            # Edges
            for edge in self.graph_edges:
                f.write(json.dumps(edge) + '\n')

        # Source registry
        sources_list = []
        for source_name, source_info in sorted(self.sources.items()):
            sources_list.append({
                'id': source_name,
                'name': source_name.replace('-', ' ').title(),
                'type': source_info['type'],
                'instances': source_info['instances'] or len(source_info['buckets']),
                'total_tables': source_info['table_count'],
                'buckets': sorted(list(source_info['buckets']))
            })

        sources_registry = {'sources': sources_list}
        with open(indices_dir / "sources.json", 'w', encoding='utf-8') as f:
            json.dump(sources_registry, f, indent=2, ensure_ascii=False)

        # 4. Pre-computed queries
        queries_dir = indices_dir / "queries"
        queries_dir.mkdir(exist_ok=True)

        # Tables by source
        tables_by_source = defaultdict(list)
        for table_info in self.tables.values():
            tables_by_source[table_info['source']].append({
                'uid': table_info['uid'],
                'name': table_info['name'],
                'bucket': table_info['bucket_clean']
            })

        with open(queries_dir / "tables-by-source.json", 'w', encoding='utf-8') as f:
            json.dump(dict(tables_by_source), f, indent=2, ensure_ascii=False)

        # Transformations by platform
        transforms_by_platform = defaultdict(list)
        for transform_info in self.transformations.values():
            transforms_by_platform[transform_info['platform']].append({
                'uid': transform_info['uid'],
                'name': transform_info['name'],
                'is_disabled': transform_info['is_disabled']
            })

        with open(queries_dir / "transformations-by-platform.json", 'w', encoding='utf-8') as f:
            json.dump(dict(transforms_by_platform), f, indent=2, ensure_ascii=False)

        # Most connected nodes
        node_connections = []
        for table_info in self.tables.values():
            connection_count = len(table_info['consumed_by']) + len(table_info['produced_by'])
            if connection_count > 0:
                node_connections.append({
                    'uid': table_info['uid'],
                    'name': table_info['name'],
                    'type': 'table',
                    'connections': connection_count,
                    'consumed_by_count': len(table_info['consumed_by']),
                    'produced_by_count': len(table_info['produced_by'])
                })

        for transform_info in self.transformations.values():
            connection_count = len(transform_info['input_tables']) + len(transform_info['output_tables'])
            if connection_count > 0:
                node_connections.append({
                    'uid': transform_info['uid'],
                    'name': transform_info['name'],
                    'type': 'transformation',
                    'connections': connection_count,
                    'input_count': len(transform_info['input_tables']),
                    'output_count': len(transform_info['output_tables'])
                })

        node_connections.sort(key=lambda x: x['connections'], reverse=True)

        with open(queries_dir / "most-connected-nodes.json", 'w', encoding='utf-8') as f:
            json.dump({'nodes': node_connections[:50]}, f, indent=2, ensure_ascii=False)

        # 5. Central aggregated manifest
        manifest_extended = {
            'project_id': self._get_project_id(),
            'twin_version': 1,
            'format_version': 2,
            'updated': datetime.now().isoformat(),
            'statistics': {
                'total_buckets': len(self.buckets),
                'total_tables': len(self.tables),
                'total_transformations': len(self.transformations),
                'total_edges': len(self.graph_edges)
            },
            'sources': sources_list,
            'transformation_platforms': dict(platform_stats)
        }

        with open(self.output_dir / "manifest-extended.json", 'w', encoding='utf-8') as f:
            json.dump(manifest_extended, f, indent=2, ensure_ascii=False)

        # 6. Simple manifest.yaml
        manifest_file = self.output_dir / "manifest.yaml"
        with open(manifest_file, 'w') as f:
            f.write("twin_version: 1\n")
            f.write(f"project_id: {manifest_extended['project_id']}\n")

        # 7. README.md
        readme_file = self.output_dir / "README.md"
        with open(readme_file, 'w') as f:
            f.write("# Keboola Project (Twin Format v2)\n\n")
            f.write("AI-optimized structure for efficient project analysis.\n\n")
            f.write("## Quick Start\n\n")
            f.write("**Read this first:** `manifest-extended.json` - Complete project overview in one file.\n\n")
            f.write("## Statistics\n\n")
            f.write(f"- **Tables:** {len(self.tables)}\n")
            f.write(f"- **Transformations:** {len(self.transformations)}\n")
            f.write(f"- **Buckets:** {len(self.buckets)}\n")
            f.write(f"- **Sources:** {len(self.sources)}\n")
            f.write(f"- **Graph edges:** {len(self.graph_edges)}\n\n")
            f.write("## Structure\n\n")
            f.write("### Core Files\n")
            f.write("- `manifest-extended.json` - **START HERE** - Complete project overview\n")
            f.write("- `manifest.yaml` - Simple project config\n\n")
            f.write("### Directories\n")
            f.write("- `buckets/` - Storage buckets and tables\n")
            f.write("  - `index.json` - Bucket directory summary\n")
            f.write("  - `{bucket}/tables/{table}/metadata.json` - Table metadata\n")
            f.write("- `transformations/` - Data transformations\n")
            f.write("  - `index.json` - Transformation directory summary\n")
            f.write("  - `{transform}/metadata.json` - Transformation metadata\n")
            f.write("- `indices/` - Project-wide indices\n")
            f.write("  - `graph.jsonl` - Complete lineage graph\n")
            f.write("  - `sources.json` - Source system registry\n")
            f.write("  - `queries/` - Pre-computed common queries\n\n")
            f.write("## Analysis Tips\n\n")
            f.write("1. **Project overview:** Read `manifest-extended.json` first (1 file)\n")
            f.write("2. **Find tables:** Check `buckets/index.json` or `indices/queries/tables-by-source.json`\n")
            f.write("3. **Find transformations:** Check `transformations/index.json` or `indices/queries/transformations-by-platform.json`\n")
            f.write("4. **Trace lineage:** Read `indices/graph.jsonl` or check `metadata.json` dependencies\n")
            f.write("5. **Source analysis:** Read `indices/sources.json`\n")

    def _get_project_id(self):
        """Get project ID from .keboola/project.json"""
        project_json = Path(".keboola/project.json")
        if project_json.exists():
            with open(project_json, 'r') as f:
                project_data = json.load(f)
                return project_data.get('project', {}).get('id', 'example')
        return 'example'

    def print_summary(self):
        """Print transformation summary."""
        print("\n" + "="*60)
        print("TRANSFORMATION SUMMARY (v2)")
        print("="*60)
        print(f"✓ Found {len(self.tables)} unique tables")
        print(f"✓ Found {len(self.transformations)} transformations")
        print(f"✓ Found {len(self.buckets)} buckets")
        print(f"✓ Found {len(self.sources)} data sources")
        print(f"✓ Created {len(self.graph_edges)} graph edges")
        print(f"\n✓ AI-optimized structure created in: {self.output_dir}/")
        print("\n📊 Agent Analysis Improvements:")
        print(f"   • manifest-extended.json - Complete overview in 1 file")
        print(f"   • buckets/index.json - {len(self.buckets)} buckets summary")
        print(f"   • transformations/index.json - {len(self.transformations)} transformations summary")
        print(f"   • indices/sources.json - {len(self.sources)} sources registry")
        print(f"   • indices/queries/ - 3 pre-computed query results")
        print("\nNext steps:")
        print("1. Review manifest-extended.json")
        print("2. Compare with updated _template/")
        print("3. Test with AI agents")

    def run(self):
        """Execute the full transformation."""
        print(f"Starting AI-optimized transformation v2: {self.source_branch} -> {self.output_dir}")
        print(f"\n[1/3] Scanning transformations...")
        self.scan_transformations()

        print(f"\n[2/3] Creating twin format structure...")
        self.create_twin_structure()

        print(f"\n[3/3] Done!")
        self.print_summary()


if __name__ == "__main__":
    transformer = TwinFormatTransformerV2(source_branch="main", output_dir="twin_format")
    transformer.run()
