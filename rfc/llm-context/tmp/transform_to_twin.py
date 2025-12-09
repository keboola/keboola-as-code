#!/usr/bin/env python3
"""
Transform Keboola Git Sync from component-based format to twin format.

This script:
1. Scans the 'main' branch for transformations and their table references
2. Extracts all unique tables (buckets/tables) from transformation configs
3. Creates the twin format structure:
   - buckets/{bucket}/tables/{table}/
   - transformations/{transform}/
   - indices/graph.jsonl
   - manifest.yaml
   - README.md
"""

import json
import os
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple


class TwinFormatTransformer:
    def __init__(self, source_branch: str = "main", output_dir: str = "twin_format"):
        self.source_branch = Path(source_branch)
        self.output_dir = Path(output_dir)
        self.tables: Set[Tuple[str, str]] = set()  # (bucket, table)
        self.transformations: Dict[str, dict] = {}
        self.graph_edges: List[dict] = []

    def parse_table_reference(self, table_ref: str) -> Tuple[str, str, str]:
        """
        Parse table reference like 'in.c-bucket.table' or 'out.c-bucket.table'
        Returns: (stage, bucket, table)
        """
        parts = table_ref.split('.')
        if len(parts) >= 3:
            stage = parts[0]  # 'in' or 'out'
            bucket = parts[1]  # 'c-bucket'
            table = '.'.join(parts[2:])  # 'table' (may contain dots)
            return stage, bucket, table
        return None, None, None

    def scan_transformations(self):
        """Scan all transformation configs and extract table references."""
        transform_base = self.source_branch / "transformation"

        if not transform_base.exists():
            print(f"⚠️  No transformation directory found at {transform_base}")
            return

        # Find all transformation config.json files
        for config_file in transform_base.rglob("config.json"):
            transform_path = config_file.parent
            transform_name = transform_path.name
            component_id = transform_path.parent.name

            # Read meta.json for transformation name
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
                'name': transform_display_name,
                'original_path': str(transform_path.relative_to(self.source_branch)),
                'component_id': component_id,
                'is_disabled': is_disabled,
                'description': description,
                'input_tables': [],
                'output_tables': []
            }

            # Process input tables
            for inp in input_tables:
                source = inp.get('source', '')
                stage, bucket, table = self.parse_table_reference(source)
                if bucket and table:
                    self.tables.add((bucket, table))
                    table_id = f"table:{bucket}/{table}"
                    self.transformations[transform_id]['input_tables'].append(table_id)
                    # Add graph edge: table -> transformation
                    self.graph_edges.append({
                        'from': table_id,
                        'to': transform_id,
                        'rel': 'consumed_by'
                    })

            # Process output tables
            for out in output_tables:
                dest = out.get('destination', '')
                stage, bucket, table = self.parse_table_reference(dest)
                if bucket and table:
                    self.tables.add((bucket, table))
                    table_id = f"table:{bucket}/{table}"
                    self.transformations[transform_id]['output_tables'].append(table_id)
                    # Add graph edge: transformation -> table
                    self.graph_edges.append({
                        'from': transform_id,
                        'to': table_id,
                        'rel': 'produces'
                    })

    def create_twin_structure(self):
        """Create the twin format directory structure and files."""
        self.output_dir.mkdir(exist_ok=True)

        # 1. Create buckets and tables
        buckets_dir = self.output_dir / "buckets"
        buckets_dir.mkdir(exist_ok=True)

        for bucket, table in sorted(self.tables):
            # Remove 'c-' prefix if present for cleaner bucket names
            bucket_clean = bucket.replace('c-', '')

            table_dir = buckets_dir / bucket_clean / "tables" / table
            table_dir.mkdir(parents=True, exist_ok=True)

            # Create model.json
            model_file = table_dir / "model.json"
            with open(model_file, 'w') as f:
                json.dump({"uid": f"table:{bucket}/{table}"}, f)

            # Create brief.md
            brief_file = table_dir / "brief.md"
            with open(brief_file, 'w') as f:
                f.write(f"# {bucket}.{table}\n\n")
                f.write("Table referenced in transformations.\n")

        # 2. Create transformations
        transforms_dir = self.output_dir / "transformations"
        transforms_dir.mkdir(exist_ok=True)

        for transform_id, transform_info in self.transformations.items():
            transform_name = transform_info['name']
            # Create safe directory name
            transform_dir_name = re.sub(r'[^\w\-]', '_', transform_name.lower())
            transform_dir = transforms_dir / transform_dir_name
            transform_dir.mkdir(exist_ok=True)

            # Create model.json
            model_file = transform_dir / "model.json"
            with open(model_file, 'w') as f:
                json.dump({"uid": transform_id}, f)

            # Create brief.md
            brief_file = transform_dir / "brief.md"
            with open(brief_file, 'w') as f:
                f.write(f"# {transform_name}\n\n")
                if transform_info['description']:
                    f.write(f"{transform_info['description']}\n\n")
                f.write(f"**Original path:** `{transform_info['original_path']}`\n\n")
                f.write(f"**Component:** `{transform_info['component_id']}`\n\n")
                if transform_info['is_disabled']:
                    f.write("**Status:** Disabled\n\n")

                if transform_info['input_tables']:
                    f.write("## Input Tables\n\n")
                    for table_id in transform_info['input_tables']:
                        f.write(f"- `{table_id}`\n")
                    f.write("\n")

                if transform_info['output_tables']:
                    f.write("## Output Tables\n\n")
                    for table_id in transform_info['output_tables']:
                        f.write(f"- `{table_id}`\n")
                    f.write("\n")

        # 3. Create indices/graph.jsonl
        indices_dir = self.output_dir / "indices"
        indices_dir.mkdir(exist_ok=True)

        graph_file = indices_dir / "graph.jsonl"
        with open(graph_file, 'w') as f:
            for edge in self.graph_edges:
                f.write(json.dumps(edge) + '\n')

        # 4. Create manifest.yaml
        manifest_file = self.output_dir / "manifest.yaml"
        with open(manifest_file, 'w') as f:
            f.write("twin_version: 1\n")
            # Try to get project_id from .keboola/project.json
            project_json = Path(".keboola/project.json")
            if project_json.exists():
                with open(project_json, 'r') as pf:
                    project_data = json.load(pf)
                    project_id = project_data.get('project', {}).get('id', 'example')
            else:
                project_id = 'example'
            f.write(f"project_id: {project_id}\n")

        # 5. Create README.md
        readme_file = self.output_dir / "README.md"
        with open(readme_file, 'w') as f:
            f.write("# Keboola Project (Twin Format)\n\n")
            f.write("This project has been transformed to the twin format.\n\n")
            f.write(f"## Statistics\n\n")
            f.write(f"- **Tables:** {len(self.tables)}\n")
            f.write(f"- **Transformations:** {len(self.transformations)}\n")
            f.write(f"- **Graph edges:** {len(self.graph_edges)}\n\n")
            f.write("## Structure\n\n")
            f.write("- `buckets/` - Storage buckets and tables\n")
            f.write("- `transformations/` - Data transformations\n")
            f.write("- `indices/graph.jsonl` - Lineage graph\n")
            f.write("- `manifest.yaml` - Project configuration\n")

    def print_summary(self):
        """Print transformation summary."""
        print("\n" + "="*60)
        print("TRANSFORMATION SUMMARY")
        print("="*60)
        print(f"✓ Found {len(self.tables)} unique tables")
        print(f"✓ Found {len(self.transformations)} transformations")
        print(f"✓ Created {len(self.graph_edges)} graph edges")
        print(f"\n✓ Twin format structure created in: {self.output_dir}/")
        print("\nNext steps:")
        print("1. Review the generated structure")
        print("2. Compare with _template/ to ensure consistency")
        print("3. Adjust if needed")
        print("4. Replace current structure with twin format")

    def run(self):
        """Execute the full transformation."""
        print(f"Starting transformation: {self.source_branch} -> {self.output_dir}")
        print(f"\n[1/3] Scanning transformations...")
        self.scan_transformations()

        print(f"\n[2/3] Creating twin format structure...")
        self.create_twin_structure()

        print(f"\n[3/3] Done!")
        self.print_summary()


if __name__ == "__main__":
    transformer = TwinFormatTransformer(source_branch="main", output_dir="twin_format")
    transformer.run()
