# indices/queries/ Directory

**Purpose:** Pre-computed query results for common analytical questions

**Why:** Instant answers without scanning directories or parsing graphs

---

## Files

### tables-by-source.json
**GENERATION:** Computed from buckets + table metadata
**PURPOSE:** Fast lookup for "show me all {source} tables"
**UPDATE:** Every sync
**API:** None (computed)

**Structure:**
```json
{
  "shopify": [
    {"uid": "table:shopify/orders", "name": "orders", "bucket": "shopify"}
  ],
  "hubspot": [...]
}
```

---

### transformations-by-platform.json
**GENERATION:** Computed from transformation metadata
**PURPOSE:** Fast lookup for "show me all {platform} transformations"
**UPDATE:** Every sync
**API:** None (computed)

**Structure:**
```json
{
  "snowflake": [
    {"uid": "transform:orders_clean", "name": "orders_clean", "is_disabled": false}
  ],
  "python": [...]
}
```

---

### most-connected-nodes.json
**GENERATION:** Computed from graph edges
**PURPOSE:** Identify critical tables/transformations for impact analysis
**UPDATE:** Every sync
**API:** None (computed)

**Structure:**
```json
{
  "nodes": [
    {
      "uid": "transform:orders_clean",
      "name": "orders_clean",
      "type": "transformation",
      "connections": 5,
      "input_count": 2,
      "output_count": 3
    }
  ]
}
```

Sorted by connection count (descending), top 50 nodes.

---

## Generation Algorithm

```python
def generate_query_files(tables, transformations, graph):
    """
    Generate all pre-computed query files

    Order doesn't matter - all are independent computations
    """
    # Group tables by source
    tables_by_source = defaultdict(list)
    for table in tables:
        tables_by_source[table['source']].append({
            'uid': table['uid'],
            'name': table['name'],
            'bucket': table['bucket']
        })

    # Group transformations by platform
    transforms_by_platform = defaultdict(list)
    for transform in transformations:
        transforms_by_platform[transform['platform']].append({
            'uid': transform['uid'],
            'name': transform['name'],
            'is_disabled': transform['is_disabled']
        })

    # Score nodes by connections
    nodes = score_and_rank_nodes(tables, transformations)

    # Write files
    write_json('tables-by-source.json', dict(tables_by_source))
    write_json('transformations-by-platform.json', dict(transforms_by_platform))
    write_json('most-connected-nodes.json', {'nodes': nodes[:50]})
```

---

**These files enable instant answers to common questions without scanning the entire project.**
