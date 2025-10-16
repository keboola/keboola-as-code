# GitHub Actions Cache Cleanup

This document describes a workflow for automatic cleaning of GitHub Actions cache, which addresses the problem of continuously growing cache size.

## Problem

GitHub Actions cache tends to grow gradually because:
1. When code/libraries change in branches, existing cache is loaded (for example from the main branch)
2. Then new items are added to it (e.g., from 200MB to 300MB)
3. Subsequently, this enlarged cache is used again and more items are added (300MB → 400MB)
4. Cache never automatically reduces its size, it only grows

This leads to:
- Longer cache download/upload times
- Higher disk space usage
- Inefficient use of GitHub cache system

## Solution: gh-cache-cleanup.yml

Workflow `.github/workflows/gh-cache-cleanup.yml` automatically cleans GitHub Actions cache, removing old and duplicate entries.

### Key Features

1. **Two types of cleaning**:
   - **Standard cleaning** (every week) - Removes cache older than 14 days
   - **Thorough cleaning** (every 4th week) - Removes cache older than 7 days and deduplicates cache

2. **Automatic scheduling**:
   - Runs every Sunday at 1:00 UTC
   - Every 4th week performs more thorough cleaning

3. **Cache deduplication**:
   - Identifies duplicate caches with the same key patterns
   - Preserves only the newest version of each cache

4. **Test mode (dry run)**:
   - Enables safe testing without actual cache deletion
   - Shows what the workflow would do in a production environment

## How It Works

1. The workflow determines the current week number and identifies the cleaning type
2. Based on the cleaning type, it sets a time filter (14 or 7 days)
3. Lists the current cache status (count, size, largest cache)
4. Removes cache older than the set time limit
5. During thorough cleaning, it also identifies and removes duplicate caches
6. Finally reports the final status

## Configuration and Usage

### Automatic Execution

The workflow is set to run automatically every Sunday at 1:00 UTC. No additional configuration is needed.

### Manual Execution

1. Go to the **Actions** section in your repository
2. Find the "GitHub Cache Cleanup" workflow
3. Click on "Run workflow"
4. Optional: Set "Run in dry-run mode" (enabled by default)
5. Click on "Run workflow"

### Test Mode (dry run)

Test mode is available for manual execution:
- Shows all actions the workflow would perform
- But doesn't actually delete any cache
- Default option when manually executed

## Implementation Details

### Identifying Old Caches

```bash
# List caches older than the set time limit
CUTOFF_DATE=$(date -d "$DATE_FILTER days ago" "+%Y-%m-%d")
OLD_CACHES=$(gh cache list --limit 100 | awk -v date="$CUTOFF_DATE" '$3 <= date {print $1}')
```

### Cache Deduplication

The workflow uses the following logic to identify and remove duplicate caches:
1. Extracts key patterns (e.g., "linux-go-1.25-004-build-default")
2. For each pattern, finds all matching cache entries
3. Sorts them by creation date (newest first)
4. Keeps the newest cache, removes all others

## Testing

For safe testing of the workflow:

1. **Use test mode**: When manually executing, leave the "Run in dry-run mode" option checked
2. **Check logs**: Look at which caches would be deleted
3. **Verify behavior**: Make sure the workflow identifies the correct caches to remove

## Expected Results

After implementing this workflow:
- GitHub Actions cache size should remain stable
- Old and unused caches will be automatically removed
- Duplicate caches will be consolidated
- Overall efficiency of CI/CD processes should improve 
