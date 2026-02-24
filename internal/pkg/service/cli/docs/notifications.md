# Notification Subscriptions

The CLI supports managing notification subscriptions for configurations. Notifications alert you when specific events occur in your Keboola project.

## Overview

- **Config-level notifications**: Associate notifications with specific configurations
- **Events**: Job failures, successes, warnings, etc.
- **Channels**: Email or webhook delivery
- **Filters**: Fine-grained control using field operators
- **Expiration**: Optional time-based subscription expiration

## File Structure

Notifications are stored in the `notifications/` subdirectory under each configuration:

```
extractor/keboola.orchestrator/my-config/
├── config.json
├── meta.json
├── description.md
└── notifications/
    ├── sub-abc123/
    │   └── meta.json
    └── sub-def456/
        └── meta.json
```

Each notification has its own directory named after the subscription ID.

## Notification File Format

**File:** `{config}/notifications/{subscription-id}/meta.json`

```json
{
  "event": "job-failed",
  "recipient": {
    "channel": "email",
    "address": "alerts@example.com"
  },
  "filters": [
    {
      "field": "configId",
      "operator": "eq",
      "value": "456"
    }
  ],
  "expiresAt": "+24 hours"
}
```

### Required Fields

- **event** (string): The event type to watch
  - `job-failed` - Job execution failed
  - `job-succeeded` - Job completed successfully
  - `job-warning` - Job completed with warnings
  - `job-processing-long` - Job running longer than expected

- **recipient** (object): Delivery target
  - **channel** (string): `email` or `webhook`
  - **address** (string): Email address or webhook URL

### Optional Fields

- **filters** (array): Conditions to match before sending notification
  - **field** (string): Field to filter on (e.g., `configId`, `branchId`, `tokenId`)
  - **operator** (string): Comparison operator
    - `eq` - Equals
    - `ne` - Not equals
    - `gt` - Greater than
    - `lt` - Less than
    - `ge` - Greater than or equal
    - `le` - Less than or equal
  - **value** (string): Value to compare against

- **expiresAt** (string): When subscription expires
  - Relative: `"+24 hours"`, `"+7 days"`
  - Absolute: RFC3339 timestamp `"2026-03-01T00:00:00Z"`

## Examples

### Email on Job Failure

```json
{
  "event": "job-failed",
  "recipient": {
    "channel": "email",
    "address": "team@example.com"
  }
}
```

### Webhook with Filters

```json
{
  "event": "job-succeeded",
  "recipient": {
    "channel": "webhook",
    "address": "https://hooks.slack.com/services/..."
  },
  "filters": [
    {
      "field": "configId",
      "operator": "eq",
      "value": "456"
    },
    {
      "field": "branchId",
      "operator": "eq",
      "value": "123"
    }
  ]
}
```

### Temporary Notification

```json
{
  "event": "job-processing-long",
  "recipient": {
    "channel": "email",
    "address": "oncall@example.com"
  },
  "expiresAt": "+7 days"
}
```

## CLI Operations

### Pull Notifications

```bash
kbc pull
```

Loads notifications from the API and saves them to local files. The manifest is updated with subscription IDs.

### Push Notifications

```bash
kbc push
```

Creates/deletes notifications based on local changes:
- **New local notification**: Creates subscription via API
- **Deleted local notification**: Removes subscription from API
- **Modified notification**: Deletes old + creates new (no update API)

### Sync Workflow

```bash
# Full round-trip
kbc pull                          # Load from API
# Edit notifications/{id}/meta.json
kbc push                          # Apply changes
```

## Manifest Format

The manifest tracks notification IDs for sync:

```json
{
  "configurations": [
    {
      "branchId": 123,
      "componentId": "keboola.orchestrator",
      "id": "456",
      "path": "extractor/keboola.orchestrator/my-config",
      "notifications": [
        {
          "id": "sub-abc123",
          "path": "notifications/sub-abc123"
        },
        {
          "id": "sub-def456",
          "path": "notifications/sub-def456"
        }
      ]
    }
  ]
}
```

## Important Notes

### No Update Operation

The notification API doesn't support updates. When you modify a notification:
1. CLI deletes the old subscription
2. CLI creates a new subscription
3. Manifest is updated with the new ID

This happens automatically during `push`.

### Branch-Level Notifications

Currently, only **config-level notifications** are supported. Branch-level notifications may be added in future versions.

### Filter Validation

Filters are validated using the SDK when loading. Invalid operators or field names will cause an error.

### Subscription IDs

After creating a notification, the API assigns a unique subscription ID. This ID is:
- Stored in the manifest
- Used as the directory name
- Required for delete operations

## Troubleshooting

### Notification not created during push

- Check that `meta.json` has valid JSON syntax
- Verify `event` is a valid event type
- Ensure `recipient.channel` is `email` or `webhook`
- Check that required fields are present

### Notification deleted unexpectedly

- Verify the notification directory exists locally
- Check manifest includes the notification
- Ensure no `.gitignore` excludes notification files

### Filter errors

- Use valid field names (`configId`, `branchId`, `tokenId`)
- Use supported operators (`eq`, `ne`, `gt`, `lt`, `ge`, `le`)
- Ensure values are strings in JSON

### Expiration not working

Currently limited by SDK - expiration handling is not fully supported in the create operation. This will be fixed in a future SDK release.

## Best Practices

1. **Use filters**: Narrow notifications to specific configs/branches to reduce noise
2. **Set expiration**: For temporary alerts during incidents/migrations
3. **Group by purpose**: Use descriptive directory names in comments
4. **Test webhooks**: Verify webhook URLs before deploying
5. **Version control**: Commit notification changes with config changes

## See Also

- [Keboola Notification API Documentation](https://keboolaconnection.docs.apiary.io/#reference/notification-api)
- CLI State Management (CLI_CONTEXT.md)
- Configuration Management (configs.md)
