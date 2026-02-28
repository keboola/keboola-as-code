# Notification Subscriptions

The CLI supports managing notification subscriptions for configurations. Notifications alert you when specific events occur in your Keboola project.

## Overview

- **Config-level notifications**: Associate notifications with specific configurations
- **Events**: Job failures, successes, warnings, etc.
- **Channels**: Email or webhook delivery
- **Auto-populated filters**: Automatically scoped to branch, component, and configuration
- **Custom filters**: Add additional filtering conditions
- **Expiration**: Optional time-based subscription expiration

## File Structure

Notifications are stored in the `notifications/` subdirectory under each configuration:

```
extractor/keboola.orchestrator/my-config/
├── config.json
├── meta.json
├── description.md
└── notifications/
    ├── alert-on-failure/
    │   ├── meta.json
    │   └── notification.json
    └── success-webhook/
        ├── meta.json
        └── notification.json
```

Each notification has its own directory with two files:
- **meta.json**: Contains the notification name
- **notification.json**: Contains the notification configuration

## Notification Files

### meta.json

Contains the notification name used for directory naming:

```json
{
  "name": "alert-on-failure"
}
```

### notification.json

Contains the notification configuration:

```json
{
  "event": "job-failed",
  "recipient": {
    "channel": "email",
    "address": "alerts@example.com"
  }
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

- **filters** (array): Additional conditions beyond auto-populated filters
  - **field** (string): Field to filter on (see Valid Filter Fields below)
  - **operator** (string): Comparison operator - `==` (equals) or `!=` (not equals)
  - **value** (string): Value to compare against

- **expiresAt** (string): When subscription expires
  - Relative: `"+24 hours"`, `"+7 days"`
  - Absolute: RFC3339 timestamp `"2026-03-01T00:00:00Z"`

## Auto-Population

**Important:** The CLI automatically populates standard filters when pushing notifications to the API. You only need to specify the event and recipient.

When you push a notification, the CLI automatically adds these filters:

```json
{
  "field": "branch.id",
  "operator": "==",
  "value": "<your-branch-id>"
},
{
  "field": "job.component.id",
  "operator": "==",
  "value": "<your-component-id>"
},
{
  "field": "job.configuration.id",
  "operator": "==",
  "value": "<your-config-id>"
}
```

This ensures notifications are automatically scoped to the parent configuration. You can add custom filters to further refine when notifications are sent.

## Valid Filter Fields

Use these field names when adding custom filters:

| Field Name | Description |
|------------|-------------|
| `branch.id` | Branch identifier |
| `job.id` | Job identifier |
| `job.component.id` | Component identifier |
| `job.configuration.id` | Configuration identifier |
| `job.token.id` | Token identifier used for the job |
| `project.id` | Project identifier |
| `eventType` | Event type |

### Deprecated Field Names

The following field names are **deprecated** and will cause validation errors. Use the correct names instead:

| Deprecated | Correct |
|------------|---------|
| `configId` | `job.configuration.id` |
| `componentId` | `job.component.id` |
| `branchId` | `branch.id` |
| `jobId` | `job.id` |
| `tokenId` | `job.token.id` |
| `projectId` | `project.id` |

## Examples

### Minimal Notification (Recommended)

The simplest notification - auto-populated filters handle scoping:

```json
{
  "event": "job-failed",
  "recipient": {
    "channel": "email",
    "address": "team@example.com"
  }
}
```

### Webhook with Custom Filter

Add custom filters beyond the auto-populated ones:

```json
{
  "event": "job-succeeded",
  "recipient": {
    "channel": "webhook",
    "address": "https://hooks.slack.com/services/..."
  },
  "filters": [
    {
      "field": "job.token.id",
      "operator": "==",
      "value": "12345"
    }
  ]
}
```

The final API request will include both auto-populated filters (branch, component, config) **and** your custom filter (token).

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

**Note:** Pulled notifications use the subscription ID as the name (e.g., `01abc...`). You can rename the directory and update the name in `meta.json` for better readability.

### Push Notifications

```bash
kbc push
```

Creates/deletes notifications based on local changes:
- **New local notification**: Creates subscription via API with auto-populated filters
- **Deleted local notification**: Removes subscription from API
- **Modified notification**: Deletes old + creates new (no update API)

### Creating a New Notification

1. Create a directory under `{config}/notifications/`:
   ```bash
   mkdir my-config/notifications/alert-on-failure
   ```

2. Create `meta.json` with a descriptive name:
   ```json
   {
     "name": "alert-on-failure"
   }
   ```

3. Create `notification.json` with event and recipient:
   ```json
   {
     "event": "job-failed",
     "recipient": {
       "channel": "email",
       "address": "alerts@example.com"
     }
   }
   ```

4. Push to create:
   ```bash
   kbc push
   ```

The API will assign a subscription ID, which is tracked in the manifest.

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
          "id": "01abc...",
          "path": "notifications/alert-on-failure"
        },
        {
          "id": "01def...",
          "path": "notifications/success-webhook"
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

Filters are validated for:
- **Structure**: Using the SDK validation
- **Field names**: Checking against valid and deprecated field names

Invalid field names will cause a clear error message with suggestions.

### Directory Naming

- **New notifications**: Use descriptive names (e.g., `alert-on-failure`, `success-webhook`)
- **Pulled notifications**: Use subscription ID initially (can be renamed)
- The manifest tracks the mapping between directory name and API subscription ID

## Migration from Old Format

If you have notifications using the old format (everything in `meta.json`):

1. **Pull fresh state**: Run `kbc pull` to get the latest format
2. **Verify**: Check that notifications have both `meta.json` and `notification.json`
3. **Update field names**: Replace deprecated field names with correct ones
4. **Remove redundant filters**: Since filters are auto-populated, you can remove the standard branch/component/config filters

## Troubleshooting

### Notification not created during push

- Check that both `meta.json` and `notification.json` exist
- Verify `meta.json` has a valid name
- Verify `notification.json` has valid JSON syntax
- Check that `event` is a valid event type
- Ensure `recipient.channel` is `email` or `webhook`

### Invalid filter field name error

Error message will indicate the deprecated field name and suggest the correct one:

```
filter[0] uses deprecated field name "configId". Use "job.configuration.id" instead
```

Update your `notification.json` to use the correct field name.

### API 400 Bad Request

If you get a 400 error during push, check:
- Filter field names are correct (see Valid Filter Fields)
- Operator is `==` or `!=`
- Values are properly formatted strings

### Notification deleted unexpectedly

- Verify both `meta.json` and `notification.json` exist
- Check manifest includes the notification
- Ensure no `.gitignore` excludes notification files

## Best Practices

1. **Minimal configuration**: Let auto-population handle standard filters - only add custom filters when needed
2. **Descriptive names**: Use clear directory names like `alert-on-failure` instead of IDs
3. **Set expiration**: For temporary alerts during incidents/migrations
4. **Test webhooks**: Verify webhook URLs before deploying
5. **Version control**: Commit notification changes with config changes
6. **Use correct field names**: Always use the dotted notation (e.g., `job.configuration.id`)

## See Also

- [Keboola Notification Documentation](https://help.keboola.com/management/notifications/)
- [Keboola API Documentation](https://developers.keboola.com/overview/api/)
- CLI State Management (CLI_CONTEXT.md)
- Configuration Management (configs.md)
