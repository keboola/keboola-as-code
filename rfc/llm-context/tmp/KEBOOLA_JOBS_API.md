# Keboola Jobs Queue API Documentation

## Overview

A job represents work being executed in Keboola. Jobs can be initiated through:
- The UI
- Scheduled Orchestrations/Flows
- Manually via API

This document describes how to run jobs programmatically using the Keboola Jobs Queue API.

---

## Getting Started

### Initial Setup Steps

1. **Create a component configuration**
   - Set up your component configuration in the Keboola UI
   - Run it manually to ensure it works correctly

2. **Review the successful job**
   - Examine the job details to identify the required parameters
   - Note the component ID and configuration ID

3. **Generate a Storage API token**
   - Create a token with appropriate restrictions
   - Limit token access to specific components for security

---

## API Endpoint

### Create and Run a Job

**Endpoint:** `POST https://queue.keboola.com/jobs`

**Important:** Use the correct endpoint for your Stack to avoid "Invalid access token" errors.

### Request Headers

| Header | Required | Description |
|--------|----------|-------------|
| `X-StorageApi-Token` | Yes | Your Storage API token |
| `Content-Type` | Yes | Must be `application/json` |

### Request Body Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mode` | string | Yes | Job execution mode (typically `"run"`) |
| `component` | string | Yes | Component ID (e.g., `keboola.ex-db-snowflake`) |
| `config` | string | Yes | Configuration ID |

---

## Examples

### Basic Job Execution

**cURL Example:**

```bash
curl -X POST https://queue.keboola.com/jobs \
  -H "X-StorageApi-Token: YOUR_TOKEN_HERE" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "run",
    "component": "keboola.ex-db-snowflake",
    "config": "493493"
  }'
```

**Request Body:**

```json
{
  "mode": "run",
  "component": "keboola.ex-db-snowflake",
  "config": "493493"
}
```

---

## Security Best Practices

- **Use restrictive tokens:** Create tokens limited to specific components only
- **Avoid sharing tokens:** Keep your Storage API tokens secure
- **Use correct endpoints:** Ensure you're using the endpoint for your specific Stack

---

## Additional Resources

- [Jobs API Documentation](https://developers.keboola.com/integrate/jobs/) - Complete Jobs API reference
- [Jobs Concept Documentation](https://developers.keboola.com/integrate/jobs/) - Understanding jobs in Keboola

---

## Common Issues

### Invalid Access Token Error

**Problem:** Receiving "Invalid access token" error
**Solution:** Verify you're using the correct endpoint for your Stack (connection URL)

### Component Not Found Error

**Problem:** Component ID not recognized
**Solution:** Double-check the component ID from a successful job run in the UI

### Configuration Not Found Error

**Problem:** Configuration ID not recognized
**Solution:** Verify the configuration ID exists and your token has access to it
