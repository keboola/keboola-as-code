# Ignore Configurations Orchestrator - Pull Test

## Purpose
This E2E test verifies that when a configuration is ignored via `.kbcignore`,
the system **validates** that no orchestrators reference that ignored configuration.
If an orchestrator references an ignored config, the operation fails with an error
instructing the user to explicitly add the orchestrator to `.kbcignore`.

## Test Setup
- **Remote state**: Contains 3 configs:
  1. `ex-generic-v2/empty` - ignored via .kbcignore
  2. `ex-generic-v2/without-rows` - not ignored
  3. `keboola.orchestrator/orchestrator` - references the "empty" config in Task 1

- **Local state**: Empty (initial pull)
- **.kbcignore**: Contains pattern to ignore the "empty" config (but NOT the orchestrator)

## Expected Behavior
- Pull operation **fails** with error code 1
- Error message instructs user to add the orchestrator to `.kbcignore`:
  ```
  Error: orchestrators reference ignored configurations:
    - orchestrator "..." references ignored config "...", please add the orchestrator to .kbcignore:
        keboola.orchestrator/<orchestrator-id>
  ```

## Fix
User must explicitly add the orchestrator to `.kbcignore`:
```
ex-generic-v2/%%TEST_BRANCH_MAIN_CONFIG_EMPTY_ID%%
keboola.orchestrator/%%TEST_BRANCH_MAIN_CONFIG_ORCHESTRATOR_ID%%
```

This ensures users are aware of orchestrator dependencies and make explicit ignore decisions.
