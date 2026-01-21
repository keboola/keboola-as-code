# E2E Tests

E2E tests are divided by the service they test.

- **CLI**
  - Location: [`test/cli`](../test/cli)
  - Run all tests: `task tests-cli`
  - Run specific test: `task e2e -- test/cli/path/to/test`
- **Templates API**
  - Location: [`test/templates/api`](../test/templates/api)
  - Run all tests: `task tests-templates-api`
  - Run specific test: `task e2e -- test/templates/api/path/to/test`
- **Stream API**
  - Location: [`test/stream/api`](../test/stream/api)
  - Run all tests: `task tests-stream-api`
  - Run specific test: `task e2e -- test/stream/api/path/to/test`

Tests for each service are grouped according to a common functionality they test. Then each directory in this group 
contains a single test.

Example of single test directories:
- `test/stream/api/exports/create`
- `test/cli/create/branch`

## Test Directory Structure

### CLI

Running a test means executing the CLI binary with predefined arguments and comparing the working directory before and 
after the execution. The test also compares exit code, stdout, and stderr.

```
└─test-name
  ├─args                - file with command line arguments used when running the program
  │                      Supports multiple commands separated by newlines. Lines starting with "#" are ignored.
  │                      You can also add additional files matching `args*` (e.g., `args.2`, `args-extra`) which
  │                      are read in lexicographic order after `args`. All commands run sequentially in the same cwd.
  │                      The final exit code must be in `expected-code` and refers to the last command.
  │                      `expected-stderr` should contain stderr of all commands concatenated. `expected-stdout`
  │                      should contain stdout of all commands concatenated.
  ├─env                 - file with additional env vars to be injected to the program
  ├─expected-code       - file with expected exit code, eg. 0 for success
  ├─expected-stdout     - file with expected stdout
  ├─expected-stderr     - file with expected stderr
  ├─expected-state.json - optional, if present, the final project's state is compared with the state defined in the file
  ├─initial-state.json  - optional, if present, TEST_PROJECT will be set to defined state befor test
  ├─interaction.txt     - optional, if present, interactive input/output is executed according to the script
  ├─in                  - dir, initial state of the working directory before program execution
  └─out                 - dir, expected state of the working directory after program execution
```

### API

A test contains one or more API requests and running a test starts the API binary and sends the requests to it.

```
└─test-name
  └─001-call1                   - directory with an API request
    ├─expected-http-code        - file with expected HTTP response code, eg. 200 for success
    ├─expected-response.json    - file with expected response body in JSON format
    └─request.json              - HTTP request description
  ├─002-call2                   - another API request
  ├─...                         - there can be one or more of them, prefixed with an incremental number to call them in order  
  ├─expected-server-stderr      - file with expected stderr from the API server, optional
  ├─expected-server-stdout      - file with expected stdout from the API server, optional
  ├─expected-etcd-kvs.txt       - file with expected state of the etcd database after running the requests, optional
  ├─expected-state.json         - file with expected state of project after running the requests, optional
  └─initial-etcd-kvs.txt        - file with initial state of the etcd database before running the requests, optional
```

## `initial-state.json` and `expected-state.json`

Contain a snapshot of a project before or after the operations run in the test.

- `branches` - List of all branches and their configurations in the same format as the project manifests.
- `buckets` - List of all buckets and their tables in the project.
- `sandboxes` - List of all workspaces.
- `schedules` - List of all schedules.
- `backend` - Backend that the tests should run on only. Test fails when backend in TEST_KBC_PROJECTS has incompatible backend.
- `legacyTransformation` - Boolean indication that the stack supports legacyTransformation. For `gcp` stack the legacyTransformation has to be set always to `false`.
- `branches/.../configs` - List of all configs (by filename without extension) available in a branch. They are created from fixtures defined in [internal/pkg/fixtures/configs](https://github.com/keboola/keboola-as-code/tree/main/internal/pkg/fixtures/configs). The `name` field of the fixture is used to generate an environment variable which contains the ID of the config, e.g. `%%TEST_BRANCH_MAIN_CONFIG_EMPTY_ID%%` where `MAIN` is the branch, and `EMPTY` is the name.
  - You can get a list of the generated environment variables by running the test with `TEST_VERBOSE=true`, e.g. `TEST_VERBOSE=true TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh -run TestCliE2E/job`

Example:

```json
{
  "backend": {
    "type": "snowflake"
  },
  "legacyTransformation": true,
  "branches": [
    {
      "branch": {
        "name": "Main",
        "description": "",
        "isDefault": true
      },
      "configs": []
    }
  ],
  "buckets": [
    {
      "id": "in.c-bucket",
      "uri": "https://%%TEST_KBC_STORAGE_API_HOST%%/v2/storage/buckets/in.c-bucket",
      "displayName": "bucket",
      "description": "",
      "tables": [
        {
          "id": "in.c-bucket.table",
          "uri": "https://%%TEST_KBC_STORAGE_API_HOST%%/v2/storage/tables/in.c-bucket.table",
          "name": "table",
          "displayName": "table",
          "primaryKey": [],
          "columns": [
            "body"
          ]
        }
      ]
    }
  ],
  "sandboxes": [
    {
      "name": "foo",
      "type": "snowflake"
    }
  ],
  "schedules": [
    {
      "name": "schedule1"
    }
  ]
}

```

## `interaction.txt`

- If the file IS NOT present, the command is executed in non-interactive mode.
- If the file is present, interactive input is simulated based on the contents of the file.

**Format:**
- Empty lines are used for higher clarity/readability and are ignored.
- Line starting with `# ` is comment and is ignored.
- Line starting with `< ` is expected command output with default timeout `10s` (see `terminal.expectTimeout`).
- Line starting with `< [...]` is expected command output with specified timeout.
    - For example definition `< [60s] Expected output.` will wait `60 seconds` for the command output.
- Line starting with `> ` is command input.
- ENV placeholders, for example `%%TEST_STORAGE_API_HOST%%`, can be used in both: expected outputs and inputs.

**Special inputs:**
- `<up arrow>`
- `<down arrow>`
- `<left arrow>`
- `<right arrow>`
- `<space>`
- `<enter>`

**Example:**
```
< Please enter Keboola Storage API host, eg. "connection.keboola.com".
> %%TEST_KBC_STORAGE_API_HOST%%

< Please enter Keboola Storage API token. The value will be hidden.
> %%TEST_KBC_STORAGE_API_TOKEN%%

< Please select project's branches you want to use with this CLI.
# Select all branches
> <down arrow>
> <enter>
```

## `request.json`

Format:

```json
{
  "path": "/v1/receivers",
  "method": "POST",
  "headers": {
    "Content-Type": "application/json",
    "X-StorageApi-Token": "%%TEST_KBC_STORAGE_API_TOKEN%%"
  },
  "body": {
    "id": "receiver-1"
  }
}
```

## Polling for tasks

**Supported only in Stream API tests.**

`Path` supports reference to previous request's response. The reference is in the format `<<001-create:response.url>>` where `001-create` is the name of the request and `url` is the path to the value in the `response`. The referenced URL will be stripped of the hostname so that it will be relative to the API server.

`Repeat.until` is an expression evaluated by [expr](https://github.com/expr-lang/expr) library against the response. 
Until the expression is met the request is repeated with the specified `timeout`. 

Timeout is in seconds and default value is `60`. Wait is a sleep between requests in seconds and default value is `3`.

Format:

```json
{
  "path": "<<001-create:response.url>>",
  "method": "GET",
  "headers": {
    "X-StorageApi-Token": "%%TEST_KBC_STORAGE_API_TOKEN%%"
  },
  "repeat": {
    "until": "isFinished == true",
    "timeout": 60,
    "wait": 3
  }
}
```

## `initial-etcd-kvs.txt` and `expected-etcd-kvs.txt`

**Supported only in Stream API tests.**

Format:

```
<<<<<
/config/export/%%TEST_KBC_PROJECT_ID%%/receiver-1/export-1
-----
{
  "projectId": %%TEST_KBC_PROJECT_ID%%,
  "receiverId": "receiver-1",
  "exportId": "export-1",
  "name": "Export 1",
  "importConditions": {
    "count": 1000,
    "size": "1MB",
    "time": 300000000000
  }
}
>>>>>

<<<<<
/secret/export/token/%%TEST_KBC_PROJECT_ID%%/receiver-1/export-1
-----
%A
>>>>>
```

## Wildcards

Wildcards can be used in `/expected-stdout`, `/expected-stderr` and `/out/*.*` for comparing dynamic values:
- `%e`: Represents a directory separator, for example `/` on Linux.
- `%s`: One or more of anything (character or white space) except the end of line character.
- `%S`: Zero or more of anything (character or white space) except the end of line character.
- `%a`: One or more of anything (character or white space) including the end of line character.
- `%A`: Zero or more of anything (character or white space) including the end of line character.
- `%w`: Zero or more white space characters.
- `%i`: A signed integer value, for example +3142, -3142.
- `%d`: An unsigned integer value, for example 123456.
- `%x`: One or more hexadecimal character. That is, characters in the range 0-9, a-f, A-F.
- `%f`: A floating point number, for example: 3.142, -3.142, 3.142E-10, 3.142e+10.
- `%c`: A single character of any sort.
- `%%`: A literal percent character: %.

Inspired by [PhpUnit](https://docs.phpunit.de/en/11.4/assertions.html#assertstringmatchesformat).

## Environment Placeholders

Environment placeholders can be used in `/expected-stdout`, `/expected-stderr`, `/in/*.*` and `/out/*.*`.

E.g. `%%TEST_STORAGE_API_HOST%%` will be replaced with a value of the ENV variable `TEST_STORAGE_API_HOST`.

## Project locking

Currently there is implemented project locking using `flock`.

When environment variables are set, it can be change into locking mechanism using `redis`.

```
TEST_KBC_PROJECTS_LOCK_HOST=redis://redis:6379
TEST_KBC_PROJECTS_LOCK_PASSWORD=password
```

This is sample example how to setup environment variables to turn on `redis` locking within the project. Current configuration is without TLS, but it is expected to use TLS in production.

To enable tls use the `TEST_KBC_PROJECTS_LOCK_HOST` with `+tls`.
```
TEST_KBC_PROJECTS_LOCK_HOST=redis+tls://redis:6380
```

## Generate new unique ID

If a ENV placeholder in the form `^TEST_NEW_TICKET_\d+$` is found, it is replaced with new ID/ticket [generated by API](https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id).
- E.g. `%%TEST_NEW_TICKET_1%%`
- The value is generated when the first occurrence is found.
- All occurrences are replaced with the same value.
- Works in `/in/*.*` and `/out/*.*` files.
