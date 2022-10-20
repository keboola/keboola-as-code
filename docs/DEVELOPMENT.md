# Keboola as Code - Development

## Setup

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code
docker-compose build
```

Create `.env` file with test project definition:
```dotenv
TEST_KBC_PROJECT_ID=
TEST_KBC_STORAGE_API_HOST=connection.keboola.com
TEST_KBC_STORAGE_API_TOKEN=
```

Or you can define more test projects this way (tests pass faster):
```dotenv
TEST_KBC_PROJECTS="connection.keboola.com|1234|project-1234-token;host2|id2|token2;...."
```

Run the test suite and download the dependencies using:

```
docker-compose run --rm -u "$UID:$GID" dev make ci
```

To start the interactive console in the container, you can use:
```
docker-compose run --rm -u "$UID:$GID" --service-ports dev bash
```

To run the one or more tests via wildcard:
```
./scripts/tests.sh -run "CliE2E/pull/.*"
```

To run tests in one specific package:
```
go test ./internal/pkg/PACKAGE/...
```

To run tests with verbose output to see HTTP requests, ENVs, etc.:
```
docker-compose run --rm dev make tests-verbose
```

In the container, you can run CLI from the source code using:
```
go run ./src/main.go help init
```

To compile a local binary to `./target`, run in the container:
```
make build-local
```

To compile a binary for all architectures to `./target`, run in the container:
```
make build
```

### API Development

API development uses [Goa code generator](https://goa.design/).

To run the API locally:
1. Start `bash` in the dev container, run `docker-compose run --rm -u "$UID:$GID" --service-ports dev bash`.
2. In the container run `make run-templates-api`
 - The API is exposed to `http://localhost:8000/`.
 - When the code changes, the API recompiles and restarts.

### Functional Tests

Each directory in the `src/tests/*` is one functional test.

#### Directory Structure

```
/src/tests
└─test-name
  ├─args                - file with command line arguments, used when running the program
  ├─expected-code       - file with expected exit code, eg. 0 for success
  ├─expected-stdout     - file with expected stdout
  ├─expected-stderr     - file with expected stderr
  ├─expected-state.json - optional, if present, the final project's state is compared with the state defined in the file
  ├─initial-state.json  - optional, if present, TEST_PROJECT will be set to defined state befor test
  ├─interaction.txt     - optional, if present, interactive input/output is executed according to the script
  ├─in                  - dir, initial state of the working directory before program execution
  └─out                 - dir, expected state of the working directory after program execution
```

##### `interaction.txt`

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
- `<enter>`

**Example:**
```
< Please enter Keboola Storage API host, eg. "connection.keboola.com".
> %%TEST_KBC_STORAGE_API_HOST%%

< Please enter Keboola Storage API token. The value will be hidden.
> %%TEST_KBC_STORAGE_API_TOKEN%%

< Please select which project's branches you want to use with this CLI.
# Select all branches
> <down arrow>
> <enter>
```


#### Wildcards

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

Inspired by [PhpUnit](https://phpunit.readthedocs.io/en/9.5/assertions.html#assertstringmatchesformat).

#### Environment Placeholders
Environment placeholders can be used in `/expected-stdout`, `/expected-stderr`, `/in/*.*` and `/out/*.*`.

Eg. `%%TEST_STORAGE_API_HOST%%` will be replaced with a value of the ENV variable `TEST_STORAGE_API_HOST`.

##### Generate new unique ID

If a ENV placeholder in the form `^TEST_NEW_TICKET_\d+$` is found, it is replaced with new ID/ticket [generated by API](https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id).
- Eg. `%%TEST_NEW_TICKET_1%%`
- The value is generated when the first occurrence is found.
- All occurrences are replaced with the same value.
- Works in `/in/*.*` and `/out/*.*` files.

### IDE setup

The scripts `make mod`, `make fix`, `make ci` calls `go mod vendor`.
It syncs all dependencies (Go modules) to the `vendor` directory.
So integration with the IDE should work automatically.
