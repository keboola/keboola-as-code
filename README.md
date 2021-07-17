# Keboola as Code

- Keboola as Code is **pull/push client** using [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
- It syncs all [component configurations](https://help.keboola.com/components/) from a [KBC](https://www.keboola.com/) project to a directory, and vice versa.
- The directory can be versioned in [Git](https://git-scm.com/) or a similar tool.

## Installation

### Manual Installation
- Download the latest release for your architecture from the [Github Releases](https://github.com/keboola/keboola-as-code/releases).
- Extract archive, eg. `unzip kbc-vx.x.x-linux_arm64.zip`.
- Make binary executable, eg. `chmod +x ./kbc`.
- Optionally move the binary to your [PATH](https://en.wikipedia.org/wiki/PATH_(variable)).
- Run.

## Directory Structure

```
Root project folder
├─ .gitignore
├─ .env.local                                   - contains API token, keep it local and secret
├─ .env.dist                                    - an ".env.local" template
├─ .keboola                                     - metadata
|  └─ manifest.json
└─ branches
   └─ [branch-id]-[branch-name]                 - eg. 10715-test
      ├─ meta.json                              - contains branch name, description
      └─ [component-type]                       - eg. extractor
         └─ [component-id]                      - eg. keboola.ex-db-oracle
            └─ [config-id]-[config-name]        - eg. 641226048-oauth-test
                ├─ meta.json                    - contains name, description, ...
                ├─ config.json                  - contains configuration
                └─ rows
                   └─ [row-id]-[row-name]       - eg. 28790-prod-fact-table
                       ├─ meta.json             - contains name, description, ...
                       └─ config.json           - contains configuration
```


### Transformations

Transformations are defined by native files, eg `.sql` or `.py`.

Directory structure:
```
[config-id]-[config-name]               - eg. 641226048-python-transformation
    ├─ meta.json                        - contains name, description, ...
    ├─ config.json                      - contains configuration
    └─ blocks
       └─ [block-order]-[block-name]    - eg. "001-my-block"
          ├─ meta.json                  - contains block name
          └─ [code-order]-[code-name]   - eg. "001-my-code"
             ├─ meta.json               - contains code name
             └─ code.[ext]              - contains content in the native language, eg. "code.sql", `code.py`, ...
```

## Environment Variables

### Priority Of Values
1. Command line flags.
2. Environment variables set in OS.
3. Environment variables from [.env files](https://github.com/bkeepers/dotenv#what-other-env-files-can-i-use) in the working directory.
4. Environment variables from [.env files](https://github.com/bkeepers/dotenv#what-other-env-files-can-i-use) in the project directory.

### Naming

- Each environment variable starts with `KBC_` prefix.
- Each flag (see `help`) can be defined by an environment variable. 
- Examples:
  - `--storage-api-token` as `KBC_STORAGE_API_TOKEN`
  - `--verbose` as `KBC_VERBOSE`

## Error Reporting

If an unexpected error occurs, the user can submit a generated log file to out support email.

Example error message:
```
Keboola Connection client had a problem and crashed.

To help us diagnose the problem you can send us a crash report.

We have generated a log file at "/tmp/keboola-as-code-1622621664.txt".

Please submit email to "support@keboola.com" and include the log file as an attachment.

We take privacy seriously, and do not perform any automated error collection.

Thank you kindly!
```

Example log file:
```
DEBUG   Version:    dev
DEBUG   Git commit: 704961bb88ec1138f9d91c0721663ea229a71d9a
DEBUG   Build date: 2021-06-02T08:14:23+0000
DEBUG   Go version: go1.16.4
DEBUG   Os/Arch:    linux/amd64
DEBUG   Running command [...]
DEBUG   Unexpected panic: error
DEBUG   Trace:
...
```

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code
docker-compose build
```

Create `.env` file:
```dotenv
TEST_PROJECT_ID=
TEST_PROJECT_NAME=
TEST_KBC_STORAGE_API_HOST=connection.keboola.com
TEST_KBC_STORAGE_API_TOKEN=
TEST_KBC_STORAGE_API_TOKEN_MASTER=
TEST_KBC_STORAGE_API_TOKEN_EXPIRED=
```

Run the test suite and download the dependencies using:

```
docker-compose run --rm dev make tests
```

To start the interactive console in the container, you can use:
```
docker-compose run --rm dev bash
```

To run the one or more tests via wildcard:
```
./scripts/tests.sh -run "Functional/pull*"
```

To run tests with verbose output to see HTTP requests, ENVs, etc.:
```
docker-compose run --rm dev make tests-verbose
```

In the container, you can run CLI from the source code using:
```
go run ./src/main.go help init
```

To compile a local binary to `./target/bin/local`, run in the container:
```
make build
```

To compile a binary for all architectures to `./target/bin`, run in the container:
```
make build-cross
```

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
  ├─in                  - dir, initial state of the working directory before program execution
  └─out                 - dir, expected state of the working directory after program execution
```

#### Wildcards

Wildcards can be used in `/expected-stdout`, `/expected-stderr` and `/out/*.*` for comparing dynamic values:
- `%e`: Represents a directory separator, for example / on Linux.
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

In IntelliJ IDEA is needed to set project GOPATH to `/go` directory, for code autocompletion to work.
