# Keboola as Code

- Keboola as Code is **pull/push client** using [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
- It syncs all [component configurations](https://help.keboola.com/components/) from a [KBC](https://www.keboola.com/) project to a directory, and vice versa.
- The directory can be versioned in [Git](https://git-scm.com/) or a similar tool.

## Directory structure

```
Root project folder
├─ .gitignore
├─ .env                                         - env variables, it mustn't be versioned/published
├─ .env.dist                                    - template of the .env file
├─ .keboola                                     - metadata
|  └─ manifest.json
└─ branches
   └─ [branch-id]-[branch-name]                 - eg. 10715-test
      ├─ config.json                            - contains branch name, description
      └─ [component-type]                       - eg. extractor
         └─ [component-id]                      - eg. keboola.ex-db-oracle
            └─ [config-id]-[config-name]        - eg. 641226048-oauth-test
                ├─ config.json                  - contains all config's editable properties
                └─ rows
                   └─ [row-id]-[row-name].json  - eg. 28790-prod-fact-table
```

## Environment variables

Priority of values:
1. Command line flags
2. Environment variables set in OS
3. Environment variables in `.env` file

List of supported variables:
```
KBC_STORAGE_API_URL=
KBC_STORAGE_API_TOKEN=
```

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

Run the test suite and download the dependencies using:

```
docker-compose run --rm dev make tests
```

To launch the interactive console in container, you can use:
```
docker-compose run --rm dev bash
```

In the container, you can run CLI from source code using:
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

### Functional tests

Each directory in the `src/tests/*` is one functional test.

#### Directory structure

```
/src/tests
└─test-name
  ├─args              - file with command line arguments, used when running the program
  ├─expected-code     - file with expected exit code, eg. 0 for success
  ├─expected-stdout   - file with expected stdout
  ├─expected-stderr   - file with expected stderr
  ├─in                - dir, initial state of the working directory before program execution
  └─out               - dir, expected state of the working directory after program execution
```

#### Wildcards

In files `/expected-stdout`, `/expected-stderr` and `/out/*.*` can be used placeholders for comparing dynamic values:
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

### IDE setup

In IntelliJ IDEA is needed to set project GOPATH to `/go` directory, for code autocompletion to work.
