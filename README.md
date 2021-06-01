# Keboola as Code

- Keboola as Code is **pull/push client** using [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
- It syncs all [component configurations](https://help.keboola.com/components/) from a [KBC](https://www.keboola.com/) project to a directory, and vice versa.
- The directory can be versioned in [Git](https://git-scm.com/) or a similar tool.

## Error Reporting

If an unexpected error occurs, the user can submit a generated log file to support email.

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

Run the test suite and download the dependencies using this command:

```
docker-compose run --rm dev ./test.sh
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

In IntelliJ IDEA is needed to set project GOPATH to `/go` directory, for code autocompletion to work.
