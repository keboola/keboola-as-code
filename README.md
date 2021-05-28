# Keboola as Code

- Keboola as Code is **pull/push client** using [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
- It syncs all [component configurations](https://help.keboola.com/components/) from a [KBC](https://www.keboola.com/) project to a directory, and vice versa.
- The directory can be versioned in [Git](https://git-scm.com/) or a similar tool.

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

In IntelliJ IDEA is needed to set project GOPATH to `/go` directory, for code autocompletion to work.
