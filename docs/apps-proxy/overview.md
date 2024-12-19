# Apps proxy Architecture Overview

- Serves for data apps authentication and authorization.
- Typicall usage is to perform OIDC login through some OIDC provider (e.g Microsoft login, google login etc.)
- Has possibility to add basic authorization which consists of password prompt on a web page.


## Entrypoint

[cmd/apps-proxy/main.go](../../cmd/apps-proxy/main.go)

## Apps Proxy Options

## Operations

In `/etc/hosts` add this:

```
127.0.0.1 test.hub.keboola.local
127.0.0.1 hub.keboola.local
```
In project directory run:

```
docker compose run --rm --service-ports dev --net=my-test bash
```

Inside this bash run:

```
make run-app-proxy
```

There is a sandboxes service mock in place which returns configuration of data app. Simply adjust the [provisioning/apps-proxy/dev/sandboxesMock.json](../../provisioning/apps-proxy/dev/sandboxesMock.json) if you want to change received configuration by local testing.

Out of the container check for <containerid> using

```
docker ps -a
```

Find ID of the container running the `dev` environment.

Next clone this repository: GitHub - [fsouza/docker-ssl-proxy](https://github.com/fsouza/docker-ssl-proxy)

In its directory run this:

```
docker build -t https-proxy .
```

And then in this command replace <containerid> with the id from earlier:

```
docker run --net=cli_default --rm \
  --env DOMAIN=test.hub.keboola.local \
  --env TARGET_HOST=<containerid> \
  --env TARGET_HOST_HEADER=test.hub.keboola.local \
  --env TARGET_PORT=8002 \
  -p 443:443 \
  --volume=./ca:/etc/nginx/ca \
  --volume=./certs:/etc/nginx/certs \
  https-proxy
```

Now the proxy should be available under https://test.hub.keboola.local/.


