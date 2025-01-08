# Apps proxy Architecture Overview

- Serves for data apps authentication and authorization.
- Typicall usage is to perform OIDC login through some OIDC provider (e.g Microsoft login, Google login etc.)
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
In project directory uncommect in [docker-compose.yml](../../docker-compose.yml) the `command` section and fill it with `apps-proxy` service. It should look like this
```
command: >
    sh -c "git config --global --add safe.directory /code
           make run-apps-proxy"
```

Then launch the dev container
```
docker compose up -d dev
```

There is a sandboxes service mock in place which returns configuration of data app. Simply adjust the [provisioning/apps-proxy/dev/sandboxesMock.json](../../provisioning/apps-proxy/dev/sandboxesMock.json) if you want to change received configuration by local testing.

Next clone this repository: GitHub - [fsouza/docker-ssl-proxy](https://github.com/fsouza/docker-ssl-proxy)

In its directory run this:

```
docker build -t https-proxy .
```

And then go back to the root repository and launch the https-proxy:

```
docker compose up https-proxy 
```

Now the proxy should be available under https://test.hub.keboola.local/.


