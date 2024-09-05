FROM grafana/xk6:latest AS build

RUN xk6 build --with github.com/LeonAdato/xk6-output-statsd

FROM scratch

COPY --from=build /xk6/k6 /usr/bin/k6

COPY scripts /scripts

ENTRYPOINT ["k6"]
