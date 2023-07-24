FROM netdata/netdata:v1.41.0

ENV DISABLE_TELEMETRY=1

COPY provisioning/benchmark-dashboard/docker/netdata.conf /etc/netdata/netdata.conf

COPY provisioning/benchmark-dashboard/docker/netdata-cloud.conf /var/lib/netdata/cloud.d/cloud.conf
RUN chmod 0770            /var/lib/netdata/cloud.d/cloud.conf
RUN chown netdata:netdata /var/lib/netdata/cloud.d/cloud.conf