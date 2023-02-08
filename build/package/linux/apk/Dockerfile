FROM alpine:3.17

ENV HOME=/opt
RUN echo 'PS1="\w > "' > ~/.bashrc && chmod 0777 -R ~

RUN apk add --no-cache bash abuild && \
    chmod 777 /etc/apk/keys

COPY index.sh /usr/local/bin/index.sh
CMD /usr/local/bin/index.sh
