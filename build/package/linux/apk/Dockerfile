FROM alpine

ENV HOME=/opt
RUN echo 'PS1="\w > "' > ~/.bashrc && chmod 0777 -R ~

RUN apk add --no-cache bash abuild && \
    chmod 777 /etc/apk/keys

CMD /scripts/index_apk.sh
