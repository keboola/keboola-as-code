FROM debian:11

ENV HOME=/opt
RUN echo 'PS1="\w > "' > ~/.bashrc && chmod 0777 -R ~

RUN apt-get update && apt-get install --no-install-recommends --yes dpkg-dev dpkg-sig apt-utils

COPY index.sh /usr/local/bin/index.sh
CMD /usr/local/bin/index.sh
