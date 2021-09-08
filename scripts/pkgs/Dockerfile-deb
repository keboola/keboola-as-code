FROM debian

ENV HOME=/opt
RUN echo 'PS1="\w > "' > ~/.bashrc && chmod 0777 -R ~

RUN apt-get update && apt-get install --no-install-recommends --yes dpkg-dev dpkg-sig apt-utils

CMD /scripts/index_deb.sh
