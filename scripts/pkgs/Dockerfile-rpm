FROM fedora

ENV HOME=/opt
RUN echo 'PS1="\w > "' > ~/.bashrc && chmod 0777 -R ~

RUN yum update -y && yum install -y createrepo

CMD /scripts/index_rpm.sh
