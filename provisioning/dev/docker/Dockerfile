FROM golang:1.22.3

ENV HOME=/my-home
ENV GOMEMLIMIT=512MiB
ENV GOCACHE=/tmp/cache/go
ENV GOMODCACHE=/tmp/cache/go-mod
ENV GOBIN=$GOPATH/bin
ENV GOFLAGS="-mod=mod"
ENV GOLANGCI_LINT_CACHE=/tmp/cache/golangci-lint
ENV PATH="$PATH:$GOBIN"

# Install editor
RUN apt-get update && apt-get install -y nano
ENV EDITOR=nano

# Install tools
RUN mkdir -p /tmp/build
COPY Makefile /tmp/build/Makefile
COPY scripts  /tmp/build/scripts
RUN cd /tmp/build && make tools && rm -rf /tmp/build
RUN apt update && apt install -y graphviz

# Install envsubstr and helm
RUN curl -L https://github.com/a8m/envsubst/releases/download/v1.2.0/envsubst-$(uname -s)-$(uname -m) -o /usr/local/bin/envsubst && \
    chmod +x /usr/local/bin/envsubst && \
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Set prompt
RUN mkdir -p ~ && \
    echo 'PS1="\w > "' > ~/.bashrc

# Fix permissions
RUN mkdir -p $GOPATH && chmod -R 1777 $GOPATH && \
    mkdir -p $GOCACHE && chmod -R 1777 $GOCACHE && \
    mkdir -p $GOMODCACHE && chmod -R 1777 $GOMODCACHE && \
    mkdir -p $GOLANGCI_LINT_CACHE && chmod -R 1777 $GOLANGCI_LINT_CACHE && \
    mkdir -p $HOME && chmod -R 1777 $HOME

WORKDIR /code/
CMD ["/bin/bash"]
