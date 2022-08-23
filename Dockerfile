FROM golang:1.18

ENV HOME=/my-home
ENV GOCACHE=/tmp/cache/go
ENV GOMODCACHE=/tmp/cache/go-mod
ENV GOFLAGS="-mod=mod"
ENV GOLANGCI_LINT_CACHE=/tmp/cache/golangci-lint
ENV PATH="$PATH:$GOPATH/bin"

# Install editor
RUN apt-get update && apt-get install -y nano
ENV EDITOR=nano

# Install tools
RUN mkdir -p /tmp/build
COPY Makefile /tmp/build/Makefile
COPY scripts  /tmp/build/scripts
RUN cd /tmp/build && make tools && rm -rf /tmp/build

# Install Helm
RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Set prompt
RUN mkdir -p ~ && \
    echo 'PS1="\w > "' > ~/.bashrc

# Fix permissions
RUN mkdir -p $GOPATH && chmod -R 777 $GOPATH && \
    mkdir -p $GOCACHE && chmod -R 777 $GOCACHE && \
    mkdir -p $GOMODCACHE && chmod -R 777 $GOMODCACHE && \
    mkdir -p $GOLANGCI_LINT_CACHE && chmod -R 777 $GOLANGCI_LINT_CACHE && \
    mkdir -p $HOME && chmod -R 777 $HOME

WORKDIR /code/
CMD ["/bin/bash"]
