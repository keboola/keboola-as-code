FROM golang:1.17-buster

RUN echo 'deb [trusted=yes] https://repo.goreleaser.com/apt/ /' | tee /etc/apt/sources.list.d/goreleaser.list
RUN apt-get update -y && \
    apt-get install -y jq time zip git binutils-common goreleaser

ENV HOME=/tmp/home
ENV GOPATH=/tmp/go
ENV GOCACHE=/tmp/go-cache
ENV GOBIN=/usr/local/bin

# Install richgo
RUN curl --silent "https://api.github.com/repos/kyoh86/richgo/releases/latest" | \
    jq -r '.assets[] | select(.name | endswith("_linux_amd64.tar.gz")).browser_download_url' | \
    wget -i - -O - | \
    tar -xz -C /usr/local/bin richgo && \
    chmod +x /usr/local/bin/richgo

# Install linter
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin v1.42.0

RUN rm -rf /tmp/* && \
    mkdir -p /tmp/home && \
    chmod -R 777 /tmp/home && \
    echo 'PS1="\w > "' > /tmp/home/.bashrc

WORKDIR /code/
CMD ["/bin/bash"]
