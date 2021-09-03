FROM golang:1.17-buster

RUN echo 'deb [trusted=yes] https://repo.goreleaser.com/apt/ /' | tee /etc/apt/sources.list.d/goreleaser.list
RUN apt-get update -y && \
    apt-get install -y jq time zip git binutils-common goreleaser

ENV HOME=/tmp/home
ENV GOPATH=/tmp/go
ENV GOCACHE=/tmp/go-cache
ENV GOBIN=/usr/local/bin

# Install staticcheck
RUN curl --silent "https://api.github.com/repos/dominikh/go-tools/releases/latest" | \
    jq -r '.assets[] | select(.name == "staticcheck_linux_amd64.tar.gz").browser_download_url' | \
    wget -i - -O - | \
    tar -xz --strip-components=1 -C /usr/local/bin staticcheck/staticcheck && \
    chmod +x /usr/local/bin/staticcheck

# Install richgo
RUN curl --silent "https://api.github.com/repos/kyoh86/richgo/releases/latest" | \
    jq -r '.assets[] | select(.name | endswith("_linux_amd64.tar.gz")).browser_download_url' | \
    wget -i - -O - | \
    tar -xz -C /usr/local/bin richgo && \
    chmod +x /usr/local/bin/richgo

# Install goimports
RUN go get golang.org/x/tools/cmd/goimports

RUN rm -rf /tmp/* && \
    mkdir -p /tmp/home && \
    chmod -R 777 /tmp/home && \
    echo 'PS1="\w > "' > /tmp/home/.bashrc

WORKDIR /code/
CMD ["/bin/bash"]
