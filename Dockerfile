FROM golang:1.17-buster

RUN apt-get update -y && \
    apt-get install -y jq time zip git binutils-common

ENV HOME=/my-home
ENV GOPATH=/code/go
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

# Install goreleaser
RUN curl --silent "https://api.github.com/repos/goreleaser/goreleaser/releases/latest" | \
    jq -r '.assets[] | select(.name | endswith("Linux_x86_64.tar.gz")).browser_download_url' | \
    wget -i - -O - | \
    tar -xz -C /usr/local/bin goreleaser && \
    chmod +x /usr/local/bin/goreleaser

RUN mkdir -p ~ && \
    echo 'PS1="\w > "' > ~/.bashrc

# Setup GPG
RUN mkdir ~/.gnupg && \
    echo "cert-digest-algo SHA256" >> ~/.gnupg/gpg.conf && \
    echo "digest-algo SHA256" >> ~/.gnupg/gpg.conf && \
    chmod -R 777 ~

WORKDIR /code/
CMD ["/bin/bash"]
