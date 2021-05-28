FROM golang:1.16-buster

RUN apt-get update -y && \
    apt-get install -y jq time zip binutils-common

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


ENV GOBIN=/code/build

WORKDIR /code/
COPY . /code/
RUN mkdir /code/target && mkdir /code/go

CMD ["/bin/bash"]
