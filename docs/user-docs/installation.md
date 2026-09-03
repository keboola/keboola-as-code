# Installation

The recommended way to install Keboola CLI is with one of the package managers listed below.
This allows you to easily upgrade to a new version.

Alternatively, you can:
- Download precompiled binaries from [cli-dist.keboola.com](https://cli-dist.keboola.com/?prefix=zip/). 
- Or build binary from [source code](#build-from-source).

Changelog can be found at [github.com/keboola/keboola-as-code/releases](https://github.com/keboola/keboola-as-code/releases).

## macOS

Installation on macOS is managed by Homebrew. If you don't have Homebrew available on your system,
[install it](https://docs.brew.sh/Installation.html) before continuing.

Install:

```bash
brew tap keboola/keboola-cli
brew install keboola-cli
kbc --version
```

Upgrade:

```bash
brew upgrade keboola-cli
```

## Debian / Ubuntu

Install:

```bash
sudo wget -P /etc/apt/trusted.gpg.d https://cli-dist.keboola.com/deb/keboola.gpg
echo "deb https://cli-dist.keboola.com/deb /" | sudo tee /etc/apt/sources.list.d/keboola.list
sudo apt-get update
sudo apt-get install keboola-cli
kbc --version
```

Upgrade:

```bash
sudo apt-get update
sudo apt-get install keboola-cli
```

## Fedora

Install:

```bash
sudo rpm --import https://cli-dist.keboola.com/rpm/keboola.gpg
echo "[keboola]
name=keboola
baseurl=https://cli-dist.keboola.com/rpm
enabled=1
gpgcheck=1
gpgkey=https://cli-dist.keboola.com/rpm/keboola.gpg
" | sudo tee /etc/yum.repos.d/keboola.repo
sudo dnf install keboola-cli
kbc --version
```

Upgrade:

```bash
sudo dnf update keboola-cli
```

## Alpine

Install:

```bash
echo "https://cli-dist.keboola.com/apk" | sudo tee -a /etc/apk/repositories
sudo wget -P /etc/apk/keys/ https://cli-dist.keboola.com/apk/keboola.rsa.pub
sudo apk update
sudo apk add keboola-cli
kbc --version
```

Upgrade:

```bash
apk update
apk add --upgrade keboola-cli
```

## Windows

### WinGet

If you don't have App Installer available on your system, either install it [from the Microsoft Store](https://apps.microsoft.com/detail/9NBLGGH4NNS1) or [download directly](https://winget.run/) before continuing.

Install:

```shell
winget install Keboola.KeboolaCLI
kbc --version
```

Upgrade:

```shell
winget upgrade Keboola.KeboolaCLI
```


### Chocolatey

If you don't have Chocolatey available on your system, [install it](https://chocolatey.org/install) before continuing.

Install:

```shell
choco install keboola-cli
kbc --version
```

Upgrade:

```shell
choco upgrade keboola-cli
```

### Scoop

If you don't have Scoop available on your system, [install it](https://scoop.sh/) before continuing.

Install:

```shell
scoop bucket add keboola https://github.com/keboola/scoop-keboola-cli.git
scoop install keboola/keboola-cli
kbc --version
```

Upgrade:

```shell
scoop update keboola/keboola-cli
```

### Download

Use a [msi installer](https://cli-dist.keboola.com/?prefix=msi/) or a [precompiled binary](https://cli-dist.keboola.com/?prefix=zip/). 

## Build From Source

1. Install the [Go environment](https://golang.org/doc/install) (if you haven't done so already).
2. Clone the source from GitHub:
```
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code
```

3. Run the build:  
On Linux or macOS:
```
go build -o target/kbc ./cmd/kbc/main.go
```
On Windows:
```
go build -o target/kbc.exe ./cmd/kbc/main.go
```

4. Binary is located in `target/kbc` or `target/kbc.exe`.

## Next Steps

- [Getting Started](getting-started.md)
- [Commands](commands.md)
