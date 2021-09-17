# Keboola as Code

- Keboola as Code is **pull/push client** using [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
- It syncs all [component configurations](https://help.keboola.com/components/) from a [KBC](https://www.keboola.com/) project to a directory, and vice versa.
- The directory can be versioned in [Git](https://git-scm.com/) or a similar tool.

## Installation

This console tool is distributed in the following ways.

### MacOS

#### [Homebrew](https://brew.sh/)

```sh
brew tap keboola/homebrew-tap
brew install keboola-as-code
kbc --version
```

### Linux

#### Debian / Ubuntu
```sh
sudo wget -P /etc/apt/trusted.gpg.d https://cli-dist.keboola.com/deb/keboola.gpg
echo "deb https://cli-dist.keboola.com/deb /" | sudo tee /etc/apt/sources.list.d/keboola.list
sudo apt-get update
sudo apt-get install kbc
kbc --version
```

#### Fedora
```sh
sudo rpm --import https://cli-dist.keboola.com/rpm/keboola.gpg
echo "[keboola]
name=keboola
baseurl=https://cli-dist.keboola.com/rpm
enabled=1
gpgcheck=1
gpgkey=https://cli-dist.keboola.com/rpm/keboola.gpg
" | sudo tee /etc/yum.repos.d/keboola.repo
sudo dnf install kbc
kbc --version
```

#### Alpine
```sh
echo "https://cli-dist.keboola.com/apk" | sudo tee -a /etc/apk/repositories
sudo wget -P /etc/apk/keys/ https://cli-dist.keboola.com/apk/keboola.rsa.pub
sudo apk update
sudo apk add kbc
kbc --version
```

#### [Homebrew](https://brew.sh/)

```
brew tap keboola/homebrew-tap
brew install keboola-as-code
kbc --version
```

### Windows

```
todo
```

### Manual Installation



## Directory Structure

```
Root project folder
├─ .gitignore
├─ .env.local                                   - contains API token, keep it local and secret
├─ .env.dist                                    - an ".env.local" template
├─ .keboola                                     - metadata
|  └─ manifest.json
└─ branches
   └─ [branch-id]-[branch-name]                 - eg. 10715-test
      ├─ meta.json                              - contains name, ...
      ├─ description.md                         - contains description
      └─ [component-type]                       - eg. extractor
         └─ [component-id]                      - eg. keboola.ex-db-oracle
            └─ [config-id]-[config-name]        - eg. 641226048-oauth-test
                ├─ meta.json                    - contains name, ...
                ├─ config.json                  - contains configuration
                ├─ description.md               - contains description
                └─ rows
                   └─ [row-id]-[row-name]       - eg. 28790-prod-fact-table
                       ├─ meta.json             - contains name, isDisabled ...
                       ├─ config.json           - contains configuration
                       └─ description.md         - contains description
```


### Transformations

Transformations are defined by native files, eg `.sql` or `.py`.

Directory structure:
```
[config-id]-[config-name]               - eg. 641226048-python-transformation
    ├─ meta.json                        - contains name, description, ...
    ├─ config.json                      - contains configuration
    └─ blocks
       └─ [block-order]-[block-name]    - eg. "001-my-block"
          ├─ meta.json                  - contains block name
          └─ [code-order]-[code-name]   - eg. "001-my-code"
             ├─ meta.json               - contains code name
             └─ code.[ext]              - contains content in the native language, eg. "code.sql", `code.py`, ...
```

## Environment Variables

### Priority Of Values
1. Command line flags.
2. Environment variables set in OS.
3. Environment variables from [.env files](https://github.com/bkeepers/dotenv#what-other-env-files-can-i-use) in the working directory.
4. Environment variables from [.env files](https://github.com/bkeepers/dotenv#what-other-env-files-can-i-use) in the project directory.

### Naming

- Each environment variable starts with `KBC_` prefix.
- Each flag (see `help`) can be defined by an environment variable. 
- Examples:
  - `--storage-api-token` as `KBC_STORAGE_API_TOKEN`
  - `--verbose` as `KBC_VERBOSE`

## Error Reporting

If an unexpected error occurs, the user can submit a generated log file to out support email.

Example error message:
```
Keboola Connection client had a problem and crashed.

To help us diagnose the problem you can send us a crash report.

We have generated a log file at "/tmp/keboola-as-code-1622621664.txt".

Please submit email to "support@keboola.com" and include the log file as an attachment.

We take privacy seriously, and do not perform any automated error collection.

Thank you kindly!
```

Example log file:
```
DEBUG   Version:    dev
DEBUG   Git commit: 704961bb88ec1138f9d91c0721663ea229a71d9a
DEBUG   Build date: 2021-06-02T08:14:23+0000
DEBUG   Go version: go1.16.4
DEBUG   Os/Arch:    linux/amd64
DEBUG   Running command [...]
DEBUG   Unexpected panic: error
DEBUG   Trace:
...
```

## Quickstart

For the basic scenarios on how to use the CLI read the [QUICKSTART.md](./docs/QUICKSTART.md) document.

## Development

- This tool is primarily developed by [Keboola](https://www.keboola.com/).
- Suggestions for improvements and new features can be submitted at https://ideas.keboola.com/.
- You can also send PR directly, but we do not guarantee that it will be accepted.
- See the [developer's guide](./docs/DEVELOPMENT.md).
