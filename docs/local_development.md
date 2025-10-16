# Local Development Guide

## Prerequisites

- Go 1.25+
- ETCD v3.6
- Git
- Task

## Installation Steps

### 1. Install Go
```bash
# For Ubuntu/Debian
sudo apt-get update
sudo apt-get install golang-1.25

# For Arch Linux
sudo pacman -S go

# Verify installation
go version
```

### 2. Install Task
```bash
# Using the official installation script
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d

# Verify installation
task --version
```

### 3. Install ETCD
```bash
# For Ubuntu/Debian
sudo apt-get install etcd

# For Arch Linux
sudo pacman -S etcd

# Start ETCD service
sudo systemctl start etcd
sudo systemctl enable etcd

# Verify ETCD is running
etcdctl endpoint health
```

### 4. Clone and Setup Project
```bash
# Clone repository
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code

# Install dependencies
go mod download
go mod vendor

# Create environment file
cp .env.dist .env
```

### 4. Configure Environment

Edit `.env` file with your settings:
```bash
TEST_KBC_TMP_DIR=/tmp
TEST_KBC_PROJECTS_FILE=~/keboola-as-code/projects.json
```

Create `projects.json` with your project configuration:
```json
[
  {
    "host": "connection.keboola.com",
    "project": 1234,
    "stagingStorage": "s3",
    "backend": "snowflake",
    "token": "<your-token>",
    "legacyTransformation": false
  }
]
```

### 5. Build and Test

```bash
# Run tests
task tests

# Build local CLI binary
task build-local

# Run specific service (e.g., templates API)
export TEMPLATES_STORAGE_API_HOST=connection.keboola.com
go run cmd/templates-api/main.go

# Run stream service
export STREAM_STORAGE_API_HOST=connection.keboola.com
go run cmd/stream-api/main.go
```

### 6. Development Tools

#### Start Documentation Server
```bash
task godoc
# Access at http://localhost:6060/pkg/github.com/keboola/keboola-as-code/?m=all
```

#### Run Tests with Verbose Output
```bash
# Verbose output
TEST_VERBOSE=true go test -race -v ./...

# HTTP client verbose
TEST_HTTP_CLIENT_VERBOSE=true go test -race -v ./...

# ETCD operations verbose
ETCD_VERBOSE=true go test -race -v ./...
```

## Troubleshooting

### Common Issues

1. ETCD Connection Issues
   - Verify ETCD is running: `systemctl status etcd`
   - Check ETCD logs: `journalctl -u etcd`
   - Ensure correct permissions on ETCD data directory

2. Go Module Issues
   - Run `go mod tidy` to clean up dependencies
   - Run `go mod vendor` to update vendor directory
   - Check `GOPATH` is set correctly

3. Permission Issues
   - Ensure correct ownership of project files
   - Check ETCD data directory permissions
   - Verify write permissions in `/tmp` directory

## Service Endpoints

When running locally:
- Templates API: http://localhost:8000
- Stream Service: http://localhost:8001
- Apps Proxy: http://localhost:8002
- Metrics endpoints:
  - Templates: :9000
  - Stream: :9001
  - Apps Proxy: :9002 
