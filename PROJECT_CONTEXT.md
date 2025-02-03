# Keboola-as-Code Project Context

## Project Overview
This is a Go-based distributed system project that provides several microservices:
- Templates API: Manages and serves templates
- Stream Service: Handles data streaming operations
- Apps Proxy: Manages application proxying and routing

The project uses ETCD for distributed storage and synchronization, with a custom wrapper (etcdop) for enhanced functionality.

## Directory Structure

### Core Components
- `/api/`: API definitions and interfaces
- `/cmd/`: Service entry points (templates-api, stream-api, apps-proxy, kbc)
- `/internal/`: Core implementation
  - `/pkg/service/`: Service implementations
  - `/pkg/telemetry/`: OpenTelemetry integration
  - `/pkg/utils/`: Shared utilities

### Deployment & Infrastructure
- `/provisioning/`: Kubernetes and deployment configurations
  - `/stream/`: Stream service deployment
  - `/templates-api/`: Templates API deployment
  - `/apps-proxy/`: Apps Proxy deployment
  - `/cli-dist/`: CLI distribution
  - `/common/`: Shared resources
  - `/dev/`: Development environment

### Documentation & Testing
- `/docs/`: Project documentation
- `/test/`: Test fixtures and utilities
- `/scripts/`: Utility scripts

## Local Development

### Docker Setup
- Docker and Docker Compose
- Go 1.24+
- Make

For Docker-based development, see [Development Guide](docs/development.md).

### Local Machine Setup
For development directly on your local machine without Docker, see [Local Development Guide](docs/local_development.md).

### Services
- Templates API: http://localhost:8000
- Stream Service: http://localhost:8001
- Apps Proxy: http://localhost:8002
- Metrics:
  - Templates: :9000
  - Stream: :9001
  - Apps Proxy: :9002

### Development Tools
- ETCD UI: http://localhost:2379
- Prometheus: http://localhost:9090
- Redis: localhost:6379

## Deployment

### Local Deployment
```bash
docker-compose up -d
```

### Production Deployment
- Uses Kubernetes
- Configuration in `/provisioning/{service}/`
- Supports horizontal scaling
- Uses ETCD for coordination

### Infrastructure Requirements
- ETCD cluster
- Redis (for locking)
- Prometheus (metrics)
- Kubernetes cluster

## CI/CD Pipeline

### Testing
- Unit tests: `make test`
- Integration tests: `make integration-test`
- E2E tests: `make e2e-test`

### Build Process
1. Code validation
   - Linting
   - Static analysis
   - Security scanning
2. Testing
   - Unit tests
   - Integration tests
3. Build
   - Docker images
   - CLI binaries
4. Deployment
   - Staging
   - Production

### Monitoring & Observability
- OpenTelemetry integration
- Prometheus metrics
- Service health checks
- Distributed tracing

## Configuration Management
- Environment variables
- ETCD for distributed configuration
- Kubernetes ConfigMaps and Secrets

## Security
- ETCD authentication
- API authentication
- TLS encryption
- Secure cookie handling
- Sandboxed environments

## Performance Considerations
- ETCD operation batching
- Connection pooling
- Resource limits
- Horizontal scaling support 
