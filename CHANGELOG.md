cat > CHANGELOG.md <<'EOF'
# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Project scaffolding (Stage 0): repo, Makefile, Dockerfile, lint config, CI.
EOF

### Added 
#   - Local kind cluster with 3 nodes (1 control plane, 2 workers).
#   - Bootstrap script installing ingress-nginx, metrics-server, cert-manager.
#   - Helm umbrella chart depending on Bitnami Postgres/Redis/Kafka/MinIO + custom Mosquitto subchart.
#   - golang-migrate hook that runs database migrations on every helm install.