#!/usr/bin/env bash
# Install the Helm umbrella chart with all infrastructure dependencies
# into the running kind cluster. Idempotent: re-running upgrades.

set -euo pipefail

CHART_DIR="deploy/helm/processing-platform"
RELEASE_NAME="pp"      # short prefix for resources (pp-postgresql, pp-redis, ...)
NAMESPACE="default"    # everything goes in default namespace for simplicity

# Step 1: Add the Bitnami repo if not already added.
if ! helm repo list | grep -q "^bitnami"; then
  echo "→ adding bitnami helm repo..."
  helm repo add bitnami https://charts.bitnami.com/bitnami
fi
helm repo update

# Step 2: Pull dependency charts into ./charts/.
# This downloads postgresql, redis, kafka, minio (mosquitto is local).
echo "→ resolving chart dependencies..."
helm dependency update "${CHART_DIR}"

# Step 3: Install (or upgrade) the umbrella chart.
echo "→ installing umbrella chart as release '${RELEASE_NAME}'..."
helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --values "${CHART_DIR}/values-local.yaml" \
  --timeout 10m \
  --wait

echo
echo "✓ Infrastructure installed."
echo "  Run 'kubectl get pods' to see everything running."
