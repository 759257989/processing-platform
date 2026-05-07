#!/usr/bin/env bash
# Bring up a local kind cluster with the cluster-level add-ons we need.
# Idempotent: re-running this is safe.

set -euo pipefail

CLUSTER_NAME="processing-platform"
KIND_CONFIG="deploy/kind/kind-config.yaml"

# Step 1: create the cluster if it doesn't already exist.
if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  echo "✓ kind cluster '${CLUSTER_NAME}' already exists — skipping creation"
else
  echo "→ creating kind cluster '${CLUSTER_NAME}' (this takes ~1 minute)..."
  kind create cluster --name "${CLUSTER_NAME}" --config "${KIND_CONFIG}"
fi

# Step 2: make sure kubectl points at the new cluster.
kubectl cluster-info --context "kind-${CLUSTER_NAME}"

# Step 3: install ingress-nginx. We'll use this when the API service exists
# in Stage 2 to expose it at http://localhost.
echo "→ installing ingress-nginx..."
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.10.0/deploy/static/provider/kind/deploy.yaml

# Wait for the ingress controller to be ready. Without this, the script
# could "succeed" while the controller is still starting.
echo "→ waiting for ingress-nginx to be ready (up to 5 min)..."
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=300s

# Step 4: install metrics-server. The HPA in Stage 5 needs this to read
# pod CPU/memory. `kubectl top pods` also needs it.
echo "→ installing metrics-server..."
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.1/components.yaml

# Patch metrics-server to skip TLS verification — required when running
# inside kind because kind nodes use self-signed kubelet certs.
kubectl patch -n kube-system deployment metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

# Step 5: install cert-manager. We may use it in Phase 2 for TLS;
# it is cheap to install now and avoids a future "install + wait" hop.
echo "→ installing cert-manager..."
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.5/cert-manager.yaml

echo "→ waiting for cert-manager to be ready..."
kubectl wait --namespace cert-manager \
  --for=condition=available deployment \
  --selector=app.kubernetes.io/instance=cert-manager \
  --timeout=300s

echo
echo "✓ Bootstrap complete. Cluster '${CLUSTER_NAME}' is up."
echo "  Run 'kubectl get nodes' to confirm 3 nodes are Ready."
