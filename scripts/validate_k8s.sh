#!/usr/bin/env bash
#
# Build every kustomize overlay and validate the result against the Kubernetes
# schemas. No cluster is contacted and none is needed — see k8s/README.md, which
# opens with the same banner infra/aws/README.md does.
#
# Two tools, because they check different things and neither subsumes the other:
#
#   kustomize build   — that the overlays compose. Catches a patch whose target
#                       matches nothing, a resource path that does not exist, and a
#                       ConfigMap reference that no generator satisfies.
#   kubeconform       — that the built YAML is valid Kubernetes. Catches a field
#                       that does not exist in the API version it is under, which
#                       kustomize is perfectly happy to pass through: it treats
#                       manifests as annotated YAML, not as typed objects.
#
# What neither catches is admission: a webhook, a quota, a PodSecurity rejection or
# a scheduler that cannot place the pod. `kind` is what would catch those, and this
# script does not create one.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The version to validate against. Deliberately not "master": a manifest that is
# valid against whatever Kubernetes merged this week is not a useful thing to know.
# 1.31 is old enough that every managed provider offers it and new enough for
# `PodDisruptionBudget` in policy/v1 and the automatic namespace labels the
# NetworkPolicies select on.
K8S_VERSION="${K8S_VERSION:-1.31.0}"

# Where kubeconform caches downloaded schemas. It fetches them over HTTPS on a cold
# cache — the core schemas from yannh/kubernetes-json-schema and the ArgoCD
# Application's from datreeio/CRDs-catalog — and reads the cache on every run after
# that. Under .cache/ so it is gitignored and so CI can key on it.
CACHE_DIR="${K8S_SCHEMA_CACHE:-.cache/kubeconform}"

SCHEMA_LOCATIONS=(
  -schema-location default
  # Custom resources are not in the default location, so without this the ArgoCD
  # Application is either skipped or an error depending on the flags, and "skipped"
  # is the outcome to be afraid of: the file that would break the deployment is
  # the one nothing checked.
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
)

for tool in kustomize kubeconform; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: $tool is not on PATH." >&2
    case "$tool" in
      kustomize) echo "  https://github.com/kubernetes-sigs/kustomize/releases" >&2 ;;
      kubeconform) echo "  https://github.com/yannh/kubeconform/releases" >&2 ;;
    esac
    exit 1
  fi
done

mkdir -p "$CACHE_DIR"

# `-strict` is the flag that makes this worth running. Without it, kubeconform
# accepts unknown fields, so a `replicas` misspelt as `replicaCount` validates
# clean and then silently does nothing on the cluster. With it, that is an error
# here.
KUBECONFORM=(
  kubeconform
  -strict
  -summary
  -kubernetes-version "$K8S_VERSION"
  -cache "$CACHE_DIR"
  "${SCHEMA_LOCATIONS[@]}"
)

status=0

for overlay in k8s/overlays/*/; do
  name="$(basename "$overlay")"
  echo "== overlay: $name"
  # The build and the validation are piped rather than staged through a temporary
  # file so that a build failure fails the pipeline: `set -o pipefail` is on, and
  # kubeconform reading an empty stdin would otherwise report success.
  kustomize build "$overlay" | "${KUBECONFORM[@]}" - || status=1
done

# The ArgoCD Application is not part of any overlay — it is applied once by hand to
# bootstrap the rest — so it is validated on its own.
echo "== argocd bootstrap"
"${KUBECONFORM[@]}" k8s/argocd/application.yaml || status=1

if [[ $status -ne 0 ]]; then
  echo "k8s validation failed." >&2
  exit 1
fi

echo "k8s manifests valid against Kubernetes ${K8S_VERSION}."
