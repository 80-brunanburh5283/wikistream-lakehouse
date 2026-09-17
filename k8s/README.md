# Dagster on Kubernetes — validated in CI, never applied

**Read this first.** These manifests have never been applied to a cluster. There is
no kubeconfig in this repository, no cluster credential, and nothing in CI that
could reach one. What CI does with this directory is `kustomize build` and
`kubeconform -strict`, which check that the overlays compose and that the result is
valid Kubernetes — see [How it is checked](#how-it-is-checked). The same disclaimer
as [`infra/aws/`](../infra/aws/README.md), for the same reason.

If you want to run the pipeline, run `make up` in the repository root. That works
and it is what the README documents. This is here to answer a narrower question:
the Compose deployment makes several decisions that are only correct on one machine,
and a cluster is where they stop being correct. This directory is those decisions,
changed.

## What is here, and what is deliberately not

Only the Dagster control plane — the webserver and the daemon.

Kafka, Spark, MinIO and Trino are not here. They could be: there are decent Helm
charts for all four, and Strimzi and the Spark operator are how those two are
usually run. They are absent because a second data plane is a second thing to keep
honest, and the first thing a reader would find is that it had drifted from the one
in `docker-compose.yml` that the tests actually exercise. One deployment described
accurately is worth more than two described optimistically.

Dagster is the exception because it is the component whose deployment genuinely
changes on a cluster, and the change is not cosmetic:

| | Compose | Kubernetes |
|---|---|---|
| Instance storage | SQLite on a shared volume | Postgres |
| dbt project | bind mounted from the working tree | baked into the image |
| Webserver replicas | 1 | 2 in `overlays/prod` |
| Daemon rollout | recreate (one container) | `strategy: Recreate`, explicitly |
| Step logs | a named volume, survives `make down` | pod-local, lost on eviction |
| Secrets | none needed | one, and it is not in this repository |

## Why Postgres here and SQLite there

`DECISIONS.md` ADR-0033 chose SQLite for the Compose deployment and named the
condition that would reverse it: more than one process needing the run store from
more than one filesystem. Kubernetes is exactly that condition.

The webserver and the daemon are separate pods. They must share the run store — the
daemon writes a run, the launcher executes it, the webserver reads its events out —
and two pods cannot share a file. A `ReadWriteMany` volume would let them try, and
SQLite over NFS is a well-known way to corrupt a database slowly. Postgres is also
what makes the webserver replicable, which `overlays/prod` uses: either replica can
serve any run's event log because neither owns it.

So the same argument reaches opposite conclusions on the two platforms, which is why
both are written down. On a laptop, Postgres is a container, 200 MB of resident set
and one more thing to be healthy before `make up` returns, to serve a handful of
writes a minute. On a cluster it is the only thing that works.

## The image

`docker/dagster-k8s.Dockerfile` is three instructions on top of the image
`docker/dagster.Dockerfile` builds: it copies `dbt/` to `/opt/dbt`, read-only.

That is needed because the Compose deployment bind mounts `./dbt` from the working
tree, which is right on a laptop — the entrypoint parses the project at start-up, so
editing a model changes both the SQL dbt runs and the graph Dagster displays without
a rebuild — and impossible on a cluster, which has no working tree. The rejected
alternatives are in the Dockerfile's header: an init container cloning this
repository, which makes every pod start depend on GitHub, and a ConfigMap, which
cannot represent `models/staging/` because a ConfigMap key cannot contain a slash.

```bash
make build-dagster-k8s      # builds the base image, then this one
```

## The two objects this repository does not create

Both by design, and the distinction between them is the interesting part.

**`dagster-postgres`, a Secret.** One key, `DAGSTER_PG_PASSWORD`. It is not in git
in any form — not as a sealed secret, not as a base64 blob, not as an
obviously-fake local default. The database in `overlays/local` reads its
`POSTGRES_PASSWORD` from the same key, so there is exactly one copy of it and no way
for two copies to diverge.

```bash
kubectl create namespace wikistream
kubectl -n wikistream create secret generic dagster-postgres \
  --from-literal=DAGSTER_PG_PASSWORD="$(openssl rand -base64 24)"
```

On a real cluster that command is the wrong shape — a password nobody can rotate and
nothing audited. There it comes from the External Secrets Operator pointing at
Secrets Manager or Vault, or from SOPS-encrypted YAML that ArgoCD decrypts. Either
is a component this repository would be describing rather than owning, so it
describes it here instead.

**`wikistream-endpoints`, a ConfigMap.** The addresses of Trino, Kafka and Postgres.
`overlays/local` generates this, because cluster-local service names are knowable
in advance. `overlays/prod` cannot, and that is the boundary worth naming: an MSK
bootstrap string is issued by AWS and differs per account — it is literally an
output of `infra/aws/outputs.tf` — so it is a value that arrives from the
environment. The Ingress hostname in the same overlay *is* in git, because a
hostname is a convention someone chose rather than an identifier something
generated.

```bash
kubectl -n wikistream create configmap wikistream-endpoints \
  --from-literal=WS_TRINO_HOST=trino.analytics.svc.cluster.local \
  --from-literal=WS_TRINO_PORT=8080 \
  --from-literal=WS_KAFKA_BOOTSTRAP_SERVERS="$(terraform -chdir=../infra/aws output -raw kafka_bootstrap_servers)" \
  --from-literal=DAGSTER_PG_HOST=... \
  --from-literal=DAGSTER_PG_DB=dagster \
  --from-literal=DAGSTER_PG_USERNAME=dagster
```

That `terraform output` will not work, because the module has never been applied and
has no state. It is written that way to show where the value comes from.

## How it is checked

```bash
make k8s-validate
```

Two tools, because they catch different things:

| Tool | Catches | Misses |
|---|---|---|
| `kustomize build` | a patch whose target matches nothing, a missing resource path, a ConfigMap reference no generator satisfies | anything about Kubernetes itself — it treats manifests as annotated YAML |
| `kubeconform -strict` | a field that does not exist in the API version it is under, a misspelling that would otherwise be silently ignored | admission: webhooks, quotas, PodSecurity rejections, a scheduler with nowhere to put the pod |

`-strict` is what makes the second one worth running. Without it, `replicas`
misspelt as `replicaCount` validates clean and then does nothing on the cluster;
with it, that is an error. The ArgoCD `Application` is a custom resource, so its
schema comes from the [CRDs-catalog](https://github.com/datreeio/CRDs-catalog) — the
alternative is `-ignore-missing-schemas`, which reports it as skipped, and a file
nothing checked is the file that breaks the deployment.

Both run in `.github/workflows/infra.yml`. Neither needs a cluster; both fetch
JSON schemas over HTTPS on a cold cache and read `.cache/kubeconform` after that.

## What it would take to trust this

Short list, and naming it is more useful than implying the directory is finished:

- **An apply.** `kubeconform` proves the manifests are well-formed. It cannot prove
  the webserver starts, that the probes pass, that the daemon's liveness command
  exits 0 in that image, or that `fsGroup: 70` makes the Postgres volume writable on
  a given storage class. A `kind` cluster would answer all four in about ten
  minutes, and it is the obvious next thing to do here.
- **NetworkPolicy enforcement.** The four policies in `base/` are enforced by the
  CNI, not by the API server, so on a cluster whose CNI does not implement them they
  are accepted and ignored. `kubectl get networkpolicy` looks identical either way.
  kind's default CNI is one of those; Calico or Cilium is what makes them real.
- **The step logs.** The default compute log manager writes captured stdout to the
  pod's own disk, so an evicted pod takes its logs with it. `dagster-aws`'s S3
  compute log manager is the fix and it is not installed in the image — configuring
  a log manager that cannot be imported would turn a missing feature into a crash
  loop.
- **Authentication on the UI.** Open-source Dagster has no login. Anyone who can
  reach the webserver can launch and cancel every job. `overlays/prod` puts nginx
  basic auth in front of it, which is a floor, not an answer; the answer is an
  identity-aware proxy.
- **A run launcher decision, revisited.** These pods keep Dagster's default
  launcher, so runs execute as subprocesses inside the webserver or the daemon —
  identical to Compose. The Kubernetes-native choice is `K8sRunLauncher`, one Job
  per run, which isolates failures and lets a run request its own resources.
  It needs `dagster-k8s` in the image and an RBAC Role, neither of which is here,
  and it would be the right change to make first if this were going to run
  anything real.

## Bootstrap order

The dependency that is easy to get wrong: pods reference the Secret and the
ConfigMap through `envFrom` with no `optional: true`, so they will not start until
both exist. That is on purpose — the alternative is a pod that starts and connects
to a database nobody meant it to.

```bash
kubectl apply -k k8s/overlays/local     # namespace, Postgres, Dagster
kubectl -n wikistream create secret generic dagster-postgres --from-literal=...
kubectl -n wikistream rollout status deploy/dagster-webserver
kubectl -n wikistream port-forward svc/dagster-webserver 3000:3000
```

For `overlays/prod`, the equivalent is one `kubectl apply -f k8s/argocd/application.yaml`
after the two objects above exist, and then never `kubectl apply` again — that is
what the `selfHeal` and `prune` settings in the Application are for.
