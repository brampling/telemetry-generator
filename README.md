# telemetry-generator

A small web tool running on the `picluster` Kubernetes cluster that, when
switched on, emits **synthetic multi-span traces, logs, and metrics** to Dash0.
Built to put realistic, multi-pod telemetry on the wire on demand — for testing
dashboards, alerts, collectors, and the Dash0 pipeline itself.

- **Controller** — Go service with an embedded web UI (the on/off switch and the
  density / span-time / trace-shape / auto-off knobs), a settings API, and the
  scheduler that initiates traces at the configured rate.
- **Generator** — Go worker fleet (3 replicas, one per node). Each `/work`
  request is one hop of a trace: it opens spans, emits logs and metrics, then
  fans out to peers through the generator Service. Because that Service
  load-balances across the replicas, a single trace's spans are produced by
  **multiple pods across multiple nodes**.

## Architecture & conventions

- **GitOps:** ArgoCD watches the `home-lab` repo's app-of-apps (`k8s/apps/`). An
  `Application` there (`telemetry-generator.yaml`) points back at this repo's
  [`k8s/`](k8s/) kustomize base, so app code and deploy manifests live together
  here.
- **Images:** built by GitHub Actions for **linux/arm64** (the cluster is
  Raspberry Pi) and pushed to `ghcr.io/brampling/telemetry-generator-*`. The
  deployed tags are pinned in [`k8s/kustomization.yaml`](k8s/kustomization.yaml).
- **Telemetry — explicit, not auto:** both services ship their own OpenTelemetry
  SDK and export OTLP (traces, metrics, logs) to the Dash0 operator's cluster
  collector at
  `dash0-operator-opentelemetry-collector-service.dash0-system.svc.cluster.local:4317`.
  The endpoint is an env var (`OTEL_EXPORTER_OTLP_ENDPOINT`) on each Deployment,
  so pointing at a custom collector later is a one-line edit. The namespace's
  `Dash0Monitoring` resource sets `instrumentWorkloads.mode: none` so the
  operator does **not** auto-instrument these pods on top of the synthetic data.
- **Secrets:** kept out of git (homelab convention) and created imperatively.

## How a trace is shaped

```
controller (root span: "GET /checkout")
   └─ POST /work  ──▶  generator pod A   (span: "gateway.route")
                          ├─ db.query, cache.get        (in-pod child spans)
                          ├─ POST /work ──▶ generator pod B  ("catalog.lookup")
                          └─ POST /work ──▶ generator pod C  ("payment.authorize")
                                              └─ POST /work ──▶ pod A/B/C ...
```

- **Depth** controls how many hops deep the fan-out goes.
- **Fan-out** controls how many peer calls each hop makes.
- **Span time** is the baseline synthetic work per span (jittered ±40%).
- **Density** is how many of these traces start per second.
- **Auto-off** stops generation after the configured window (default **10 min**)
  so a forgotten run doesn't flood Dash0 indefinitely.

~5% of spans synthesize an error (error status + error log) so the data isn't
uniformly green.

## Controls (web UI)

The controller serves a single-page panel at `/`. Every knob is live — Save &
apply pushes the new settings to the running scheduler immediately.

| Control | Meaning | Default |
|---|---|---|
| Generation | Master on/off | off |
| Density | Traces started per second | 2/s |
| Span time | Baseline synthetic work per span (ms) | 50 ms |
| Trace depth | Hops a trace fans out across pods | 3 |
| Fan-out | Peer calls per hop | 2 |
| Auto-off after | Stops itself when this elapses (0 = never) | 600 s |

## Deploy

GitOps via the `home-lab` repo. Two one-time prerequisites (not in git):

```sh
# 1. Namespace (ArgoCD also creates it via CreateNamespace=true).
kubectl create namespace telemetry-generator

# 2. GHCR pull secret — the images are private. Use a GitHub PAT with
#    read:packages (the "new github key" for this project).
kubectl -n telemetry-generator create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=brampling \
  --docker-password='<GHCR_PAT_WITH_read:packages>'
```

Then register the app by committing `k8s/apps/telemetry-generator.yaml` to the
`home-lab` repo — the app-of-apps root picks it up and ArgoCD syncs this repo's
`k8s/` base. No Dash0 token is needed here: telemetry flows through the operator
collector, which already holds it.

### Access the UI

- **In-cluster / LAN:** `kubectl -n telemetry-generator port-forward svc/controller 8080:80` then open <http://localhost:8080>.
- **Public:** add a Cloudflare Tunnel public hostname in the Cloudflare Zero Trust
  dashboard with **Service** = `http://controller.telemetry-generator.svc.cluster.local:80`
  (`cloudflared` runs in-cluster, so it resolves the Service name directly).

### Health check (Dash0 synthetic)

`GET /health` on the controller aggregates the health of the whole app into one
response for an external monitor:

- **`200`** when the controller and every generator pod are healthy.
- **`503`** when anything is degraded — a generator pod failing `/readyz`, or
  fewer than `GENERATOR_EXPECTED` (`3`, matching the generator replica count)
  healthy pods discovered.

The controller resolves the `generator-headless` Service (one DNS record per
pod) and probes each pod's `/readyz` concurrently, so a single `/health` poll
covers all pods — not just the one a load-balanced Service would hit. The JSON
body breaks the result down per service with per-pod latency.

Point a Dash0 synthetic check at `https://<your-hostname>/health` and assert on
HTTP `200` (optionally also on body `"status":"ok"`).

> Keep `GENERATOR_EXPECTED` on the controller Deployment in sync with the
> generator `replicas` — otherwise a removed/added replica is misreported.

### Verify it's working

```sh
kubectl -n telemetry-generator get pods -o wide      # 1 controller + 3 generators, spread across nodes
kubectl -n telemetry-generator logs deploy/controller -f
```

Turn generation on in the UI, then look in Dash0: traces named like
`GET /checkout` with spans attributed to different `k8s.pod.name` / `k8s.node.name`.

## CI

| Workflow | Trigger | Does |
|---|---|---|
| `build-controller` | changes under `cmd/controller`, `internal`, module, Dockerfile | `go vet` + `go test`, then build/push the arm64 controller image |
| `build-generator` | changes under `cmd/generator`, `internal`, module, Dockerfile | `go vet` + `go test`, then build/push the arm64 generator image |
| `gitleaks` | every push/PR | secret scan over full history |

Images push with the workflow's automatic `GITHUB_TOKEN` (no custom secret).
After a build, bump the pinned tags in `k8s/kustomization.yaml` to the new
`sha-…` so ArgoCD rolls it out.

## Layout

```
.
├── cmd/
│   ├── controller/      # UI + API + scheduler entrypoint
│   └── generator/       # worker entrypoint
├── internal/
│   ├── telemetry/       # shared OTel SDK setup (traces/metrics/logs -> OTLP)
│   ├── settings/        # live config store + auto-off
│   ├── scheduler/       # rate-driven trace initiator (controller)
│   ├── gen/             # per-hop span/log/metric generation + peer fan-out
│   └── ui/              # embedded single-page control panel
├── Dockerfile.controller
├── Dockerfile.generator
└── k8s/                 # kustomize base (ArgoCD points here)
```
