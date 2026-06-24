// Command generator is the telemetry worker. Each replica runs an HTTP server
// whose /work endpoint represents one hop of a synthetic trace: it creates
// spans, emits logs and metrics, and fans out to peers through the generator
// Service so a single trace is produced across multiple pods and nodes.
//
// It holds no state and never starts traces on its own — the controller drives
// it. Scaling the Deployment changes how many pods a trace can spread across.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brampling/telemetry-generator/internal/gen"
	"github.com/brampling/telemetry-generator/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := telemetry.NewLogger("generator")

	shutdown, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    env("OTEL_SERVICE_NAME", "telemetry-generator-generator"),
		ServiceVersion: version,
		Environment:    env("DEPLOY_ENV", "dev"),
	})
	if err != nil {
		logger.ErrorContext(ctx, "telemetry setup failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(sctx)
	}()

	// Calls fan out back through the Service in front of the generator pods.
	peerURL := env("PEER_ENDPOINT", "http://generator")
	g, err := gen.New(peerURL, logger)
	if err != nil {
		logger.ErrorContext(ctx, "generator init failed", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	// /work is the hop entrypoint. otelhttp creates the server span (extracting
	// any incoming trace context) and names it after the requested operation.
	work := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.Process(r.Context(), gen.FromRequest(r))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/work", otelhttp.NewHandler(work, "work",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if op := r.URL.Query().Get("op"); op != "" {
				return op
			}
			return r.Method + " " + r.URL.Path
		}),
	))
	// Kubelet liveness/readiness probes are intentionally not instrumented: they
	// fire constantly and would bury the real traces in probe-span noise.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	// /probe is the controller health checker's per-pod entrypoint. Unlike the
	// kubelet probes it IS instrumented (otelhttp extracts the incoming trace
	// context from the synthetic check) so each /health poll produces a span on
	// every generator pod, fanning the synthetic trace across the fleet.
	mux.Handle("/probe", otelhttp.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
		"probe",
	))

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	logger.InfoContext(ctx, "generator listening", "addr", srv.Addr, "peer", peerURL, "version", version)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.ErrorContext(ctx, "server error", "err", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
