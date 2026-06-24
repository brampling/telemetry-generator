// Command controller serves the web UI and REST API for the telemetry
// generator, holds the live settings, and runs the scheduler that initiates
// traces. It is the single source of truth for the on/off switch and the
// density / span-time / trace-shape / auto-off knobs. Run a single replica:
// the settings live in memory and the scheduler owns the global rate.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brampling/telemetry-generator/internal/scheduler"
	"github.com/brampling/telemetry-generator/internal/settings"
	"github.com/brampling/telemetry-generator/internal/telemetry"
	"github.com/brampling/telemetry-generator/internal/ui"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := telemetry.NewLogger("controller")

	shutdown, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    env("OTEL_SERVICE_NAME", "telemetry-generator-controller"),
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

	store := settings.New(settings.Defaults())

	genURL := env("GENERATOR_ENDPOINT", "http://generator")
	sched, err := scheduler.New(store, genURL, logger)
	if err != nil {
		logger.ErrorContext(ctx, "scheduler init failed", "err", err)
		os.Exit(1)
	}
	go sched.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, store.State())
	})
	mux.HandleFunc("PUT /api/settings", func(w http.ResponseWriter, r *http.Request) {
		var next settings.Settings
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			http.Error(w, "invalid settings: "+err.Error(), http.StatusBadRequest)
			return
		}
		state := store.Update(next)
		logger.InfoContext(r.Context(), "settings updated",
			"enabled", state.Enabled, "density", state.Density,
			"workMillis", state.WorkMillis, "depth", state.Depth,
			"fanout", state.Fanout, "durationSeconds", state.DurationSeconds)
		writeJSON(w, http.StatusOK, state)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(ui.IndexHTML)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	logger.InfoContext(ctx, "controller listening", "addr", srv.Addr, "generator", genURL, "version", version)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.ErrorContext(ctx, "server error", "err", err)
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
