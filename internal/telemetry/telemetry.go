// Package telemetry wires OpenTelemetry explicitly: we construct the trace,
// metric, and log providers ourselves and export everything over OTLP/gRPC to
// the endpoint in OTEL_EXPORTER_OTLP_ENDPOINT (the Dash0 operator's cluster
// collector in-cluster, or a custom collector later). No auto-instrumentation /
// agent injection is used — the namespace's Dash0Monitoring resource sets
// instrumentWorkloads.mode=none.
//
// This package is shared by both services (controller and generator); each
// passes its own Config so the telemetry is correctly attributed.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config identifies a service in the telemetry it produces.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
}

// Setup installs global trace/meter/logger providers and returns a shutdown
// function that flushes and closes them (call it on exit). It samples every
// trace (AlwaysSample) — this is a telemetry generator, the whole point is to
// emit the traces it creates.
func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(), // OTEL_RESOURCE_ATTRIBUTES: k8s.* from the downward API
		resource.WithTelemetrySDK(),
		resource.WithProcessRuntimeDescription(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			semconv.DeploymentEnvironment(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	var shutdowns []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		var errs error
		for i := len(shutdowns) - 1; i >= 0; i-- {
			errs = errors.Join(errs, shutdowns[i](ctx))
		}
		return errs
	}

	// W3C trace context + baggage so trace IDs propagate across services
	// (controller -> generator -> generator), giving multi-pod traces.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	// Traces.
	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return shutdown, fmt.Errorf("trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	shutdowns = append(shutdowns, tp.Shutdown)

	// Metrics.
	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return shutdown, fmt.Errorf("metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(15*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	shutdowns = append(shutdowns, mp.Shutdown)

	// Go runtime metrics (GC, goroutines, memory), explicitly enabled.
	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
		return shutdown, fmt.Errorf("runtime metrics: %w", err)
	}

	// Logs.
	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return shutdown, fmt.Errorf("log exporter: %w", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)
	otellog.SetLoggerProvider(lp)
	shutdowns = append(shutdowns, lp.Shutdown)

	return shutdown, nil
}

// NewLogger returns an slog.Logger that fans out to stderr (so logs show in
// `kubectl logs`) and to the OTel log pipeline (so they reach Dash0 with trace
// correlation). Use the *Context methods so span context is attached.
func NewLogger(name string) *slog.Logger {
	stderr := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	otelHandler := otelslog.NewHandler(name)
	return slog.New(fanout{handlers: []slog.Handler{stderr, otelHandler}})
}

// fanout is a slog.Handler that dispatches each record to several handlers.
type fanout struct{ handlers []slog.Handler }

func (f fanout) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f fanout) Handle(ctx context.Context, r slog.Record) error {
	var err error
	for _, h := range f.handlers {
		if h.Enabled(ctx, r.Level) {
			err = errors.Join(err, h.Handle(ctx, r.Clone()))
		}
	}
	return err
}

func (f fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		hs[i] = h.WithAttrs(attrs)
	}
	return fanout{handlers: hs}
}

func (f fanout) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		hs[i] = h.WithGroup(name)
	}
	return fanout{handlers: hs}
}
