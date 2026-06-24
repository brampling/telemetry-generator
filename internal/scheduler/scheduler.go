// Package scheduler drives trace generation from the controller. While
// generation is enabled it starts a root span at the configured rate and calls
// the generator Service to continue the trace downstream, so every trace begins
// in the controller and fans out across the generator pods.
package scheduler

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/brampling/telemetry-generator/internal/settings"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// rootOps name the synthetic transaction each trace represents (the controller's
// root span), so traces look like real user requests entering a system.
var rootOps = []string{
	"GET /checkout", "POST /orders", "GET /products", "GET /cart",
	"POST /payments", "GET /home", "GET /search",
}

// maxInflight caps concurrent triggers so a slow or backed-up generator fleet
// can't let goroutines pile up without bound; excess ticks are dropped.
const maxInflight = 64

// Scheduler periodically triggers traces according to the live settings.
type Scheduler struct {
	store   *settings.Store
	logger  *slog.Logger
	tracer  trace.Tracer
	client  *http.Client
	genURL  string
	sem     chan struct{}
	started metric.Int64Counter
	dropped metric.Int64Counter
}

// New builds a Scheduler. genURL is the base URL of the generator Service.
func New(store *settings.Store, genURL string, logger *slog.Logger) (*Scheduler, error) {
	meter := otel.Meter("telemetry-generator/scheduler")
	started, err := meter.Int64Counter("telemetrygen.traces.started",
		metric.WithDescription("Traces initiated by the controller"))
	if err != nil {
		return nil, err
	}
	dropped, err := meter.Int64Counter("telemetrygen.traces.dropped",
		metric.WithDescription("Trace ticks dropped because too many were in flight"))
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		store:   store,
		logger:  logger,
		tracer:  otel.Tracer("telemetry-generator/scheduler"),
		client:  &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport), Timeout: 30 * time.Second},
		genURL:  genURL,
		sem:     make(chan struct{}, maxInflight),
		started: started,
		dropped: dropped,
	}, nil
}

// Run drives generation until ctx is cancelled. Each loop reads the current
// settings (so live UI edits take effect immediately) and waits 1/density
// before the next trace. When disabled it idles, polling for the switch to flip.
func (s *Scheduler) Run(ctx context.Context) {
	const idle = 250 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		st := s.store.Get()
		if !st.Enabled || st.Density <= 0 {
			if !sleep(ctx, idle) {
				return
			}
			continue
		}
		s.trigger(ctx, st)
		interval := time.Duration(float64(time.Second) / st.Density)
		if !sleep(ctx, interval) {
			return
		}
	}
}

// trigger starts one root span and calls the generator to extend the trace. It
// is non-blocking beyond acquiring an in-flight slot; the actual HTTP call runs
// in its own goroutine so the scheduler keeps to its cadence.
func (s *Scheduler) trigger(ctx context.Context, st settings.Settings) {
	select {
	case s.sem <- struct{}{}:
	default:
		s.dropped.Add(ctx, 1)
		return
	}
	go func() {
		defer func() { <-s.sem }()

		op := rootOps[rand.IntN(len(rootOps))]
		ctx, span := s.tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		span.SetAttributes(
			attribute.String("gen.entrypoint", op),
			attribute.Int("gen.depth", st.Depth),
			attribute.Int("gen.fanout", st.Fanout),
		)

		u, _ := url.Parse(s.genURL + "/work")
		q := u.Query()
		q.Set("op", "gateway.route")
		q.Set("depth", strconv.Itoa(st.Depth))
		q.Set("fanout", strconv.Itoa(st.Fanout))
		q.Set("work", strconv.Itoa(st.WorkMillis))
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
		if err != nil {
			s.logger.ErrorContext(ctx, "build generator request", "err", err)
			return
		}
		resp, err := s.client.Do(req)
		if err != nil {
			s.logger.ErrorContext(ctx, "generator call failed", "op", op, "err", err)
			return
		}
		_ = resp.Body.Close()
		s.started.Add(ctx, 1, metric.WithAttributes(attribute.String("gen.entrypoint", op)))
	}()
}

// sleep waits for d or until ctx is cancelled; it returns false if cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
