// Package gen produces synthetic distributed traces (plus correlated logs and
// metrics). Each generator pod handles a "work" request as one hop: it opens a
// few local spans, does jittered synthetic work, then fans out HTTP calls to
// peer pods through the generator Service. Because the Service load-balances
// across the 2-3 replicas spread over nodes, the child spans of a single trace
// are produced by different pods on different nodes — exactly the multi-pod
// trace shape we want.
package gen

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// downstreamOps are the synthetic operation names a hop can call. They read
// like a small microservice graph so traces look realistic in Dash0.
var downstreamOps = []string{
	"cart.add", "catalog.lookup", "inventory.reserve", "payment.authorize",
	"shipping.quote", "recommendation.fetch", "pricing.calculate",
	"user.profile", "review.summary", "notification.enqueue",
}

// localOps are the in-pod child spans each hop emits (work that doesn't cross
// the network), so every hop is itself multi-span.
var localOps = []string{"db.query", "cache.get", "serialize", "validate", "render"}

// errorRate is the fraction of hops that synthesize an error, so the generated
// data includes error spans/logs and isn't uniformly green.
const errorRate = 0.05

// Params controls one hop of trace generation. They are propagated to peers via
// query string (alongside the W3C trace context headers) so the whole trace
// shares a shape.
type Params struct {
	Op         string // operation name for this hop's server span
	Depth      int    // remaining hops below this one (1 = leaf)
	Fanout     int    // peer calls this hop makes (when Depth > 1)
	WorkMillis int    // baseline synthetic work per span
}

// FromRequest parses hop params from query parameters, applying defaults.
func FromRequest(r *http.Request) Params {
	q := r.URL.Query()
	p := Params{
		Op:         q.Get("op"),
		Depth:      atoiDefault(q.Get("depth"), 1),
		Fanout:     atoiDefault(q.Get("fanout"), 1),
		WorkMillis: atoiDefault(q.Get("work"), 25),
	}
	if p.Op == "" {
		p.Op = "request"
	}
	return p
}

func atoiDefault(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

// Generator carries the instruments and the peer endpoint used to handle and
// fan out work. It is safe for concurrent use.
type Generator struct {
	tracer  trace.Tracer
	logger  *slog.Logger
	client  *http.Client
	peerURL string

	spans  metric.Int64Counter
	errors metric.Int64Counter
	work   metric.Float64Histogram
}

// New builds a Generator. peerURL is the base URL of the generator Service
// (e.g. http://generator.telemetry-generator.svc.cluster.local:80); calls to it
// fan out across the replica set.
func New(peerURL string, logger *slog.Logger) (*Generator, error) {
	meter := otel.Meter("telemetry-generator/gen")
	spans, err := meter.Int64Counter("telemetrygen.spans.emitted",
		metric.WithDescription("Synthetic spans emitted by this generator"))
	if err != nil {
		return nil, err
	}
	errors, err := meter.Int64Counter("telemetrygen.spans.errors",
		metric.WithDescription("Synthetic spans that ended in error"))
	if err != nil {
		return nil, err
	}
	work, err := meter.Float64Histogram("telemetrygen.work.duration",
		metric.WithDescription("Synthetic per-span work duration"),
		metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	return &Generator{
		tracer: otel.Tracer("telemetry-generator/gen"),
		logger: logger,
		// otelhttp transport propagates trace context to peers and emits a
		// client span per call.
		client:  &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport), Timeout: 30 * time.Second},
		peerURL: peerURL,
		spans:   spans,
		errors:  errors,
		work:    work,
	}, nil
}

// Process handles one hop. The caller has already established the server span
// (via the otelhttp handler); here we add local child spans, do synthetic work,
// and fan out to peers when depth remains.
func (g *Generator) Process(ctx context.Context, p Params) {
	// A couple of in-pod child spans so every hop is multi-span on its own.
	n := 1 + rand.IntN(2)
	for i := 0; i < n; i++ {
		g.localSpan(ctx, localOps[rand.IntN(len(localOps))], p.WorkMillis)
	}

	if p.Depth <= 1 {
		return
	}

	// Fan out concurrently to peers. Each call lands on whichever replica the
	// Service picks, so children accumulate on other pods/nodes.
	var wg sync.WaitGroup
	for i := 0; i < p.Fanout; i++ {
		child := Params{
			Op:         downstreamOps[rand.IntN(len(downstreamOps))],
			Depth:      p.Depth - 1,
			Fanout:     p.Fanout,
			WorkMillis: p.WorkMillis,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.callPeer(ctx, child)
		}()
	}
	wg.Wait()
}

// localSpan emits a single in-pod work span with a log line and metrics, and
// occasionally synthesizes an error.
func (g *Generator) localSpan(ctx context.Context, op string, workMillis int) {
	ctx, span := g.tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	d := jitter(workMillis)
	span.SetAttributes(
		attribute.String("gen.op", op),
		attribute.Float64("gen.work_ms", float64(d.Milliseconds())),
	)
	time.Sleep(d)

	g.work.Record(ctx, float64(d.Milliseconds()), metric.WithAttributes(attribute.String("gen.op", op)))
	g.spans.Add(ctx, 1, metric.WithAttributes(attribute.String("gen.op", op)))

	if rand.Float64() < errorRate {
		err := fmt.Errorf("synthetic failure in %s", op)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		g.errors.Add(ctx, 1, metric.WithAttributes(attribute.String("gen.op", op)))
		g.logger.ErrorContext(ctx, "operation failed", "op", op, "duration_ms", d.Milliseconds())
		return
	}
	g.logger.InfoContext(ctx, "operation completed", "op", op, "duration_ms", d.Milliseconds())
}

// callPeer issues a child "work" request to the generator Service. The otelhttp
// transport injects trace context, so the peer's server span is a child of the
// current span and the trace continues on another pod.
func (g *Generator) callPeer(ctx context.Context, p Params) {
	u, _ := url.Parse(g.peerURL + "/work")
	q := u.Query()
	q.Set("op", p.Op)
	q.Set("depth", strconv.Itoa(p.Depth))
	q.Set("fanout", strconv.Itoa(p.Fanout))
	q.Set("work", strconv.Itoa(p.WorkMillis))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		g.logger.ErrorContext(ctx, "build peer request", "err", err)
		return
	}
	resp, err := g.client.Do(req)
	if err != nil {
		g.logger.ErrorContext(ctx, "peer call failed", "op", p.Op, "err", err)
		return
	}
	_ = resp.Body.Close()
}

// jitter returns base milliseconds +/- 40%, never negative, so span durations
// vary naturally.
func jitter(base int) time.Duration {
	if base <= 0 {
		return time.Duration(rand.IntN(5)) * time.Millisecond
	}
	factor := 0.6 + rand.Float64()*0.8 // 0.6 .. 1.4
	return time.Duration(float64(base)*factor) * time.Millisecond
}
