// Package health aggregates the health of every service in the app into a
// single report, so an external monitor (a Dash0 synthetic check) can poll one
// URL and learn whether the whole system is up.
//
// The controller is healthy by definition if it can answer the request. To
// reach every generator pod (not just the one a load-balanced Service would
// pick), the checker resolves the generator *headless* Service, which returns
// the address of each pod, and probes /readyz on each concurrently.
package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ServiceStatus is the health of one checked component.
type ServiceStatus struct {
	Name      string `json:"name"`
	Instance  string `json:"instance,omitempty"`
	Status    string `json:"status"` // "ok" | "unhealthy"
	Detail    string `json:"detail,omitempty"`
	LatencyMS int64  `json:"latencyMs,omitempty"`
}

// Report is the aggregate result. Status is "ok" only when every component is
// healthy and at least the expected number of generators were found.
type Report struct {
	Status     string           `json:"status"` // "ok" | "degraded"
	CheckedAt  string           `json:"checkedAt"`
	Generators GeneratorSummary `json:"generators"`
	Services   []ServiceStatus  `json:"services"`
}

// GeneratorSummary counts discovered vs. healthy vs. expected generator pods.
type GeneratorSummary struct {
	Expected   int `json:"expected"`
	Discovered int `json:"discovered"`
	Healthy    int `json:"healthy"`
}

// Healthy reports whether the overall status is "ok".
func (r Report) Healthy() bool { return r.Status == "ok" }

// Checker probes the generator fleet via the headless Service.
type Checker struct {
	headlessHost string
	port         string
	expected     int
	client       *http.Client
	resolver     *net.Resolver
	tracer       trace.Tracer
	propagator   propagation.TextMapPropagator
	now          func() time.Time
}

// New builds a Checker. headlessHost is the FQDN of the generator headless
// Service (its A records are the pod IPs); port is the generator container
// port; expected is the desired replica count (status is degraded if fewer
// healthy pods are found).
func New(headlessHost, port string, expected int) *Checker {
	return &Checker{
		headlessHost: headlessHost,
		port:         port,
		expected:     expected,
		// A /health poll is meant to produce a trace: it's how an operator
		// diagnoses a red synthetic from the trace alone, without touching the
		// cluster. Each probe span records its own failure detail, so a pod that
		// is down (and thus emits no server span of its own) still shows up here.
		client:     &http.Client{Timeout: 2 * time.Second},
		resolver:   net.DefaultResolver,
		tracer:     otel.Tracer("telemetry-generator/health"),
		propagator: otel.GetTextMapPropagator(),
		now:        time.Now,
	}
}

// Check runs all probes and returns the aggregate report.
func (c *Checker) Check(ctx context.Context) Report {
	services := []ServiceStatus{{Name: "controller", Status: "ok"}}

	ips, err := c.resolver.LookupHost(ctx, c.headlessHost)
	if err != nil {
		// No generator endpoints resolvable at all — the fleet is unreachable.
		// Record it on the server span so the dead synthetic is diagnosable from
		// the trace: there are no per-pod spans to fall back on in this case.
		detail := fmt.Sprintf("discovery failed: %v", err)
		span := trace.SpanFromContext(ctx)
		span.SetStatus(codes.Error, detail)
		span.SetAttributes(attribute.String("generator.headless_host", c.headlessHost))
		services = append(services, ServiceStatus{
			Name:   "generator",
			Status: "unhealthy",
			Detail: detail,
		})
		return c.finish(services, 0, 0)
	}
	sort.Strings(ips)

	var mu sync.Mutex
	var wg sync.WaitGroup
	healthy := 0
	for _, ip := range ips {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			st := c.probe(ctx, ip)
			mu.Lock()
			defer mu.Unlock()
			services = append(services, st)
			if st.Status == "ok" {
				healthy++
			}
		}(ip)
	}
	wg.Wait()

	// Keep generator entries in a stable order for readable output.
	sort.Slice(services, func(i, j int) bool {
		if services[i].Name != services[j].Name {
			return services[i].Name < services[j].Name
		}
		return services[i].Instance < services[j].Instance
	})

	return c.finish(services, len(ips), healthy)
}

// probe hits /probe on a single generator pod. It wraps the call in a client
// span carrying the pod identity, latency, and (on failure) the reason, then
// injects the trace context so the pod emits its own server span as a child.
// The span is the unit of troubleshooting: if this probe fails, the trace shows
// which pod and why — even when the pod is down and emits no span itself.
func (c *Checker) probe(ctx context.Context, ip string) ServiceStatus {
	ctx, span := c.tracer.Start(ctx, "probe generator",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("generator.instance", ip)))
	defer span.End()

	st := ServiceStatus{Name: "generator", Instance: ip}
	url := fmt.Sprintf("http://%s/probe", net.JoinHostPort(ip, c.port))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return c.fail(span, &st, err.Error())
	}
	c.propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))

	start := c.now()
	resp, err := c.client.Do(req)
	st.LatencyMS = time.Since(start).Milliseconds()
	span.SetAttributes(attribute.Int64("probe.latency_ms", st.LatencyMS))
	if err != nil {
		return c.fail(span, &st, err.Error())
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		return c.fail(span, &st, fmt.Sprintf("status %d", resp.StatusCode))
	}
	st.Status = "ok"
	span.SetStatus(codes.Ok, "")
	return st
}

// fail marks both the report entry and its span as unhealthy with the same
// reason, so the JSON body and the trace tell the identical story.
func (c *Checker) fail(span trace.Span, st *ServiceStatus, detail string) ServiceStatus {
	st.Status, st.Detail = "unhealthy", detail
	span.SetStatus(codes.Error, detail)
	return *st
}

func (c *Checker) finish(services []ServiceStatus, discovered, healthy int) Report {
	r := Report{
		Status:    "ok",
		CheckedAt: c.now().UTC().Format(time.RFC3339),
		Generators: GeneratorSummary{
			Expected:   c.expected,
			Discovered: discovered,
			Healthy:    healthy,
		},
		Services: services,
	}
	for _, s := range services {
		if s.Status != "ok" {
			r.Status = "degraded"
		}
	}
	if healthy < c.expected {
		r.Status = "degraded"
	}
	return r
}
