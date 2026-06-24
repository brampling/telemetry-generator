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
		// Plain client (no OTel transport): health polling should not generate
		// a trace on every synthetic tick.
		client:   &http.Client{Timeout: 2 * time.Second},
		resolver: net.DefaultResolver,
		now:      time.Now,
	}
}

// Check runs all probes and returns the aggregate report.
func (c *Checker) Check(ctx context.Context) Report {
	services := []ServiceStatus{{Name: "controller", Status: "ok"}}

	ips, err := c.resolver.LookupHost(ctx, c.headlessHost)
	if err != nil {
		// No generator endpoints resolvable at all — the fleet is unreachable.
		services = append(services, ServiceStatus{
			Name:   "generator",
			Status: "unhealthy",
			Detail: fmt.Sprintf("discovery failed: %v", err),
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

// probe hits /readyz on a single generator pod.
func (c *Checker) probe(ctx context.Context, ip string) ServiceStatus {
	st := ServiceStatus{Name: "generator", Instance: ip}
	url := fmt.Sprintf("http://%s/readyz", net.JoinHostPort(ip, c.port))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		st.Status, st.Detail = "unhealthy", err.Error()
		return st
	}
	start := c.now()
	resp, err := c.client.Do(req)
	st.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		st.Status, st.Detail = "unhealthy", err.Error()
		return st
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		st.Status, st.Detail = "unhealthy", fmt.Sprintf("status %d", resp.StatusCode)
		return st
	}
	st.Status = "ok"
	return st
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
