// Package settings holds the live generation configuration that the UI edits
// and the scheduler reads. State lives only in the controller's memory: on
// restart it resets to disabled, which is the safe default for a load tool.
package settings

import (
	"sync"
	"time"
)

// Bounds for each knob. The UI clamps too, but the store is the source of truth.
const (
	MaxDensity      = 100.0 // traces/sec
	MaxWorkMillis   = 2000
	MaxDepth        = 8
	MaxFanout       = 5
	MaxDurationSecs = 24 * 60 * 60 // 24h
)

// Settings is the user-facing generation config. It is serialised to/from the
// UI as JSON.
type Settings struct {
	// Enabled is the on/off switch. When true the scheduler emits traces.
	Enabled bool `json:"enabled"`
	// Density is the target trace generation rate in traces per second.
	Density float64 `json:"density"`
	// WorkMillis is the baseline synthetic work each span performs, in
	// milliseconds (jittered per span). Higher values => longer spans.
	WorkMillis int `json:"workMillis"`
	// Depth is how many hops deep each trace fans out (controller -> gen -> gen
	// ...). More depth => taller traces spanning more pods.
	Depth int `json:"depth"`
	// Fanout is how many downstream peer calls each hop makes.
	Fanout int `json:"fanout"`
	// DurationSeconds auto-disables generation this many seconds after it is
	// turned on. 0 means run until manually stopped.
	DurationSeconds int `json:"durationSeconds"`
}

// Defaults returns the initial settings: off, a modest steady rate, a
// multi-pod trace shape, and a 10-minute auto-off so a forgotten run stops
// itself.
func Defaults() Settings {
	return Settings{
		Enabled:         false,
		Density:         2,
		WorkMillis:      50,
		Depth:           3,
		Fanout:          2,
		DurationSeconds: 600,
	}
}

// clamp coerces every field into its valid range.
func (s Settings) clamp() Settings {
	if s.Density < 0 {
		s.Density = 0
	}
	if s.Density > MaxDensity {
		s.Density = MaxDensity
	}
	if s.WorkMillis < 0 {
		s.WorkMillis = 0
	}
	if s.WorkMillis > MaxWorkMillis {
		s.WorkMillis = MaxWorkMillis
	}
	if s.Depth < 1 {
		s.Depth = 1
	}
	if s.Depth > MaxDepth {
		s.Depth = MaxDepth
	}
	if s.Fanout < 1 {
		s.Fanout = 1
	}
	if s.Fanout > MaxFanout {
		s.Fanout = MaxFanout
	}
	if s.DurationSeconds < 0 {
		s.DurationSeconds = 0
	}
	if s.DurationSeconds > MaxDurationSecs {
		s.DurationSeconds = MaxDurationSecs
	}
	return s
}

// State is a settings snapshot plus derived runtime info for the UI.
type State struct {
	Settings
	// RemainingSeconds is the time left before auto-off, when enabled with a
	// duration set; nil otherwise.
	RemainingSeconds *int `json:"remainingSeconds,omitempty"`
}

// Store is the concurrency-safe holder of the live settings. It also enforces
// the auto-off duration: once an enabled run exceeds its duration, the next
// read reports it disabled.
type Store struct {
	mu        sync.Mutex
	cur       Settings
	enabledAt time.Time // when Enabled last transitioned false->true
	now       func() time.Time
}

// New returns a Store seeded with the given settings.
func New(initial Settings) *Store {
	return &Store{cur: initial.clamp(), now: time.Now}
}

// expireLocked turns generation off if its auto-off duration has elapsed.
// Caller must hold s.mu.
func (s *Store) expireLocked() {
	if s.cur.Enabled && s.cur.DurationSeconds > 0 {
		deadline := s.enabledAt.Add(time.Duration(s.cur.DurationSeconds) * time.Second)
		if !s.now().Before(deadline) {
			s.cur.Enabled = false
		}
	}
}

// Get returns the current settings, after applying any pending auto-off.
func (s *Store) Get() Settings {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	return s.cur
}

// State returns the current settings plus derived UI info (auto-off countdown).
func (s *Store) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	st := State{Settings: s.cur}
	if s.cur.Enabled && s.cur.DurationSeconds > 0 {
		deadline := s.enabledAt.Add(time.Duration(s.cur.DurationSeconds) * time.Second)
		remaining := int(deadline.Sub(s.now()).Seconds())
		if remaining < 0 {
			remaining = 0
		}
		st.RemainingSeconds = &remaining
	}
	return st
}

// Update replaces the settings (clamped) and returns the resulting state. The
// auto-off timer restarts whenever generation transitions from off to on, or
// is re-applied while already on (so saving while running extends the window).
func (s *Store) Update(next Settings) State {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	s.cur = next.clamp()
	if s.cur.Enabled {
		// Restart the countdown on any save that leaves generation enabled, so
		// the run always gets a fresh duration window (covers off->on and
		// adjusting knobs mid-run).
		s.enabledAt = s.now()
	}
	st := State{Settings: s.cur}
	if s.cur.Enabled && s.cur.DurationSeconds > 0 {
		remaining := s.cur.DurationSeconds
		st.RemainingSeconds = &remaining
	}
	return st
}
