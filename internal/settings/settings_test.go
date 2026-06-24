package settings

import (
	"testing"
	"time"
)

// fakeClock lets a test advance time deterministically for the auto-off logic.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newStoreWithClock(initial Settings, c *fakeClock) *Store {
	s := New(initial)
	s.now = c.now
	return s
}

func TestUpdateClamps(t *testing.T) {
	s := New(Defaults())
	got := s.Update(Settings{
		Enabled:         true,
		Density:         MaxDensity * 10,
		WorkMillis:      -5,
		Depth:           99,
		Fanout:          0,
		DurationSeconds: MaxDurationSecs + 1,
	})
	if got.Density != MaxDensity {
		t.Errorf("density = %v, want clamped to %v", got.Density, MaxDensity)
	}
	if got.WorkMillis != 0 {
		t.Errorf("workMillis = %v, want clamped to 0", got.WorkMillis)
	}
	if got.Depth != MaxDepth {
		t.Errorf("depth = %v, want clamped to %v", got.Depth, MaxDepth)
	}
	if got.Fanout != 1 {
		t.Errorf("fanout = %v, want clamped to 1", got.Fanout)
	}
	if got.DurationSeconds != MaxDurationSecs {
		t.Errorf("durationSeconds = %v, want clamped to %v", got.DurationSeconds, MaxDurationSecs)
	}
}

func TestAutoOffExpires(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStoreWithClock(Defaults(), clk)

	s.Update(Settings{Enabled: true, Density: 1, Depth: 2, Fanout: 1, DurationSeconds: 600})
	if !s.Get().Enabled {
		t.Fatal("expected enabled right after turning on")
	}

	clk.advance(599 * time.Second)
	if !s.Get().Enabled {
		t.Fatal("expected still enabled before the duration elapses")
	}

	clk.advance(2 * time.Second) // now past the 600s window
	if s.Get().Enabled {
		t.Fatal("expected auto-off after the duration elapses")
	}
}

func TestZeroDurationRunsForever(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStoreWithClock(Defaults(), clk)

	s.Update(Settings{Enabled: true, Density: 1, Depth: 2, Fanout: 1, DurationSeconds: 0})
	clk.advance(365 * 24 * time.Hour)
	if !s.Get().Enabled {
		t.Fatal("duration 0 should never auto-off")
	}
}

func TestStateReportsRemaining(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := newStoreWithClock(Defaults(), clk)

	s.Update(Settings{Enabled: true, Density: 1, Depth: 2, Fanout: 1, DurationSeconds: 600})
	clk.advance(100 * time.Second)
	st := s.State()
	if st.RemainingSeconds == nil {
		t.Fatal("expected a remaining countdown while enabled")
	}
	if *st.RemainingSeconds != 500 {
		t.Errorf("remaining = %d, want 500", *st.RemainingSeconds)
	}
}
