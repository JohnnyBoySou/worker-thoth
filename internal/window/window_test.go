package window

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, start, end, tz string) Window {
	t.Helper()
	w, err := Parse(start, end, tz)
	if err != nil {
		t.Fatalf("Parse(%q,%q,%q) error: %v", start, end, tz, err)
	}
	return w
}

// at builds a time in the given IANA location.
func at(t *testing.T, tz string, hour, min int) time.Time {
	t.Helper()
	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", tz, err)
	}
	return time.Date(2026, 6, 24, hour, min, 0, 0, loc)
}

func TestDisabledWindowAlwaysOpen(t *testing.T) {
	w := mustParse(t, "", "", "")
	if w.Enabled() {
		t.Fatal("empty bounds should be disabled")
	}
	if !w.IsOpen(time.Now()) {
		t.Error("disabled window must always be open")
	}
	if w.NextOpen(time.Now()) != 0 {
		t.Error("disabled window NextOpen must be 0")
	}
}

func TestSameDayWindow(t *testing.T) {
	w := mustParse(t, "09:00", "17:00", "America/Sao_Paulo")
	cases := []struct {
		h, m int
		open bool
	}{
		{8, 59, false},
		{9, 0, true},
		{12, 30, true},
		{16, 59, true},
		{17, 0, false}, // end is exclusive
		{23, 0, false},
	}
	for _, c := range cases {
		if got := w.IsOpen(at(t, "America/Sao_Paulo", c.h, c.m)); got != c.open {
			t.Errorf("IsOpen(%02d:%02d) = %v, want %v", c.h, c.m, got, c.open)
		}
	}
}

func TestWrapAroundMidnightWindow(t *testing.T) {
	// Overnight batch window: 22:00 → 05:00.
	w := mustParse(t, "22:00", "05:00", "America/Sao_Paulo")
	cases := []struct {
		h, m int
		open bool
	}{
		{21, 59, false},
		{22, 0, true},
		{23, 30, true},
		{0, 0, true},
		{4, 59, true},
		{5, 0, false}, // end exclusive
		{12, 0, false},
	}
	for _, c := range cases {
		if got := w.IsOpen(at(t, "America/Sao_Paulo", c.h, c.m)); got != c.open {
			t.Errorf("IsOpen(%02d:%02d) = %v, want %v", c.h, c.m, got, c.open)
		}
	}
}

func TestTimezoneIsApplied(t *testing.T) {
	// 00:00–06:00 in São Paulo (UTC-3). 04:00 UTC == 01:00 BRT → open.
	w := mustParse(t, "00:00", "06:00", "America/Sao_Paulo")
	utc4 := time.Date(2026, 6, 24, 4, 0, 0, 0, time.UTC)
	if !w.IsOpen(utc4) {
		t.Error("04:00 UTC is 01:00 BRT, should be inside 00:00-06:00 window")
	}
	// 12:00 UTC == 09:00 BRT → closed.
	utc12 := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	if w.IsOpen(utc12) {
		t.Error("12:00 UTC is 09:00 BRT, should be outside the window")
	}
}

func TestNextOpen(t *testing.T) {
	w := mustParse(t, "00:00", "06:00", "America/Sao_Paulo")

	// At 09:00 BRT, next open (00:00) is 15h away.
	d := w.NextOpen(at(t, "America/Sao_Paulo", 9, 0))
	if d != 15*time.Hour {
		t.Errorf("NextOpen at 09:00 = %v, want 15h", d)
	}

	// Already open → 0.
	if d := w.NextOpen(at(t, "America/Sao_Paulo", 2, 0)); d != 0 {
		t.Errorf("NextOpen while open = %v, want 0", d)
	}

	// Seconds within the current minute are trimmed: 23:59:30 → 30s to 00:00.
	now := time.Date(2026, 6, 24, 23, 59, 30, 0, mustLoad(t, "America/Sao_Paulo"))
	if d := w.NextOpen(now); d != 30*time.Second {
		t.Errorf("NextOpen at 23:59:30 = %v, want 30s", d)
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct{ start, end, tz string }{
		{"09:00", "", "UTC"},           // only one bound
		{"", "17:00", "UTC"},           // only one bound
		{"9am", "17:00", "UTC"},        // bad format
		{"09:60", "17:00", "UTC"},      // bad minute
		{"24:00", "17:00", "UTC"},      // bad hour
		{"09:00", "09:00", "UTC"},      // equal bounds
		{"09:00", "17:00", "Mars/Bug"}, // bad tz
	}
	for _, c := range cases {
		if _, err := Parse(c.start, c.end, c.tz); err == nil {
			t.Errorf("Parse(%q,%q,%q) expected error, got nil", c.start, c.end, c.tz)
		}
	}
}

func mustLoad(t *testing.T, tz string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", tz, err)
	}
	return loc
}
