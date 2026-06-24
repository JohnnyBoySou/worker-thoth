// Package window models a daily time-of-day window, in a configured timezone,
// during which the worker is allowed to process jobs (i.e. download audio and
// call Whisper). Outside the window the worker stays idle and jobs accumulate in
// the queue. A zero/disabled window is always open, preserving the default
// "process anytime" behavior.
package window

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Window is an immutable daily processing window. The zero value (Enabled ==
// false) is always open.
type Window struct {
	enabled  bool
	startMin int // minutes since local midnight, inclusive
	endMin   int // minutes since local midnight, exclusive
	loc      *time.Location
}

// Parse builds a Window from "HH:MM" start/end strings and an IANA timezone
// (e.g. "America/Sao_Paulo"). When both start and end are empty the window is
// disabled (always open) and tz is ignored. It is an error to set only one
// bound, to use a malformed time, to give equal start and end, or to name an
// unknown timezone. A window whose start is after its end wraps past midnight
// (e.g. 22:00–05:00).
func Parse(start, end, tz string) (Window, error) {
	start, end = strings.TrimSpace(start), strings.TrimSpace(end)
	if start == "" && end == "" {
		return Window{enabled: false}, nil
	}
	if start == "" || end == "" {
		return Window{}, fmt.Errorf("window needs both start and end (got start=%q end=%q)", start, end)
	}

	startMin, err := parseHHMM(start)
	if err != nil {
		return Window{}, fmt.Errorf("invalid window start: %w", err)
	}
	endMin, err := parseHHMM(end)
	if err != nil {
		return Window{}, fmt.Errorf("invalid window end: %w", err)
	}
	if startMin == endMin {
		return Window{}, fmt.Errorf("window start and end are equal (%s); leave both empty to disable", start)
	}

	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Window{}, fmt.Errorf("invalid window timezone %q: %w", tz, err)
	}

	return Window{enabled: true, startMin: startMin, endMin: endMin, loc: loc}, nil
}

// parseHHMM parses a 24h "HH:MM" string into minutes since midnight.
func parseHHMM(s string) (int, error) {
	h, m, ok := strings.Cut(s, ":")
	if !ok {
		return 0, fmt.Errorf("expected HH:MM, got %q", s)
	}
	hour, err := strconv.Atoi(h)
	if err != nil || hour < 0 || hour > 23 {
		return 0, fmt.Errorf("invalid hour in %q", s)
	}
	min, err := strconv.Atoi(m)
	if err != nil || min < 0 || min > 59 {
		return 0, fmt.Errorf("invalid minute in %q", s)
	}
	return hour*60 + min, nil
}

// Enabled reports whether a window restriction is configured.
func (w Window) Enabled() bool { return w.enabled }

// IsOpen reports whether processing is allowed at time t. A disabled window is
// always open.
func (w Window) IsOpen(t time.Time) bool {
	if !w.enabled {
		return true
	}
	m := minuteOfDay(t.In(w.loc))
	if w.startMin < w.endMin {
		return m >= w.startMin && m < w.endMin
	}
	return m >= w.startMin || m < w.endMin // wraps past midnight
}

// NextOpen returns the duration until the window next opens, or 0 if it is
// already open (or disabled).
func (w Window) NextOpen(t time.Time) time.Duration {
	if w.IsOpen(t) {
		return 0
	}
	lt := t.In(w.loc)
	m := minuteOfDay(lt)

	var deltaMin int
	if m < w.startMin {
		deltaMin = w.startMin - m
	} else {
		deltaMin = (24*60 - m) + w.startMin
	}

	// deltaMin counts whole minutes from the start of the current minute; trim
	// the seconds already elapsed so the result lands exactly on the open edge.
	d := time.Duration(deltaMin)*time.Minute -
		(time.Duration(lt.Second())*time.Second + time.Duration(lt.Nanosecond())*time.Nanosecond)
	if d < 0 {
		return 0
	}
	return d
}

func minuteOfDay(t time.Time) int { return t.Hour()*60 + t.Minute() }
