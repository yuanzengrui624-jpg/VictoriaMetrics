package storage

import (
	"reflect"
	"testing"
	"time"
)

func TestSetDownsamplingPeriodsFailure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		if err := SetDownsamplingPeriods(s); err == nil {
			t.Fatalf("expecting non-nil error for SetDownsamplingPeriods(%q)", s)
		}
		downsamplingPeriods = nil
	}
	f("foobar")
	f("30d")
	f("30d:")
	f(":5m")
	f("30d:5m:1h")
	f("-30d:5m")
	f("30d:100ms")
	// interval for bigger offset must be bigger
	f("30d:5m,180d:5m")
	f("30d:1h,180d:5m")
	// interval for bigger offset must be divisible by the interval for smaller offset
	f("30d:5m,180d:7m")
	// duplicate offsets
	f("30d:5m,30d:10m")
}

func TestSetDownsamplingPeriodsSuccess(t *testing.T) {
	f := func(s string, dspsExpected []downsamplingPeriod) {
		t.Helper()
		if err := SetDownsamplingPeriods(s); err != nil {
			t.Fatalf("unexpected error for SetDownsamplingPeriods(%q): %s", s, err)
		}
		if !reflect.DeepEqual(downsamplingPeriods, dspsExpected) {
			t.Fatalf("unexpected downsamplingPeriods for %q\ngot\n%v\nwant\n%v", s, downsamplingPeriods, dspsExpected)
		}
		downsamplingPeriods = nil
	}
	f("", nil)
	f("30d:5m", []downsamplingPeriod{
		{offsetMsecs: 30 * 24 * 3600 * 1000, intervalMsecs: 5 * 60 * 1000},
	})
	// tiers must be sorted by offset in descending order independently of the input order
	f("30d:5m,180d:1h", []downsamplingPeriod{
		{offsetMsecs: 180 * 24 * 3600 * 1000, intervalMsecs: 3600 * 1000},
		{offsetMsecs: 30 * 24 * 3600 * 1000, intervalMsecs: 5 * 60 * 1000},
	})
	f("180d:1h,30d:5m", []downsamplingPeriod{
		{offsetMsecs: 180 * 24 * 3600 * 1000, intervalMsecs: 3600 * 1000},
		{offsetMsecs: 30 * 24 * 3600 * 1000, intervalMsecs: 5 * 60 * 1000},
	})
}

func TestDedupIntervalForTimestamp(t *testing.T) {
	if err := SetDownsamplingPeriods("30d:5m,180d:1h"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() {
		downsamplingPeriods = nil
	}()
	SetDedupInterval(30 * time.Second)
	defer SetDedupInterval(0)

	now := int64(1e15)
	msecsPerDay := int64(24 * 3600 * 1000)
	f := func(ageDays int64, intervalExpected int64) {
		t.Helper()
		interval := dedupIntervalForTimestamp(now-ageDays*msecsPerDay, now)
		if interval != intervalExpected {
			t.Fatalf("unexpected interval for age of %d days; got %d; want %d", ageDays, interval, intervalExpected)
		}
	}
	f(0, 30*1000)
	f(29, 30*1000)
	f(30, 5*60*1000)
	f(179, 5*60*1000)
	f(180, 3600*1000)
	f(10000, 3600*1000)

	// The global dedup interval must be used if it is bigger than the tier interval.
	SetDedupInterval(10 * time.Minute)
	f(30, 10*60*1000)
	f(180, 3600*1000)
}

func TestDeduplicateSamplesWithDownsampling(t *testing.T) {
	if err := SetDownsamplingPeriods("30d:5m"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() {
		downsamplingPeriods = nil
	}()

	now := time.Now().UnixMilli()
	boundary := now - 30*24*3600*1000

	// Old samples every 1m for 20m before an hour-aligned point well beyond the tier boundary;
	// recent samples every 1m for 20m around now.
	oldStart := (boundary - 365*24*3600*1000) / 3600000 * 3600000
	var timestamps []int64
	var values []float64
	for i := int64(0); i < 20; i++ {
		timestamps = append(timestamps, oldStart+i*60*1000)
		values = append(values, float64(i))
	}
	recentStart := (now - 20*60*1000) / 3600000 * 3600000
	for i := int64(0); i < 20; i++ {
		timestamps = append(timestamps, recentStart+i*60*1000)
		values = append(values, float64(100+i))
	}

	// The reference result is DeduplicateSamples applied per segment:
	// the old segment with the 5m tier interval, the recent segment with the base interval (0 - no dedup).
	expectedTimestamps, expectedValues := DeduplicateSamples(append([]int64{}, timestamps[:20]...), append([]float64{}, values[:20]...), 5*60*1000)
	if len(expectedTimestamps) >= 20 {
		t.Fatalf("BUG in test: the old segment wasn't thinned; got %d samples", len(expectedTimestamps))
	}
	expectedTimestamps = append(expectedTimestamps, timestamps[20:]...)
	expectedValues = append(expectedValues, values[20:]...)

	tsResult, vResult := DeduplicateSamplesWithDownsampling(timestamps, values, 0)
	if !reflect.DeepEqual(tsResult, expectedTimestamps) {
		t.Fatalf("unexpected timestamps\ngot\n%v\nwant\n%v\nboundary: %d", tsResult, expectedTimestamps, boundary)
	}
	if !reflect.DeepEqual(vResult, expectedValues) {
		t.Fatalf("unexpected values\ngot\n%v\nwant\n%v", vResult, expectedValues)
	}

	// Without configured downsampling tiers the function must be equivalent to DeduplicateSamples.
	downsamplingPeriods = nil
	timestampsCopy := append([]int64{}, timestamps...)
	valuesCopy := append([]float64{}, values...)
	tsResult, _ = DeduplicateSamplesWithDownsampling(timestampsCopy, valuesCopy, 0)
	if len(tsResult) != len(timestamps) {
		t.Fatalf("unexpected number of samples without downsampling; got %d; want %d", len(tsResult), len(timestamps))
	}
}
