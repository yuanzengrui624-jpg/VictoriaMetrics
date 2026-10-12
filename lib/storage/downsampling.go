package storage

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

type downsamplingPeriod struct {
	offsetMsecs   int64
	intervalMsecs int64
}

// sorted by offsetMsecs desc; set before storage init, read-only afterwards
var downsamplingPeriods []downsamplingPeriod

// SetDownsamplingPeriods sets downsampling tiers from a comma-separated list of `offset:interval` pairs, e.g. "30d:5m,180d:1h".
// Samples older than `offset` are deduplicated with `interval` the same way as -dedup.minScrapeInterval does.
func SetDownsamplingPeriods(s string) error {
	downsamplingPeriods = nil
	if s = strings.TrimSpace(s); s == "" {
		return nil
	}
	var dsps []downsamplingPeriod
	for _, x := range strings.Split(s, ",") {
		x = strings.TrimSpace(x)
		parts := strings.Split(x, ":")
		if len(parts) != 2 {
			return fmt.Errorf("cannot parse downsampling period %q; must have `offset:interval` format", x)
		}
		offset, err := timeutil.ParseDuration(parts[0])
		if err != nil {
			return fmt.Errorf("cannot parse offset in downsampling period %q: %w", x, err)
		}
		interval, err := timeutil.ParseDuration(parts[1])
		if err != nil {
			return fmt.Errorf("cannot parse interval in downsampling period %q: %w", x, err)
		}
		if offset <= 0 || interval < time.Second {
			return fmt.Errorf("downsampling period %q: offset must be positive and interval cannot be smaller than 1s", x)
		}
		dsps = append(dsps, downsamplingPeriod{offsetMsecs: offset.Milliseconds(), intervalMsecs: interval.Milliseconds()})
	}
	sort.Slice(dsps, func(i, j int) bool { return dsps[i].offsetMsecs > dsps[j].offsetMsecs })
	for i := 1; i < len(dsps); i++ {
		prev, curr := dsps[i-1], dsps[i]
		if curr.offsetMsecs == prev.offsetMsecs {
			return fmt.Errorf("duplicate downsampling offset %s", time.Duration(curr.offsetMsecs)*time.Millisecond)
		}
		if prev.intervalMsecs <= curr.intervalMsecs || prev.intervalMsecs%curr.intervalMsecs != 0 {
			return fmt.Errorf("interval for offset %s must be bigger than and divisible by the interval for offset %s", time.Duration(prev.offsetMsecs)*time.Millisecond, time.Duration(curr.offsetMsecs)*time.Millisecond)
		}
	}
	downsamplingPeriods = dsps
	return nil
}

func isDownsamplingEnabled() bool {
	return len(downsamplingPeriods) > 0
}

// dedupIntervalForTimestamp returns the dedup interval for a sample with the given timestamp:
// the interval of the matching downsampling tier, but not smaller than the global dedup interval.
func dedupIntervalForTimestamp(ts, nowMsecs int64) int64 {
	d := globalDedupInterval
	for _, dsp := range downsamplingPeriods {
		if nowMsecs-ts >= dsp.offsetMsecs {
			if dsp.intervalMsecs > d {
				d = dsp.intervalMsecs
			}
			break
		}
	}
	return d
}

// DeduplicateSamplesWithDownsampling is the same as DeduplicateSamples, but it splits the samples
// at downsampling tier boundaries and deduplicates every segment with the interval of its tier.
func DeduplicateSamplesWithDownsampling(srcTimestamps []int64, srcValues []float64, dedupInterval int64) ([]int64, []float64) {
	if len(downsamplingPeriods) == 0 {
		return DeduplicateSamples(srcTimestamps, srcValues, dedupInterval)
	}
	if len(srcTimestamps) == 0 {
		return srcTimestamps, srcValues
	}
	nowMsecs := time.Now().UnixMilli()
	dstTimestamps, dstValues := srcTimestamps[:0], srcValues[:0]
	start := 0
	// segments go from oldest to newest; DeduplicateSamples works in place and every segment's
	// output lands at or before its input, so the appends never overwrite unread source samples
	for _, dsp := range downsamplingPeriods {
		if start >= len(srcTimestamps) {
			break
		}
		boundary := nowMsecs - dsp.offsetMsecs
		end := start + sort.Search(len(srcTimestamps)-start, func(i int) bool { return srcTimestamps[start+i] > boundary })
		if end > start {
			interval := max(dsp.intervalMsecs, dedupInterval)
			timestamps, values := DeduplicateSamples(srcTimestamps[start:end], srcValues[start:end], interval)
			dstTimestamps = append(dstTimestamps, timestamps...)
			dstValues = append(dstValues, values...)
		}
		start = end
	}
	if start < len(srcTimestamps) {
		timestamps, values := DeduplicateSamples(srcTimestamps[start:], srcValues[start:], dedupInterval)
		dstTimestamps = append(dstTimestamps, timestamps...)
		dstValues = append(dstValues, values...)
	}
	return dstTimestamps, dstValues
}
