package storage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

// SetDedupInterval sets the deduplication interval, which is applied to raw samples during data ingestion and querying.
//
// De-duplication is disabled if dedupInterval is 0.
//
// This function must be called before initializing the storage.
func SetDedupInterval(dedupInterval time.Duration) {
	globalDedupInterval = dedupInterval.Milliseconds()
}

// GetDedupInterval returns the dedup interval in milliseconds, which has been set via SetDedupInterval.
func GetDedupInterval() int64 {
	return globalDedupInterval
}

var globalDedupInterval int64

var downsamplingMeta []DownSamplingMeta

// GetDownSamplingMeta returns downsampling rules set via SetDownsampling.
// The slice is ordered from the largest age to the smallest.
func GetDownSamplingMeta() []DownSamplingMeta {
	return downsamplingMeta
}

// SetDownsampling parses rules like -downsampling.period=30d:1m,180d:5m.
// Invalid rules are skipped. This must be called before initializing the storage.
func SetDownsampling(downsampling string) {
	downsamplingMeta = nil
	if downsampling == "" {
		return
	}
	for _, rule := range strings.Split(downsampling, ",") {
		chunks := strings.Split(rule, ":")
		if len(chunks) != 2 {
			continue
		}
		duration, err1 := convertDuration(chunks[0])
		interval, err2 := convertDuration(chunks[1])
		if err1 != nil || err2 != nil || interval <= time.Millisecond {
			continue
		}
		downsamplingMeta = append(downsamplingMeta, DownSamplingMeta{
			Duration:             duration,
			DownsamplingInterval: interval,
		})
	}
	if len(downsamplingMeta) > 1 {
		sort.Slice(downsamplingMeta, func(i, j int) bool {
			return downsamplingMeta[i].Duration > downsamplingMeta[j].Duration
		})
	}
}

func convertDuration(duration string) (time.Duration, error) {
	var hours int
	switch {
	case strings.HasSuffix(duration, "y"):
		value, err := strconv.Atoi(strings.TrimSuffix(duration, "y"))
		if err != nil {
			return 0, fmt.Errorf("invalid time range: %q", duration)
		}
		hours = value * 365 * 24
		return time.ParseDuration(fmt.Sprintf("%dh", hours))
	case strings.HasSuffix(duration, "w"):
		value, err := strconv.Atoi(strings.TrimSuffix(duration, "w"))
		if err != nil {
			return 0, fmt.Errorf("invalid time range: %q", duration)
		}
		hours = value * 7 * 24
		return time.ParseDuration(fmt.Sprintf("%dh", hours))
	case strings.HasSuffix(duration, "d"):
		value, err := strconv.Atoi(strings.TrimSuffix(duration, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid time range: %q", duration)
		}
		hours = value * 24
		return time.ParseDuration(fmt.Sprintf("%dh", hours))
	case strings.HasSuffix(duration, "h"), strings.HasSuffix(duration, "m"), strings.HasSuffix(duration, "s"), strings.HasSuffix(duration, "ms"):
		parsed, err := time.ParseDuration(duration)
		if err != nil {
			return 0, fmt.Errorf("invalid time range: %q", duration)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("invalid time duration string: %q", duration)
	}
}

// DownSamplingMeta is one age threshold and the dedup interval applied after that age.
type DownSamplingMeta struct {
	Duration             time.Duration
	DownsamplingInterval time.Duration
}

func isDedupEnabled() bool {
	return globalDedupInterval > 0
}

// DeduplicateSamples removes samples from src* if they are closer to each other than dedupInterval in milliseconds.
func DeduplicateSamples(srcTimestamps []int64, srcValues []float64, dedupInterval int64) ([]int64, []float64) {
	if !needsDedup(srcTimestamps, dedupInterval) {
		// Fast path - nothing to deduplicate
		return srcTimestamps, srcValues
	}
	tsNext := srcTimestamps[0] + dedupInterval - 1
	tsNext -= tsNext % dedupInterval
	dstTimestamps := srcTimestamps[:0]
	dstValues := srcValues[:0]
	for i, ts := range srcTimestamps[1:] {
		if ts <= tsNext {
			continue
		}
		// Choose the maximum value with the timestamp equal to tsPrev.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333
		j := i
		tsPrev := srcTimestamps[j]
		vPrev := srcValues[j]
		// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
		// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
		for j > 0 && srcTimestamps[j-1] == tsPrev {
			j--
			if decimal.IsStaleNaN(srcValues[j]) {
				continue
			}
			if decimal.IsStaleNaN(vPrev) {
				vPrev = srcValues[j]
				continue
			}
			if srcValues[j] > vPrev {
				vPrev = srcValues[j]
			}
		}
		dstTimestamps = append(dstTimestamps, tsPrev)
		dstValues = append(dstValues, vPrev)
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = ts + dedupInterval - 1
			tsNext -= tsNext % dedupInterval
		}
	}
	j := len(srcTimestamps) - 1
	tsPrev := srcTimestamps[j]
	vPrev := srcValues[j]
	// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
	// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
	for j > 0 && srcTimestamps[j-1] == tsPrev {
		j--
		if decimal.IsStaleNaN(srcValues[j]) {
			continue
		}
		if decimal.IsStaleNaN(vPrev) {
			vPrev = srcValues[j]
			continue
		}
		if srcValues[j] > vPrev {
			vPrev = srcValues[j]
		}
	}
	dstTimestamps = append(dstTimestamps, tsPrev)
	dstValues = append(dstValues, vPrev)
	return dstTimestamps, dstValues
}

func deduplicateSamplesDuringMerge(srcTimestamps, srcValues []int64, dedupInterval int64) ([]int64, []int64) {
	if !needsDedup(srcTimestamps, dedupInterval) {
		// Fast path - nothing to deduplicate
		return srcTimestamps, srcValues
	}
	tsNext := srcTimestamps[0] + dedupInterval - 1
	tsNext -= tsNext % dedupInterval
	dstTimestamps := srcTimestamps[:0]
	dstValues := srcValues[:0]
	for i, ts := range srcTimestamps[1:] {
		if ts <= tsNext {
			continue
		}
		// Choose the maximum value with the timestamp equal to tsPrev.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333
		j := i
		tsPrev := srcTimestamps[j]
		vPrev := srcValues[j]
		// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
		// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
		for j > 0 && srcTimestamps[j-1] == tsPrev {
			j--
			if decimal.IsStaleNaNInt64(srcValues[j]) {
				continue
			}
			if decimal.IsStaleNaNInt64(vPrev) {
				vPrev = srcValues[j]
				continue
			}
			if srcValues[j] > vPrev {
				vPrev = srcValues[j]
			}
		}
		dstTimestamps = append(dstTimestamps, tsPrev)
		dstValues = append(dstValues, vPrev)
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = ts + dedupInterval - 1
			tsNext -= tsNext % dedupInterval
		}
	}
	j := len(srcTimestamps) - 1
	tsPrev := srcTimestamps[j]
	vPrev := srcValues[j]
	// if multiple samples have the same timestamp, choose the maximum value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333;
	// always prefer a non-decimal.StaleNaN value, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10196
	for j > 0 && srcTimestamps[j-1] == tsPrev {
		j--
		if decimal.IsStaleNaNInt64(srcValues[j]) {
			continue
		}
		if decimal.IsStaleNaNInt64(vPrev) {
			vPrev = srcValues[j]
			continue
		}
		if srcValues[j] > vPrev {
			vPrev = srcValues[j]
		}
	}
	dstTimestamps = append(dstTimestamps, tsPrev)
	dstValues = append(dstValues, vPrev)
	return dstTimestamps, dstValues
}

func needsDedup(timestamps []int64, dedupInterval int64) bool {
	if len(timestamps) < 2 || dedupInterval <= 0 {
		return false
	}
	tsNext := timestamps[0] + dedupInterval - 1
	tsNext -= tsNext % dedupInterval
	for _, ts := range timestamps[1:] {
		if ts <= tsNext {
			return true
		}
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = ts + dedupInterval - 1
			tsNext -= tsNext % dedupInterval
		}
	}
	return false
}
