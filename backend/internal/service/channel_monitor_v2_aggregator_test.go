//go:build unit

package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelMonitorV2MaxChunkForDepth(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

	// Within last day → tightest ceiling (2h).
	require.Equal(t, channelMonitorV2MaxChunkNear1d, channelMonitorV2MaxChunkForDepth(now, now.Add(-2*time.Hour)))
	// Between 1d and 7d → 4h.
	require.Equal(t, channelMonitorV2MaxChunkNear7d, channelMonitorV2MaxChunkForDepth(now, now.Add(-2*24*time.Hour)))
	// Older than 7d → 6h (never 24h default).
	require.Equal(t, channelMonitorV2MaxChunkFar, channelMonitorV2MaxChunkForDepth(now, now.Add(-10*24*time.Hour)))
	require.Less(t, channelMonitorV2MaxChunkFar, 24*time.Hour)
	require.Equal(t, time.Hour, channelMonitorV2BackfillChunkInit)
	require.Equal(t, 15*time.Minute, channelMonitorV2MinBackfillChunk)
}

func TestChannelMonitorV2RetentionDuration(t *testing.T) {
	require.Equal(t, 48*time.Hour, channelMonitorV2RetentionDuration("d"))
	require.Equal(t, 8*24*time.Hour, channelMonitorV2RetentionDuration("w"))
	require.Equal(t, 31*24*time.Hour, channelMonitorV2RetentionDuration("m"))
	require.Equal(t, 48*time.Hour, channelMonitorV2RetentionDuration("24h"))
	require.Equal(t, 8*24*time.Hour, channelMonitorV2RetentionDuration("7d"))
	require.Equal(t, 31*24*time.Hour, channelMonitorV2RetentionDuration("30d"))
	require.Equal(t, 31*24*time.Hour, channelMonitorV2RetentionDuration(""))
}

func TestChannelMonitorV2AggregatorAdaptiveChunk(t *testing.T) {
	s := NewChannelMonitorV2Aggregator(nil, nil, nil)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	cursor := now.Add(-3 * time.Hour)

	// Failure shrinks chunk and sets backoff floor.
	s.backfillChunk = 2 * time.Hour
	s.recordBackfillFailure(now, cursor)
	require.Equal(t, time.Hour, s.backfillChunk)
	require.Equal(t, time.Minute, s.nextWaitFloor)
	require.Equal(t, 1, s.backfillFailures)

	// Repeated failure halves again and raises floor.
	s.recordBackfillFailure(now, cursor)
	require.Equal(t, 30*time.Minute, s.backfillChunk)
	require.Equal(t, 2*time.Minute, s.nextWaitFloor)

	// Fast success grows within depth ceiling and clears backoff.
	s.recordBackfillSuccess(cursor.Add(-30*time.Minute), 5*time.Second, now)
	require.Equal(t, 0, s.backfillFailures)
	require.Equal(t, time.Duration(0), s.nextWaitFloor)
	require.Greater(t, s.backfillChunk, 30*time.Minute)
	require.LessOrEqual(t, s.backfillChunk, channelMonitorV2MaxChunkForDepth(now, cursor.Add(-30*time.Minute)))
}

func TestChannelMonitorV2BackfillStart(t *testing.T) {
	start := time.Date(2026, 10, 3, 15, 4, 0, 0, time.UTC)
	require.Equal(t, time.Date(2026, 10, 3, 15, 0, 0, 0, time.UTC), channelMonitorV2BackfillStart(start))
	require.Equal(t, start.Truncate(time.Hour).Add(time.Hour), channelMonitorV2BackfillEnd(start))
	require.Equal(t, start.Truncate(time.Hour), channelMonitorV2BackfillEnd(start.Truncate(time.Hour)))
	for _, chunk := range []time.Duration{15 * time.Minute, 90 * time.Minute, 135 * time.Minute} {
		next := channelMonitorV2BackfillStart(channelMonitorV2BackfillStart(start).Add(-chunk))
		require.Equal(t, 0, next.Minute())
		require.True(t, next.Before(start))
	}
}
