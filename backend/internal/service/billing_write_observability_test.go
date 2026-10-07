//go:build unit

package service

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestBillingDBWaitSampler(t *testing.T) {
	var s billingDBWaitSampler
	now := time.Now()
	_, _, _, ok := s.sample(sql.DBStats{WaitCount: 100, WaitDuration: time.Second}, now)
	require.False(t, ok)
	count, duration, interval, ok := s.sample(sql.DBStats{WaitCount: 103, WaitDuration: 2 * time.Second}, now.Add(75*time.Second))
	require.True(t, ok)
	require.EqualValues(t, 3, count)
	require.Equal(t, time.Second, duration)
	require.Equal(t, 75*time.Second, interval)
	_, _, _, ok = s.sample(sql.DBStats{}, now.Add(90*time.Second))
	require.False(t, ok)
	count, duration, _, ok = s.sample(sql.DBStats{WaitCount: 1, WaitDuration: time.Millisecond}, now.Add(150*time.Second))
	require.True(t, ok)
	require.EqualValues(t, 1, count)
	require.Equal(t, time.Millisecond, duration)
}

func TestUsageRecordPendingAge(t *testing.T) {
	pool := NewUsageRecordWorkerPoolWithOptions(UsageRecordWorkerPoolOptions{BillingWriteObservabilityEnabled: true, WorkerCount: 1, QueueSize: 1, OverflowPolicy: "drop"})
	started, release, ran := make(chan struct{}), make(chan struct{}), make(chan struct{})
	// Unblock before Stop even if an assertion fails.
	defer pool.Stop()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	require.Equal(t, UsageRecordSubmitModeEnqueued, pool.Submit(func(context.Context) { close(started); <-release }))
	<-started
	require.Zero(t, pool.Stats().OldestPendingAge) // Running tasks do not count.
	require.Equal(t, UsageRecordSubmitModeEnqueued, pool.Submit(func(context.Context) { close(ran) }))
	require.Positive(t, pool.Stats().OldestPendingAge)
	require.Equal(t, UsageRecordSubmitModeDropped, pool.Submit(func(context.Context) {}))
	pool.pending.mu.Lock()
	pendingCount := pool.pending.tasks.Len()
	pool.pending.mu.Unlock()
	require.Equal(t, 1, pendingCount) // Rejected submissions are removed.
	unblock()
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("queued task did not run")
	}
	require.Zero(t, pool.Stats().OldestPendingAge)
}

func TestBillingWriteObservabilityConfiguration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Billing.AutomaticBatchEnabled = enabled
		opts := usageRecordPoolOptionsFromConfig(cfg)
		require.Equal(t, enabled, opts.BillingWriteObservabilityEnabled)
		opts.WorkerCount, opts.QueueSize = 1, 1
		pool := NewUsageRecordWorkerPoolWithOptions(opts)
		require.Equal(t, enabled, pool.statsCancel != nil)
		pool.Stop()
	}
}
