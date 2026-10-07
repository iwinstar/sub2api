package service

import (
	"container/list"
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// Counters belong to one process and one pool. The first sample (including after
// a restart or counter reset) establishes a baseline rather than reporting a spike.
type billingDBWaitSampler struct {
	mu        sync.Mutex
	previous  sql.DBStats
	sampledAt time.Time
}

func (s *billingDBWaitSampler) sample(stats sql.DBStats, now time.Time) (int64, time.Duration, time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, sampledAt := s.previous, s.sampledAt
	s.previous, s.sampledAt = stats, now
	if sampledAt.IsZero() || !now.After(sampledAt) || stats.WaitCount < previous.WaitCount || stats.WaitDuration < previous.WaitDuration {
		return 0, 0, 0, false
	}
	return stats.WaitCount - previous.WaitCount, stats.WaitDuration - previous.WaitDuration, now.Sub(sampledAt), true
}

func (c *OpsMetricsCollector) logDBPoolWait() {
	stats := c.db.Stats()
	count, duration, interval, ok := c.billingDBWait.sample(stats, time.Now())
	if !ok {
		return
	}
	logger.L().Info("db.pool_wait",
		zap.String("component", "service.ops_metrics_collector"),
		zap.String("instance_id", c.instanceID),
		zap.Float64("sample_interval_seconds", interval.Seconds()),
		zap.Int64("wait_count_delta", count),
		zap.Float64("wait_duration_ms_delta", float64(duration)/float64(time.Millisecond)),
		zap.Int("in_use", stats.InUse), zap.Int("idle", stats.Idle),
		zap.Int("max_open", stats.MaxOpenConnections))
}

// Track only tasks that have not started. O(1) insertion/removal and oldest
// lookup keep observation independent of the queue size and execution duration.
type usageRecordPendingTasks struct {
	mu    sync.Mutex
	tasks list.List
}

func (p *UsageRecordWorkerPool) trySubmitTracked(task UsageRecordTask) bool {
	if !p.billingWriteObservabilityEnabled {
		_, ok := p.pool.TrySubmit(func() { p.execute(task) })
		return ok
	}
	p.pending.mu.Lock()
	entry := p.pending.tasks.PushBack(time.Now())
	p.pending.mu.Unlock()
	_, ok := p.pool.TrySubmit(func() {
		p.pending.remove(entry)
		p.execute(task)
	})
	if !ok {
		p.pending.remove(entry)
	}
	return ok
}

func (p *usageRecordPendingTasks) remove(entry *list.Element) {
	p.mu.Lock()
	p.tasks.Remove(entry)
	p.mu.Unlock()
}

func (p *usageRecordPendingTasks) oldestAge() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if first := p.tasks.Front(); first != nil {
		if enqueuedAt, ok := first.Value.(time.Time); ok {
			return time.Since(enqueuedAt)
		}
	}
	return 0
}

// Independent of auto-scaling: fixed worker pools need overflow visibility too.
func (p *UsageRecordWorkerPool) startStatsLogger() {
	ctx, cancel := context.WithCancel(context.Background())
	p.statsCancel = cancel
	p.lifecycleWg.Add(1)
	go func() {
		defer p.lifecycleWg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := p.Stats()
				logger.L().Info("usage_record.pool_stats",
					zap.String("component", "service.usage_record_worker_pool"),
					zap.Int64("running_workers", stats.RunningWorkers),
					zap.Uint64("waiting_tasks", stats.WaitingTasks),
					zap.Float64("oldest_pending_age_ms", float64(stats.OldestPendingAge)/float64(time.Millisecond)),
					zap.Uint64("sync_fallback_tasks_total", stats.SyncFallbackTasks),
					zap.Uint64("dropped_queue_full_total", stats.DroppedQueueFull),
					zap.Uint64("dropped_pool_stopped_total", stats.DroppedPoolStopped))
			}
		}
	}()
}
