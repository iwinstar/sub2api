package service

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"
)

// BillingDedupCursor is process-local scan progress, not a durable checkpoint.
// Losing it only repeats a scan. A repository advances it only after commit.
type BillingDedupCursor struct {
	Started                 bool
	PrefixRange             bool
	PageSize                int
	ConsecutiveFailures     int
	ConsecutiveFastPages    int
	ID, EndID               int64
	RequestID, EndRequestID string
	APIKeyID, EndAPIKeyID   int64
}

type BillingDedupPageStats struct {
	Scanned, Deleted, Retained, Anomalies int64
	Done                                  bool
}

type BillingDedupRetentionRepository interface {
	// A zero cutoff disables deletion for that category.
	DeleteBillingDedupPage(context.Context, *BillingDedupCursor, time.Time, time.Time, bool, bool) (BillingDedupPageStats, error)
}

// A separate recurring job keeps deletion independent of aggregation failures,
// usage-log retention, backfills, and the six-hour archival schedule.
func (s *DashboardAggregationService) runScheduledBillingDedupDeletion() {
	if !atomic.CompareAndSwapInt32(&s.dedupDeletionRunning, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&s.dedupDeletionRunning, 0)
	repo, ok := s.repo.(BillingDedupRetentionRepository)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Use the existing leader-lock infrastructure with a separate job key;
	// a long aggregation must not starve deletion on every timer tick.
	release, ok := tryAcquireSingletonLeaderLock(ctx, s.lockCache, s.db, "dashboard:billing-dedup-deletion:leader", s.instanceID, time.Minute)
	if !ok {
		return
	}
	defer release()
	now := time.Now().UTC()
	for i := range s.dedupDeletionCursors {
		image := i >= 2
		archive := i%2 == 1
		if !s.billingDedupStepEnabled(i) {
			continue
		}
		schedule := &s.dedupDeletionSchedules[i]
		if schedule.next.After(now) {
			continue
		}
		var cutoff, videoCutoff time.Time
		if image {
			cutoff = now.AddDate(0, 0, -s.cfg.Retention.UsageBillingDedupBatchImageDeleteDays)
		} else if s.cfg.Retention.UsageBillingDedupOrdinaryAutoDelete {
			cutoff = now.Add(-time.Hour)
		}
		if days := s.cfg.Retention.UsageBillingDedupVideoDeleteDays; days > 0 {
			videoCutoff = now.AddDate(0, 0, -days)
		}
		// Give every table/category its own budget so one backlog cannot starve
		// the others. Errors in one step do not suppress the remaining steps.
		stepCtx, stepCancel := context.WithTimeout(ctx, 5*time.Second)
		start := time.Now()
		var total BillingDedupPageStats
		failed := false
		for stepCtx.Err() == nil {
			cursor := &s.dedupDeletionCursors[i]
			pageStart := time.Now()
			stats, err := repo.DeleteBillingDedupPage(stepCtx, &s.dedupDeletionCursors[i], cutoff, videoCutoff, archive, image)
			if err != nil {
				failed = true
				cursor.ConsecutiveFailures++
				cursor.reducePageSize(image)
				slog.Warn("billing dedup deletion page failed; retrying smaller page next cycle", "archive", archive, "batch_image", image,
					"failures", cursor.ConsecutiveFailures, "next_page_size", cursor.PageSize, "error", err)
				if cursor.ConsecutiveFailures >= 3 {
					slog.Error("billing dedup deletion repeatedly unable to commit page", "archive", archive, "batch_image", image,
						"failures", cursor.ConsecutiveFailures, "cursor_id", cursor.ID, "cursor_request_id", cursor.RequestID, "cursor_api_key_id", cursor.APIKeyID)
				}
				break
			}
			cursor.ConsecutiveFailures = 0
			cursor.adjustPageSizeAfterSuccess(image, time.Since(pageStart), stats.Scanned)
			total.Scanned += stats.Scanned
			total.Deleted += stats.Deleted
			total.Retained += stats.Retained
			total.Anomalies += stats.Anomalies
			total.Done = stats.Done
			if stats.Done {
				break
			}
		}
		budgetExhausted := stepCtx.Err() != nil
		schedule.update(now, time.Now(), total.Deleted, total.Done, failed)
		stepCancel()
		slog.Info("billing dedup deletion progress", "archive", archive, "batch_image", image,
			"scanned", total.Scanned, "deleted", total.Deleted, "retained", total.Retained,
			"next_interval", schedule.interval, "round_complete", total.Done, "budget_exhausted", budgetExhausted, "duration", time.Since(start))
		if total.Anomalies > 0 {
			slog.Warn("billing dedup credentials retained: missing job or unsettled terminal job", "archive", archive, "count", total.Anomalies)
		}
	}
}

// Only tune batch size, never scan position. At one row, persistent lock or
// storage failures still need operator attention and are reported each cycle.
func (c *BillingDedupCursor) reducePageSize(image bool) {
	c.ConsecutiveFastPages = 0
	maximum := 10000
	if image {
		maximum = 1000
	}
	if c.PageSize <= 0 || c.PageSize > maximum {
		c.PageSize = maximum
	}
	c.PageSize = max(1, c.PageSize/2)
}

// Recover gradually after a backlog: five consecutive full, fast pages permit
// one doubling. Empty/tail pages are not evidence that a larger batch is cheap.
func (c *BillingDedupCursor) adjustPageSizeAfterSuccess(image bool, elapsed time.Duration, scanned int64) {
	if elapsed > time.Second {
		c.reducePageSize(image)
		return
	}
	maximum := 10000
	if image {
		maximum = 1000
	}
	if c.PageSize <= 0 || c.PageSize > maximum {
		c.PageSize = maximum
	}
	fullPage := c.PageSize
	// Image pages can stop after 100 candidate batches before the scan limit.
	if image {
		fullPage = min(fullPage, 100)
	}
	if elapsed >= 200*time.Millisecond || scanned < int64(fullPage) || c.PageSize == maximum {
		c.ConsecutiveFastPages = 0
		return
	}
	c.ConsecutiveFastPages++
	if c.ConsecutiveFastPages >= 5 {
		c.PageSize = min(maximum, c.PageSize*2)
		c.ConsecutiveFastPages = 0
	}
}

const (
	billingDedupInitialInterval = 600 * time.Second
	billingDedupMinInterval     = 60 * time.Second
	// Keep the maximum below the timing wheel's one-hour delay limit.
	billingDedupMaxInterval = 1800 * time.Second
	billingDedupTargetRows  = 50000
)

// Each table/category keeps independent timing. Incomplete scans are not rate
// samples: their row count measures processing capacity, not arrival rate.
type billingDedupSchedule struct {
	last, next time.Time
	interval   time.Duration
	rate       float64
	catchingUp bool
}

func (s *billingDedupSchedule) update(start, end time.Time, deleted int64, done, failed bool) {
	interval := s.interval
	if interval == 0 {
		interval = billingDedupInitialInterval
	}
	seconds := interval.Seconds()
	switch {
	case failed:
		seconds *= 1.5
		s.catchingUp = true
	case !done:
		seconds = billingDedupMinInterval.Seconds()
		s.catchingUp = true
	case s.catchingUp:
		// Take a fresh, complete round before estimating steady-state traffic.
		seconds = billingDedupMinInterval.Seconds()
		s.catchingUp = false
		s.rate = 0
	case deleted == 0:
		seconds *= 1.5
		s.rate = 0
	default:
		elapsed := start.Sub(s.last).Seconds()
		if s.last.IsZero() || elapsed <= 0 {
			elapsed = interval.Seconds()
		}
		rate := float64(deleted) / elapsed
		if s.rate == 0 {
			s.rate = rate
		} else {
			s.rate = s.rate*0.7 + rate*0.3
		}
		seconds = billingDedupTargetRows / s.rate
	}
	seconds = math.Max(billingDedupMinInterval.Seconds(), math.Min(billingDedupMaxInterval.Seconds(), math.Round(seconds)))
	s.interval = time.Duration(seconds) * time.Second
	s.last = start
	s.next = end.Add(s.interval)
}

func (s *DashboardAggregationService) billingDedupStepEnabled(i int) bool {
	if i >= 2 {
		return s.cfg.Retention.UsageBillingDedupBatchImageDeleteDays > 0
	}
	return s.cfg.Retention.UsageBillingDedupOrdinaryAutoDelete || s.cfg.Retention.UsageBillingDedupVideoDeleteDays > 0
}

func (s *DashboardAggregationService) scheduleBillingDedupDeletion(delay time.Duration) {
	s.timingWheel.Schedule("dashboard:billing-dedup-deletion", delay, func() {
		nextDelay := billingDedupMaxInterval
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("billing dedup deletion scheduler panicked; retrying", "panic", recovered)
				nextDelay = billingDedupMinInterval
			}
			// Keep the chain alive even when a deletion callback panics.
			s.scheduleBillingDedupDeletion(nextDelay)
		}()

		s.runScheduledBillingDedupDeletion()
		now := time.Now()
		for i, schedule := range s.dedupDeletionSchedules {
			if !s.billingDedupStepEnabled(i) {
				continue
			}
			remaining := schedule.next.Sub(now)
			// A lost leader election should retry, without resetting adaptive state.
			if schedule.next.IsZero() {
				remaining = billingDedupInitialInterval
			}
			if remaining <= 0 {
				remaining = billingDedupMinInterval
			}
			if remaining < time.Second {
				remaining = time.Second
			}
			if remaining < nextDelay {
				nextDelay = remaining
			}
		}
	})
}
