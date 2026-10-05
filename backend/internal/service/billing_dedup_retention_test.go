package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type billingDedupRetentionStub struct {
	DashboardAggregationRepository
	steps           []int
	fail            bool
	ordinaryCutoffs []time.Time
}

func (r *billingDedupRetentionStub) DeleteBillingDedupPage(_ context.Context, _ *BillingDedupCursor, cutoff, _ time.Time, archive, image bool) (BillingDedupPageStats, error) {
	i := 0
	if archive {
		i++
	}
	if image {
		i += 2
	}
	r.steps = append(r.steps, i)
	r.ordinaryCutoffs = append(r.ordinaryCutoffs, cutoff)
	if r.fail && i == 0 {
		return BillingDedupPageStats{}, errors.New("test failure")
	}
	return BillingDedupPageStats{Done: true}, nil
}

func TestBillingDedupDeletionIndependentSteps(t *testing.T) {
	r := &billingDedupRetentionStub{fail: true}
	s := NewDashboardAggregationService(r, nil, &config.Config{DashboardAgg: config.DashboardAggregationConfig{Retention: config.DashboardAggregationRetentionConfig{UsageBillingDedupOrdinaryAutoDelete: true, UsageBillingDedupBatchImageDeleteDays: 7}}})
	// Aggregation and usage-log cleanup are disabled; an error in step 0 must
	// not suppress either archive cleanup or the batch-image steps.
	for i := range s.dedupDeletionSchedules {
		s.dedupDeletionSchedules[i].next = time.Time{}
	}
	s.runScheduledBillingDedupDeletion()
	require.Equal(t, []int{0, 1, 2, 3}, r.steps)
	require.Equal(t, 5000, s.dedupDeletionCursors[0].PageSize)
	require.Equal(t, 1, s.dedupDeletionCursors[0].ConsecutiveFailures)
	for i := range s.dedupDeletionSchedules {
		s.dedupDeletionSchedules[i].next = time.Time{}
	}
	s.runScheduledBillingDedupDeletion()
	require.Equal(t, 2500, s.dedupDeletionCursors[0].PageSize)
	require.Equal(t, 2, s.dedupDeletionCursors[0].ConsecutiveFailures)
	r.fail = false
	for i := range s.dedupDeletionSchedules {
		s.dedupDeletionSchedules[i].next = time.Time{}
	}
	s.runScheduledBillingDedupDeletion()
	require.Zero(t, s.dedupDeletionCursors[0].ConsecutiveFailures)
}

func TestBillingDedupPageSizeReductionPreservesPosition(t *testing.T) {
	for _, image := range []bool{false, true} {
		cursor := BillingDedupCursor{Started: true, ID: 123, EndID: 456, RequestID: "batch_image_hold:test", APIKeyID: 5}
		cursor.reducePageSize(image)
		if image {
			require.Equal(t, 500, cursor.PageSize)
		} else {
			require.Equal(t, 5000, cursor.PageSize)
		}
		for i := 0; i < 20; i++ {
			cursor.reducePageSize(image)
		}
		require.Equal(t, 1, cursor.PageSize)
		require.EqualValues(t, 123, cursor.ID)
		require.EqualValues(t, 456, cursor.EndID)
		require.Equal(t, "batch_image_hold:test", cursor.RequestID)
		require.EqualValues(t, 5, cursor.APIKeyID)
	}
}

func TestBillingDedupDeletionDisabled(t *testing.T) {
	r := &billingDedupRetentionStub{}
	s := NewDashboardAggregationService(r, nil, &config.Config{})
	for i := range s.dedupDeletionSchedules {
		s.dedupDeletionSchedules[i].next = time.Time{}
	}
	s.runScheduledBillingDedupDeletion()
	require.Empty(t, r.steps)
}

func TestBillingDedupVideoOnlySchedulesSharedScan(t *testing.T) {
	r := &billingDedupRetentionStub{}
	s := NewDashboardAggregationService(r, nil, &config.Config{DashboardAgg: config.DashboardAggregationConfig{Retention: config.DashboardAggregationRetentionConfig{UsageBillingDedupVideoDeleteDays: 7}}})
	for i := range s.dedupDeletionSchedules {
		s.dedupDeletionSchedules[i].next = time.Time{}
	}
	s.runScheduledBillingDedupDeletion()
	require.Equal(t, []int{0, 1}, r.steps)
}

func TestBillingDedupPageSizeRecovery(t *testing.T) {
	for _, image := range []bool{false, true} {
		c := BillingDedupCursor{PageSize: 64, ID: 123, EndID: 456}
		for i := 0; i < 4; i++ {
			c.adjustPageSizeAfterSuccess(image, 100*time.Millisecond, 64)
		}
		require.Equal(t, 64, c.PageSize)
		c.adjustPageSizeAfterSuccess(image, 100*time.Millisecond, 64)
		require.Equal(t, 128, c.PageSize)
		require.Zero(t, c.ConsecutiveFastPages)
		for i := 0; i < 100; i++ {
			c.adjustPageSizeAfterSuccess(image, 100*time.Millisecond, 10000)
		}
		maximum := 10000
		if image {
			maximum = 1000
		}
		require.Equal(t, maximum, c.PageSize)
		require.EqualValues(t, 123, c.ID)
		require.EqualValues(t, 456, c.EndID)
	}
}

func TestBillingDedupPageSizeRecoveryRequiresConsecutiveFullFastPages(t *testing.T) {
	for _, tc := range []struct {
		elapsed time.Duration
		scanned int64
	}{
		{200 * time.Millisecond, 64}, {100 * time.Millisecond, 0}, {100 * time.Millisecond, 63},
	} {
		c := BillingDedupCursor{PageSize: 64, ConsecutiveFastPages: 4}
		c.adjustPageSizeAfterSuccess(false, tc.elapsed, tc.scanned)
		require.Zero(t, c.ConsecutiveFastPages)
		require.Equal(t, 64, c.PageSize)
	}
	c := BillingDedupCursor{PageSize: 64, ConsecutiveFastPages: 4}
	c.adjustPageSizeAfterSuccess(false, 2*time.Second, 64)
	require.Equal(t, 32, c.PageSize)
	require.Zero(t, c.ConsecutiveFastPages)
	c.ConsecutiveFastPages = 4
	c.reducePageSize(false)
	require.Equal(t, 16, c.PageSize)
	require.Zero(t, c.ConsecutiveFastPages)
	c = BillingDedupCursor{PageSize: 512, ConsecutiveFastPages: 4}
	c.adjustPageSizeAfterSuccess(true, 100*time.Millisecond, 100)
	require.Equal(t, 1000, c.PageSize)
}

func TestBillingDedupAdaptiveSchedule(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tc := range []struct {
		name         string
		rows         int64
		done, failed bool
		seconds      int
	}{
		{"initial_empty", 0, true, false, 900},
		{"high_rate", 500000, true, false, 60},
		{"moderate_rate", 100000, true, false, 300},
		{"low_rate", 1, true, false, 1800},
		{"round_up", 199400, true, false, 150},
		{"round_up_fraction", 199000, true, false, 151},
		{"backlog", 1, false, false, 60},
		{"error_is_not_empty_or_backlog", 0, false, true, 900},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var schedule billingDedupSchedule
			end := now.Add(time.Second)
			schedule.update(now, end, tc.rows, tc.done, tc.failed)
			require.Equal(t, time.Duration(tc.seconds)*time.Second, schedule.interval)
			require.Equal(t, end.Add(schedule.interval), schedule.next)
		})
	}
}

func TestBillingDedupAdaptiveRecoveryAndSmoothing(t *testing.T) {
	now := time.Unix(1800000000, 0)
	var s billingDedupSchedule
	s.update(now, now, 50000, true, false)
	require.Equal(t, 600*time.Second, s.interval)
	now = now.Add(600 * time.Second)
	s.update(now, now, 100000, true, false)
	require.Equal(t, 462*time.Second, s.interval) // 0.7*83.33 + 0.3*166.67
	s.update(now, now, 1, false, false)
	require.Equal(t, time.Minute, s.interval)
	previousRate := s.rate
	s.update(now.Add(time.Minute), now.Add(time.Minute), 1000000, true, false)
	require.Equal(t, time.Minute, s.interval)
	require.Zero(t, s.rate) // backlog drain must not become a traffic sample
	require.Positive(t, previousRate)
	for i := 0; i < 30; i++ {
		s.update(now, now, 0, true, false)
	}
	require.Equal(t, 30*time.Minute, s.interval)
	s.update(now, now, 0, false, true)
	require.Equal(t, 30*time.Minute, s.interval)
}

func TestBillingDedupAdaptiveStepsRespectDueTime(t *testing.T) {
	r := &billingDedupRetentionStub{}
	s := NewDashboardAggregationService(r, nil, &config.Config{DashboardAgg: config.DashboardAggregationConfig{Retention: config.DashboardAggregationRetentionConfig{
		UsageBillingDedupOrdinaryAutoDelete: true, UsageBillingDedupBatchImageDeleteDays: 7,
	}}})
	s.dedupDeletionSchedules[0].next = time.Now().Add(time.Hour)
	s.dedupDeletionSchedules[1].next = time.Now().Add(time.Hour)
	s.runScheduledBillingDedupDeletion()
	require.Equal(t, []int{2, 3}, r.steps)
	require.Equal(t, 900*time.Second, s.dedupDeletionSchedules[2].interval)
	s.runScheduledBillingDedupDeletion()
	require.Equal(t, []int{2, 3}, r.steps) // nothing is due yet
}

func TestBillingDedupOrdinaryUsesOneHour(t *testing.T) {
	r := &billingDedupRetentionStub{}
	s := NewDashboardAggregationService(r, nil, &config.Config{DashboardAgg: config.DashboardAggregationConfig{Retention: config.DashboardAggregationRetentionConfig{UsageBillingDedupOrdinaryAutoDelete: true}}})
	before := time.Now().Add(-time.Hour)
	s.runScheduledBillingDedupDeletion()
	after := time.Now().Add(-time.Hour)
	require.Equal(t, []int{0, 1}, r.steps)
	for _, cutoff := range r.ordinaryCutoffs {
		require.False(t, cutoff.Before(before))
		require.False(t, cutoff.After(after))
	}
}
