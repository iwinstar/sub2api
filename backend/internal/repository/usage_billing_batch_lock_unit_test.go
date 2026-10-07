//go:build unit

package repository

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestBillingBatchProbeRequiresTwoSnapshots(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	cmd := &service.UsageBillingCommand{UserID: 1, BalanceCost: 1, APIKeyID: 2, APIKeyQuotaCost: 1, AccountID: 8, AccountQuotaCost: 1, AccountType: service.AccountTypeAPIKey}
	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL lock_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`(?s)SELECT id FROM users .* FOR NO KEY UPDATE SKIP LOCKED`).
		WithArgs(pq.Array([]int64{1})).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1)).RowsWillBeClosed()
	mock.ExpectQuery(`(?s)SELECT id FROM api_keys .* FOR NO KEY UPDATE SKIP LOCKED`).
		WithArgs(pq.Array([]int64{2})).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(2)).RowsWillBeClosed()
	mock.ExpectQuery(`(?s)SELECT id FROM accounts .* FOR NO KEY UPDATE SKIP LOCKED`).
		WithArgs(pq.Array([]int64{8})).WillReturnRows(sqlmock.NewRows([]string{"id"})).RowsWillBeClosed()
	mock.ExpectRollback() // The first probe must release locks before starting the second.
	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL lock_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`(?s)SELECT id FROM users .* FOR NO KEY UPDATE SKIP LOCKED`).
		WithArgs(pq.Array([]int64{1})).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1)).RowsWillBeClosed()
	mock.ExpectQuery(`(?s)SELECT id FROM api_keys .* FOR NO KEY UPDATE SKIP LOCKED`).
		WithArgs(pq.Array([]int64{2})).WillReturnRows(sqlmock.NewRows([]string{"id"})).RowsWillBeClosed()
	mock.ExpectRollback()
	base := &usageBillingRepository{db: db}
	blocked, err := base.probeBillingLocks(context.Background(), []*service.UsageBillingCommand{cmd})
	require.NoError(t, err)
	require.Equal(t, []billingSubject{{'u', 1}}, blocked)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBillingBatchLockedUserDoesNotIsolateSharedAccount(t *testing.T) {
	var locked atomic.Bool
	locked.Store(true)
	var blockedCalls atomic.Int32
	fake := &billingQueueFake{
		single: func(_ context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
			if c.UserID == 1 {
				blockedCalls.Add(1)
				if locked.Load() {
					return nil, &pq.Error{Code: "55P03"}
				}
			}
			return &service.UsageBillingApplyResult{Applied: true}, nil
		},
		probe: func(context.Context, []*service.UsageBillingCommand) ([]billingSubject, error) {
			if locked.Load() {
				return []billingSubject{{'u', 1}}, nil
			}
			return nil, nil
		},
	}
	r := newUsageBillingBatchRepository(fake, 8, 8)
	defer r.Stop()
	defer locked.Store(false)
	command := func(user int64) *service.UsageBillingCommand {
		return &service.UsageBillingCommand{UserID: user, BalanceCost: 1, AccountID: 8, AccountQuotaCost: 1, AccountType: service.AccountTypeAPIKey}
	}
	done := make(chan error, 1)
	go func() { _, err := r.Apply(context.Background(), command(1)); done <- err }()
	require.Eventually(t, func() bool { return r.isBlocked(command(1)) }, time.Second, time.Millisecond)
	require.False(t, r.isBlocked(command(2)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := r.Apply(ctx, command(2))
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.EqualValues(t, 1, blockedCalls.Load(), "known row lock should be probed, not waited on with an account gate")
	locked.Store(false)
	require.NoError(t, <-done)
	require.False(t, r.isBlocked(command(1)))
}

func TestBillingBatchBusyAccountGateDoesNotBlockSameShard(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	var active atomic.Int32
	var overlapping atomic.Bool
	fake := &billingQueueFake{single: func(_ context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if c.AccountID == 8 {
			if active.Add(1) != 1 {
				overlapping.Store(true)
			}
			defer active.Add(-1)
			switch calls.Add(1) {
			case 1:
				return nil, service.ErrUsageBillingOutcomeUnknown
			case 2:
				close(entered)
				<-release
			}
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}}
	r := newUsageBillingBatchRepository(fake, 1, 8)
	defer r.Stop()
	defer once.Do(func() { close(release) })
	command := func(account int64) *service.UsageBillingCommand {
		return &service.UsageBillingCommand{AccountID: account, AccountQuotaCost: 1, AccountType: service.AccountTypeAPIKey}
	}
	done := make(chan error, 2)
	go func() { _, err := r.Apply(context.Background(), command(8)); done <- err }()
	<-entered
	go func() { _, err := r.Apply(context.Background(), command(8)); done <- err }()
	require.Eventually(t, func() bool { return len(r.permits) == 2 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Both accounts share the sole executor, but have independent account gates.
	result, err := r.Apply(ctx, command(16))
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.EqualValues(t, 2, calls.Load(), "main worker must skip the busy account")
	once.Do(func() { close(release) })
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.False(t, overlapping.Load())
}

func TestBillingBatchSuccessClearsIsolationAndNearDeadlineCanFinish(t *testing.T) {
	var remaining time.Duration
	fake := &billingQueueFake{single: func(ctx context.Context, _ *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		deadline, _ := ctx.Deadline()
		remaining = time.Until(deadline)
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}}
	r := newUsageBillingBatchRepository(fake, 1, 1)
	defer r.Stop()
	item := &usageBillingBatchItem{cmd: &service.UsageBillingCommand{UserID: 1, BalanceCost: 1}, enqueued: time.Now().Add(-billingProcessingBudget + 100*time.Millisecond), result: make(chan billingBatchOutcome, 1)}
	item.state.Store(billingItemExecuting)
	r.blocked[billingSubject{'u', 1}] = time.Now().Add(billingBlockedTTL)
	require.NoError(t, r.execute([]*usageBillingBatchItem{item}, true))
	require.Greater(t, remaining, 3*time.Second)
	require.False(t, r.isBlocked(item.cmd))
}

func TestBillingBatchProbeRateLimitIsSharedBySubject(t *testing.T) {
	var calls atomic.Int32
	var starts []time.Time
	var mu sync.Mutex
	fake := &billingQueueFake{probe: func(_ context.Context, cmds []*service.UsageBillingCommand) ([]billingSubject, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		// Requests share the account and key too, but only the blocked user
		// should be probed. Do not expand the probe back to the entire request.
		require.Len(t, cmds, 1)
		require.Equal(t, []billingSubject{{'u', 1}}, billingSubjects(cmds[0]))
		if calls.Add(1) == 1 {
			return []billingSubject{{'u', 1}}, nil
		}
		return nil, nil
	}}
	r := newUsageBillingBatchRepository(fake, 8, 128)
	defer r.Stop()
	cmd := &service.UsageBillingCommand{UserID: 1, BalanceCost: 1, APIKeyID: 2, APIKeyQuotaCost: 1, AccountID: 8, AccountQuotaCost: 1, AccountType: service.AccountTypeAPIKey}
	r.blocked[billingSubject{'u', 1}] = time.Now().Add(billingBlockedTTL)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.refreshBlocked([]*usageBillingBatchItem{{cmd: cmd}}, true)
		}()
	}
	wg.Wait()
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		r.refreshBlocked([]*usageBillingBatchItem{{cmd: cmd}}, true)
		return !r.isBlocked(cmd)
	}, time.Second, 5*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, starts, 2)
	require.GreaterOrEqual(t, starts[1].Sub(starts[0]), billingProbeInterval-5*time.Millisecond)
}

func TestBillingBatchBackgroundProbeBoundAndDrain(t *testing.T) {
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	fake := &billingQueueFake{probe: func(context.Context, []*service.UsageBillingCommand) ([]billingSubject, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}}
	r := newUsageBillingBatchRepository(fake, 1, 8)
	defer r.Stop()
	defer once.Do(func() { close(release) })
	for id := int64(1); id <= 3; id++ {
		r.blockedMu.Lock()
		r.blocked[billingSubject{'u', id}] = time.Now().Add(billingBlockedTTL)
		r.blockedMu.Unlock()
		r.refreshBlocked([]*usageBillingBatchItem{{cmd: &service.UsageBillingCommand{UserID: id, BalanceCost: 1}}}, true)
	}
	<-entered
	<-entered
	require.Len(t, r.probeSlots, 2)
	done := make(chan struct{})
	go func() { r.Stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("Stop returned before the background probes drained")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop failed to drain probes")
	}
}

func TestBillingBatchBlockedTrafficLeavesRecoveryAvailable(t *testing.T) {
	var locked atomic.Bool
	locked.Store(true)
	var confirmationCalls atomic.Int32
	fake := &billingQueueFake{
		probe: func(context.Context, []*service.UsageBillingCommand) ([]billingSubject, error) {
			if locked.Load() {
				return []billingSubject{{'u', 1}}, nil
			}
			return nil, nil
		},
		single: func(_ context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
			if cmd.UserID == 1 && locked.Load() {
				return nil, &pq.Error{Code: "55P03"}
			}
			if cmd.RequestID == "confirm" && confirmationCalls.Add(1) == 1 {
				return nil, service.ErrUsageBillingOutcomeUnknown
			}
			return &service.UsageBillingApplyResult{Applied: true}, nil
		},
	}
	r := newUsageBillingBatchRepository(fake, 8, 128)
	defer r.Stop()
	defer locked.Store(false)
	r.blocked[billingSubject{'u', 1}] = time.Now().Add(billingBlockedTTL)
	done := make(chan error, 64)
	for i := 0; i < cap(done); i++ {
		go func() {
			_, err := r.Apply(context.Background(), &service.UsageBillingCommand{UserID: 1, BalanceCost: 1})
			done <- err
		}()
	}
	require.Eventually(t, func() bool { return len(r.permits) == cap(done) }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := r.Apply(ctx, &service.UsageBillingCommand{RequestID: "confirm", UserID: 2, BalanceCost: 1})
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.EqualValues(t, 2, confirmationCalls.Load())
	require.Empty(t, r.recovery, "ordinary blocked work must not occupy recovery")
	locked.Store(false)
	for i := 0; i < cap(done); i++ {
		require.NoError(t, <-done)
	}
}

func TestBillingBatchAccountShardsStayBalancedAndStable(t *testing.T) {
	r := newUsageBillingBatchRepository(&billingQueueFake{}, 8, 8)
	defer r.Stop()
	counts := make([]int, 8)
	for i := int64(1); i <= 20; i++ {
		id := i * 8
		shard := r.accountShard(id)
		counts[shard]++
		require.Equal(t, shard, r.accountShard(id))
	}
	for _, count := range counts {
		require.GreaterOrEqual(t, count, 2)
		require.LessOrEqual(t, count, 3)
	}
}

func TestBillingBatchAccountGateCollisionAcrossShards(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fake := &billingQueueFake{single: func(_ context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if cmd.AccountID == 1 {
			once.Do(func() { close(entered) })
			<-release
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}}
	fake.batch = func(ctx context.Context, cmds []*service.UsageBillingCommand) ([]billingBatchOutcome, error) {
		results := make([]billingBatchOutcome, len(cmds))
		for i, cmd := range cmds {
			results[i].value, results[i].err = fake.single(ctx, cmd)
		}
		return results, nil
	}
	r := newUsageBillingBatchRepository(fake, 8, 128)
	defer r.Stop()
	defer close(release)
	command := func(id int64) *service.UsageBillingCommand {
		return &service.UsageBillingCommand{AccountID: id, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: 1}
	}
	require.NotEqual(t, r.accountShard(1), r.accountShard(257))
	// Saturate X and hold its gate continuously, a stronger condition than
	// hoping the test happens to catch a gap between successive transactions.
	for i := 0; i < 32; i++ {
		go func() { _, _ = r.Apply(context.Background(), command(1)) }()
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, err := r.Apply(ctx, command(257))
	require.NoError(t, err)
	require.True(t, result.Applied)
	// Even after ordinal wraparound a shared gate implies the same shard.
	for i := int64(2); i < 600; i++ {
		r.accountShard(i)
	}
	gates := map[int]uint64{}
	for i := int64(1); i < 600; i++ {
		g := r.accountGateIndex(command(i))
		shard := r.accountShard(i)
		if previous, ok := gates[g]; ok {
			require.Equal(t, previous, shard)
		}
		gates[g] = shard
	}
}

func TestBillingBatchInitialDiagnosisWaitsEvenWhenRefreshSlotsFull(t *testing.T) {
	var calls atomic.Int32
	var locked atomic.Bool
	locked.Store(true)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var once sync.Once
	fake := &billingQueueFake{
		single: func(_ context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
			if c.UserID == 1 {
				calls.Add(1)
				if locked.Load() {
					return nil, &pq.Error{Code: "55P03"}
				}
			}
			return &service.UsageBillingApplyResult{Applied: true}, nil
		},
		probe: func(context.Context, []*service.UsageBillingCommand) ([]billingSubject, error) {
			once.Do(func() { close(started); <-release })
			if locked.Load() {
				return []billingSubject{{'u', 1}}, nil
			}
			return nil, nil
		},
	}
	r := newUsageBillingBatchRepository(fake, 1, 8)
	defer r.Stop()
	defer locked.Store(false)
	defer releaseOnce.Do(func() { close(release) })
	for i := 0; i < cap(r.probeSlots); i++ {
		r.probeSlots <- struct{}{}
	}
	defer func() {
		for len(r.probeSlots) > 0 {
			<-r.probeSlots
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := r.Apply(context.Background(), &service.UsageBillingCommand{UserID: 1, BalanceCost: 1})
		done <- err
	}()
	<-started
	time.Sleep(80 * time.Millisecond) // longer than retryAt and the real probe gap
	require.EqualValues(t, 1, calls.Load())
	require.Empty(t, r.queues[0], "failed request must not requeue before diagnosis completes")
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool { return r.isBlocked(&service.UsageBillingCommand{UserID: 1, BalanceCost: 1}) }, time.Second, time.Millisecond)
	healthy, err := r.Apply(context.Background(), &service.UsageBillingCommand{UserID: 2, BalanceCost: 1})
	require.NoError(t, err)
	require.True(t, healthy.Applied)
	locked.Store(false)
	r.clearBlocked(&service.UsageBillingCommand{UserID: 1, BalanceCost: 1})
	require.NoError(t, <-done)
}

func TestBillingBatchGateCountMatchesNondefaultWorkers(t *testing.T) {
	r := newUsageBillingBatchRepository(&billingQueueFake{}, 3, 8)
	defer r.Stop()
	require.Zero(t, len(r.accountGates)%len(r.queues))
	for id := int64(1); id <= 600; id++ {
		cmd := &service.UsageBillingCommand{AccountID: id}
		require.Equal(t, r.accountShard(id), uint64(r.accountGateIndex(cmd)%len(r.queues)))
	}
}
