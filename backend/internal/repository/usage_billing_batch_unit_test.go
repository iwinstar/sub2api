//go:build unit

package repository

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type billingQueueFake struct {
	service.UsageBillingRepository
	single func(context.Context, *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error)
	batch  func(context.Context, []*service.UsageBillingCommand) ([]billingBatchOutcome, error)
	probe  func(context.Context, []*service.UsageBillingCommand) ([]billingSubject, error)
}

func (f *billingQueueFake) apply(ctx context.Context, c *service.UsageBillingCommand, lock bool) (*service.UsageBillingApplyResult, error) {
	if !lock {
		panic("missing lock timeout")
	}
	return f.single(ctx, c)
}
func (f *billingQueueFake) ApplyBatch(ctx context.Context, c []*service.UsageBillingCommand) ([]billingBatchOutcome, error) {
	if f.batch != nil {
		return f.batch(ctx, c)
	}
	out := make([]billingBatchOutcome, len(c))
	for i, cmd := range c {
		result, err := f.single(ctx, cmd)
		if err != nil {
			return nil, err
		}
		out[i].value = result
	}
	return out, nil
}

func (f *billingQueueFake) probeBillingLocks(ctx context.Context, cmds []*service.UsageBillingCommand) ([]billingSubject, error) {
	if f.probe != nil {
		return f.probe(ctx, cmds)
	}
	return nil, nil
}

func TestBillingBatchQueueDrainCancellationAndFull(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var batches atomic.Int32
	fake := &billingQueueFake{single: func(context.Context, *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}, batch: func(_ context.Context, c []*service.UsageBillingCommand) ([]billingBatchOutcome, error) {
		batches.Add(1)
		out := make([]billingBatchOutcome, len(c))
		for i := range out {
			out[i].value = &service.UsageBillingApplyResult{Applied: true}
		}
		return out, nil
	}}
	r := newUsageBillingBatchRepository(fake, 1, 4)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, e := r.Apply(ctx, &service.UsageBillingCommand{RequestID: "first"})
		require.NoError(t, e)
		require.True(t, v.Applied)
	}()
	<-started
	cancel() // execution won: cancelling the caller must not abandon its result.
	cancelled, cancelWaiting := context.WithCancel(context.Background())
	waitingDone := make(chan error, 1)
	go func() {
		_, e := r.Apply(cancelled, &service.UsageBillingCommand{RequestID: "cancelled"})
		waitingDone <- e
	}()
	require.Eventually(t, func() bool { return len(r.queues[0]) == 1 }, time.Second, time.Millisecond)
	cancelWaiting()
	require.ErrorIs(t, <-waitingDone, context.Canceled)
	for _, id := range []string{"second", "third"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			v, e := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: id})
			require.NoError(t, e)
			require.True(t, v.Applied)
		}(id)
	}
	require.Eventually(t, func() bool { return len(r.queues[0]) == 3 }, time.Second, time.Millisecond)
	fullCtx, cancelFull := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelFull()
	_, err := r.Apply(fullCtx, &service.UsageBillingCommand{RequestID: "full"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	stopped := make(chan struct{})
	go func() { r.Stop(); close(stopped) }()
	require.Eventually(t, func() bool { r.mu.RLock(); defer r.mu.RUnlock(); return r.stopped }, time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("shutdown completed with transaction in flight")
	default:
	}
	close(release)
	wg.Wait()
	<-stopped
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 1, batches.Load())
	_, err = r.Apply(context.Background(), &service.UsageBillingCommand{})
	require.ErrorIs(t, err, errUsageBillingStopped)
}

func TestBillingBatchRecoveryUnknownAndLockIsolation(t *testing.T) {
	var calls atomic.Int32
	blocked, release := make(chan struct{}), make(chan struct{})
	fake := &billingQueueFake{single: func(ctx context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if c.RequestID == "unknown" {
			if calls.Add(1) == 1 {
				return nil, service.ErrUsageBillingOutcomeUnknown
			}
			return &service.UsageBillingApplyResult{}, nil
		}
		if c.RequestID == "blocked" {
			if calls.Add(1) == 3 {
				return nil, service.ErrUsageBillingOutcomeUnknown
			}
			close(blocked)
			<-release
			return nil, errors.New("terminal failure")
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}, batch: func(context.Context, []*service.UsageBillingCommand) ([]billingBatchOutcome, error) {
		panic("unexpected batch")
	}}
	r := newUsageBillingBatchRepository(fake, 1, 4)
	defer r.Stop()
	result, err := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: "unknown"})
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.True(t, result.ConfirmedExisting)
	done := make(chan error, 1)
	go func() {
		_, e := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: "blocked"})
		done <- e
	}()
	<-blocked
	result, err = r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: "healthy"})
	require.NoError(t, err)
	require.True(t, result.Applied)
	close(release)
	require.Error(t, <-done)
}

func TestBillingBatchCommitClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		unknown bool
	}{
		{"network", io.EOF, true}, {"protocol", errors.New("unexpected command tag"), true},
		{"rejected", &pq.Error{Code: "23514", Severity: "ERROR"}, false},
		{"fatal", &pq.Error{Code: "57P01", Severity: "FATAL"}, true},
		{"failed transaction", pq.ErrInFailedTransaction, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, e := sqlmock.New()
			require.NoError(t, e)
			defer db.Close()
			mock.ExpectBegin()
			mock.ExpectCommit().WillReturnError(tc.err)
			tx, e := db.Begin()
			require.NoError(t, e)
			e = commitUsageBilling(context.Background(), tx)
			require.Equal(t, tc.unknown, errors.Is(e, service.ErrUsageBillingOutcomeUnknown))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
	t.Run("ended transaction without cancellation evidence", func(t *testing.T) {
		db, mock, e := sqlmock.New()
		require.NoError(t, e)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectRollback()
		tx, e := db.Begin()
		require.NoError(t, e)
		require.NoError(t, tx.Rollback())
		require.ErrorIs(t, commitUsageBilling(context.Background(), tx), service.ErrUsageBillingOutcomeUnknown)
		require.NoError(t, mock.ExpectationsWereMet())
	})
	t.Run("cancel before commit", func(t *testing.T) {
		db, mock, e := sqlmock.New()
		require.NoError(t, e)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectRollback()
		tx, e := db.Begin()
		require.NoError(t, e)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, commitUsageBilling(ctx, tx), context.Canceled)
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestBillingBatchRecoverySaturationRetainsCharges(t *testing.T) {
	entered := make(chan struct{}, billingRecoveryWorkers)
	release := make(chan struct{})
	var failed atomic.Bool
	fake := &billingQueueFake{single: func(_ context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if c.RequestID == "hold" {
			entered <- struct{}{}
			<-release
		}
		if c.RequestID == "retry" && !failed.Swap(true) {
			return nil, service.ErrUsageBillingOutcomeUnknown
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}, batch: func(_ context.Context, cmds []*service.UsageBillingCommand) ([]billingBatchOutcome, error) {
		out := make([]billingBatchOutcome, len(cmds))
		for i := range out {
			out[i].value = &service.UsageBillingApplyResult{Applied: true}
		}
		return out, nil
	}}
	r := newUsageBillingBatchRepository(fake, 1, 64)
	defer r.Stop()
	defer close(release)
	seed := func(id string) []*usageBillingBatchItem {
		item := &usageBillingBatchItem{cmd: &service.UsageBillingCommand{RequestID: id}, enqueued: time.Now(), result: make(chan billingBatchOutcome, 1)}
		item.state.Store(billingItemExecuting)
		return []*usageBillingBatchItem{item}
	}
	for i := 0; i < billingRecoveryWorkers; i++ {
		r.recovery <- seed("hold")
	}
	for i := 0; i < billingRecoveryWorkers; i++ {
		<-entered
	}
	for i := 0; i < billingRecoveryCapacity; i++ {
		r.recovery <- seed("queued")
	}
	done := make(chan error, 1)
	go func() {
		_, err := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: "retry"})
		done <- err
	}()
	require.Eventually(t, failed.Load, time.Second, time.Millisecond)
	result, err := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: "healthy"})
	require.NoError(t, err)
	require.True(t, result.Applied)
	select {
	case err := <-done:
		t.Fatalf("recovery saturation abandoned charge: %v", err)
	default:
	}
	// Release workers without closing twice in the deferred cleanup.
	release <- struct{}{}
	release <- struct{}{}
	require.NoError(t, <-done)
}

func TestBillingBatchPanicContained(t *testing.T) {
	var recoveryAttempt atomic.Bool
	fake := &billingQueueFake{single: func(_ context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if c.RequestID == "recovery" && !recoveryAttempt.Swap(true) {
			return nil, service.ErrUsageBillingOutcomeUnknown
		}
		if c.RequestID != "healthy" {
			panic("test panic")
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}}
	r := newUsageBillingBatchRepository(fake, 1, 4)
	defer r.Stop()
	for _, id := range []string{"primary", "recovery"} {
		_, err := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: id})
		require.ErrorContains(t, err, "test panic")
	}
	result, err := r.Apply(context.Background(), &service.UsageBillingCommand{RequestID: "healthy"})
	require.NoError(t, err)
	require.True(t, result.Applied)
}

func TestBillingBatchAccountRecoverySerializesAndStopDrains(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	fake := &billingQueueFake{single: func(_ context.Context, c *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
		if c.AccountID == 2 {
			require.EqualValues(t, 1, active.Add(1), "same account executed concurrently")
			defer active.Add(-1)
			switch calls.Add(1) {
			case 1:
				return nil, service.ErrUsageBillingOutcomeUnknown
			case 2:
				close(entered)
				<-release
				return nil, &pq.Error{Code: "55P03"}
			}
		}
		return &service.UsageBillingApplyResult{Applied: true}, nil
	}}
	r := newUsageBillingBatchRepository(fake, 2, 2)
	command := func(account int64) *service.UsageBillingCommand {
		return &service.UsageBillingCommand{AccountID: account, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: 1}
	}
	done := make(chan error, 2)
	go func() { _, err := r.Apply(context.Background(), command(2)); done <- err }()
	<-entered
	go func() { _, err := r.Apply(context.Background(), command(2)); done <- err }()
	require.Eventually(t, func() bool { return len(r.permits) == 2 }, time.Second, time.Millisecond)
	waiting := make(chan error, 1)
	go func() { _, err := r.Apply(context.Background(), command(3)); waiting <- err }()
	stopped := make(chan struct{})
	go func() { r.Stop(); close(stopped) }()
	require.ErrorIs(t, <-waiting, errUsageBillingStopped)
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop failed to drain recovery requeue")
	}
	require.GreaterOrEqual(t, calls.Load(), int32(4))
}

func TestBillingBatchConfigurationSelectsRepository(t *testing.T) {
	base := &usageBillingRepository{}
	for _, cfg := range []*config.Config{nil, {}} {
		batch := ProvideUsageBillingBatchRepository(base, cfg)
		require.Nil(t, batch, "disabled batching must not start background workers")
		require.False(t, base.optimizedWrites)
		require.False(t, ProvideBillingAccountRepository(nil, nil, nil, cfg).(*accountRepository).optimizedBillingWrites)
		require.Same(t, base, ProvideUsageBillingRepository(base, batch))
	}
	cfg := &config.Config{}
	cfg.Billing.AutomaticBatchEnabled = true
	batch := ProvideUsageBillingBatchRepository(base, cfg)
	require.NotNil(t, batch)
	require.True(t, base.optimizedWrites)
	require.True(t, ProvideBillingAccountRepository(nil, nil, nil, cfg).(*accountRepository).optimizedBillingWrites)
	defer batch.Stop()
	require.Same(t, batch, ProvideUsageBillingRepository(base, batch))
}
