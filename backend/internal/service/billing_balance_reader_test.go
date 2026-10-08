//go:build unit

package service

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type balanceReaderStub struct {
	UserRepository
	read func(context.Context, int64) (float64, error)
}

func (s *balanceReaderStub) GetBalance(ctx context.Context, id int64) (float64, error) {
	return s.read(ctx, id)
}

func TestBillingBalanceReader(t *testing.T) {
	for _, wantErr := range []error{nil, ErrUserNotFound, context.DeadlineExceeded} {
		repo := &balanceReaderStub{read: func(ctx context.Context, id int64) (float64, error) {
			require.EqualValues(t, 7, id)
			return -2, wantErr
		}}
		svc := &BillingCacheService{userRepo: repo}
		balance, err := svc.getUserBalanceFromDB(context.Background(), 7)
		if wantErr == nil {
			require.NoError(t, err)
			require.Equal(t, -2.0, balance)
		} else {
			require.ErrorIs(t, err, wantErr)
		}
	}
	repo := &balanceLoadUserRepoStub{balance: 3}
	svc := &BillingCacheService{userRepo: repo}
	balance, err := svc.getUserBalanceFromDB(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, 3.0, balance)
}

func TestBillingBalanceReaderDetachedLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo := &balanceReaderStub{read: func(loadCtx context.Context, _ int64) (float64, error) {
		require.NoError(t, loadCtx.Err())
		deadline, ok := loadCtx.Deadline()
		require.True(t, ok)
		require.Positive(t, time.Until(deadline))
		require.LessOrEqual(t, time.Until(deadline), balanceLoadTimeout)
		return 4, nil
	}}
	svc := &BillingCacheService{userRepo: repo, cache: &billingCacheMissStub{}}
	balance, err := svc.GetUserBalance(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 4.0, balance)
}

func TestBillingBalanceReaderSingleflight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers = 16
		release := make(chan struct{})
		var calls atomic.Int64
		repo := &balanceReaderStub{read: func(context.Context, int64) (float64, error) {
			calls.Add(1)
			<-release
			return 12.34, nil
		}}
		// Calling GetByID through the embedded nil UserRepository would panic.
		svc := &BillingCacheService{userRepo: repo, cache: &billingCacheMissStub{}}
		type result struct {
			balance float64
			err     error
		}
		results := make(chan result, callers)
		for i := 0; i < callers; i++ {
			go func() {
				balance, err := svc.GetUserBalance(context.Background(), 99)
				results <- result{balance, err}
			}()
		}
		// Wait until the loader and all singleflight followers are blocked.
		synctest.Wait()
		loadsWhileBlocked := calls.Load()
		close(release)
		for i := 0; i < callers; i++ {
			got := <-results
			require.NoError(t, got.err)
			require.Equal(t, 12.34, got.balance)
		}
		require.EqualValues(t, 1, loadsWhileBlocked)
		require.EqualValues(t, 1, calls.Load())
	})
}
