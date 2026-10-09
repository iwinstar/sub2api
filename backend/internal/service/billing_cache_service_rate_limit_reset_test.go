package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type rateLimitResetLoaderStub struct {
	calls atomic.Int32
	err   error
}

func (s *rateLimitResetLoaderStub) GetRateLimitData(context.Context, int64) (*APIKeyRateLimitData, error) {
	return &APIKeyRateLimitData{}, nil
}

func (s *rateLimitResetLoaderStub) ResetRateLimitWindows(context.Context, int64) error {
	s.calls.Add(1)
	return s.err
}

type rateLimitResetCacheStub struct {
	billingCacheWorkerStub
	invalidated chan bool
	filled      atomic.Bool
}

func (s *rateLimitResetCacheStub) SetAPIKeyRateLimit(context.Context, int64, *APIKeyRateLimitCacheData) error {
	s.filled.Store(true)
	return nil
}

func (s *rateLimitResetCacheStub) InvalidateAPIKeyRateLimit(context.Context, int64) error {
	s.invalidated <- s.filled.Load()
	return nil
}

func TestEvaluateRateLimits_ResetAndInvalidate(t *testing.T) {
	now := time.Now()
	expired := now.Add(-8 * 24 * time.Hour)
	for _, tc := range []struct {
		name       string
		windows    [3]*time.Time
		usage      float64
		resets     int32
		invalidate bool
		resetErr   error
		wantErr    error
	}{
		{name: "nil_zero", invalidate: true},
		{name: "nil_stale_usage", usage: 11, invalidate: true},
		{name: "valid_below_limit", windows: [3]*time.Time{&now, &now, &now}, usage: 1},
		{name: "valid_at_limit", windows: [3]*time.Time{&now, &now, &now}, usage: 10, wantErr: ErrAPIKeyRateLimit5hExceeded},
		{name: "expired_5h_then_nil", windows: [3]*time.Time{&expired, nil, nil}, usage: 11, resets: 1, invalidate: true},
		{name: "expired_1d", windows: [3]*time.Time{nil, &expired, nil}, usage: 11, resets: 1, invalidate: true},
		{name: "expired_7d", windows: [3]*time.Time{nil, nil, &expired}, usage: 11, resets: 1, invalidate: true},
		{name: "all_expired_one_reset", windows: [3]*time.Time{&expired, &expired, &expired}, usage: 11, resets: 1, invalidate: true},
		{name: "mixed_preserves_active_limit", windows: [3]*time.Time{&expired, &now, nil}, usage: 11, resets: 1, invalidate: true, wantErr: ErrAPIKeyRateLimit1dExceeded},
		{name: "nil_preserves_active_limit", windows: [3]*time.Time{nil, nil, &now}, usage: 11, invalidate: true, wantErr: ErrAPIKeyRateLimit7dExceeded},
		{name: "reset_error_still_invalidates", windows: [3]*time.Time{nil, &expired, nil}, usage: 11, resets: 1, invalidate: true, resetErr: errors.New("reset failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loader := &rateLimitResetLoaderStub{err: tc.resetErr}
			cache := &rateLimitResetCacheStub{invalidated: make(chan bool, 2)}
			s := &BillingCacheService{cache: cache, apiKeyRateLimitLoader: loader}
			err := s.evaluateRateLimits(context.Background(), &APIKey{ID: 1, RateLimit5h: 10, RateLimit1d: 10, RateLimit7d: 10}, tc.usage, tc.usage, tc.usage, tc.windows[0], tc.windows[1], tc.windows[2])
			require.ErrorIs(t, err, tc.wantErr)
			if tc.invalidate {
				select {
				case <-cache.invalidated:
				case <-time.After(2 * time.Second):
					t.Fatal("cache was not invalidated")
				}
			}
			require.Equal(t, tc.resets, loader.calls.Load())
		})
	}
}

func TestCheckAPIKeyRateLimits_NilCacheFillStillInvalidates(t *testing.T) {
	loader := &rateLimitResetLoaderStub{}
	cache := &rateLimitResetCacheStub{invalidated: make(chan bool, 1)}
	s := &BillingCacheService{cache: cache, apiKeyRateLimitLoader: loader}
	require.NoError(t, s.checkAPIKeyRateLimits(context.Background(), &APIKey{ID: 1, RateLimit1d: 10}))
	select {
	case filledBeforeInvalidation := <-cache.invalidated:
		require.True(t, filledBeforeInvalidation, "nil window fill must be followed by invalidation")
	case <-time.After(2 * time.Second):
		t.Fatal("cache was not invalidated")
	}
	require.Zero(t, loader.calls.Load())
}
