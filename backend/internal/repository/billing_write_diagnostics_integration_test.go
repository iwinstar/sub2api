//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type billingProbeMeasurement struct {
	*usageBillingRepository
	calls atomic.Int64
	nanos atomic.Int64
}

func (b *billingProbeMeasurement) probeBillingLocks(ctx context.Context, cmds []*service.UsageBillingCommand) ([]billingSubject, error) {
	start := time.Now()
	defer func() { b.calls.Add(1); b.nanos.Add(time.Since(start).Nanoseconds()) }()
	return b.usageBillingRepository.probeBillingLocks(ctx, cmds)
}

func TestBillingBatchProbePerformance(t *testing.T) {
	if os.Getenv("SUB2API_BILLING_WRITE_BENCH") != "1" {
		t.Skip("opt-in billing diagnostics")
	}
	for repeat := 0; repeat < 3; repeat++ {
		for _, connections := range []int{33, 3} {
			for _, blocked := range []bool{false, true} {
				t.Run(fmt.Sprintf("repeat=%d/connections=%d/blocked=%t", repeat, connections, blocked), func(t *testing.T) { runBillingProbeDiagnostic(t, connections, blocked) })
			}
		}
	}
}

func runBillingProbeDiagnostic(t *testing.T, connections int, blocked bool) {
	ctx := context.Background()
	client := testEntClient(t)
	account := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Extra: map[string]any{"quota_limit": 1000000}})
	var healthyAccount *service.Account
	for i := 0; i < 8; i++ {
		healthyAccount = mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Extra: map[string]any{"quota_limit": 1000000}})
	}
	var users, keys []int64
	for i := 0; i < 32; i++ {
		u := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 1000000})
		k := mustCreateApiKey(t, client, &service.APIKey{UserID: u.ID, Key: "sk-" + uuid.NewString(), Name: "probe-perf", Quota: 1000000})
		users = append(users, u.ID)
		keys = append(keys, k.ID)
	}
	_, vacuumErr := integrationDB.ExecContext(ctx, `VACUUM ANALYZE`)
	require.NoError(t, vacuumErr)
	original := integrationDB.Stats().MaxOpenConnections
	integrationDB.SetMaxOpenConns(connections)
	integrationDB.SetMaxIdleConns(connections)
	defer func() { integrationDB.SetMaxIdleConns(2); integrationDB.SetMaxOpenConns(original) }()
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	if blocked {
		_, err = tx.ExecContext(ctx, `SELECT id FROM accounts WHERE id=$1 FOR NO KEY UPDATE`, account.ID)
		require.NoError(t, err)
	}
	backend := &billingProbeMeasurement{usageBillingRepository: &usageBillingRepository{db: integrationDB}}
	queue := newUsageBillingBatchRepository(backend, 8, 16384)
	queue.shardMu.Lock()
	queue.accountOrdinals[account.ID], queue.accountOrdinals[healthyAccount.ID] = 0, 8
	queue.shardMu.Unlock()
	defer queue.Stop()
	blockedDone := make(chan error, 1)
	if blocked {
		queue.blockedMu.Lock()
		queue.blocked[billingSubject{'a', account.ID}] = time.Now().Add(billingBlockedTTL)
		queue.blockedMu.Unlock()
		go func() {
			_, e := queue.Apply(ctx, &service.UsageBillingCommand{RequestID: uuid.NewString(), APIKeyID: keys[0], AccountID: account.ID, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: .01})
			blockedDone <- e
		}()
	}
	var wg sync.WaitGroup
	var completed, settled, errors atomic.Int64
	var mu sync.Mutex
	var latencies []time.Duration
	start := time.Now()
	end := start.Add(4 * time.Second)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var local []time.Duration
			for time.Now().Before(end) {
				began := time.Now()
				v, e := queue.Apply(ctx, &service.UsageBillingCommand{RequestID: uuid.NewString(), UserID: users[i], APIKeyID: keys[i], BalanceCost: .01, APIKeyQuotaCost: .01, APIKeyRateLimitCost: .01, AccountID: healthyAccount.ID, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: .01})
				if e != nil || v == nil || !v.Applied {
					errors.Add(1)
				} else {
					settled.Add(1)
					if time.Now().Before(end) {
						completed.Add(1)
						local = append(local, time.Since(began))
					}
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	calls, nanos := backend.calls.Load(), backend.nanos.Load()
	require.NoError(t, tx.Rollback())
	if blocked {
		require.NoError(t, <-blockedDone)
	}
	var charged float64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT (extra->>'quota_used')::numeric FROM accounts WHERE id=$1`, healthyAccount.ID).Scan(&charged))
	require.InDelta(t, float64(settled.Load())*.01, charged, 1e-6)
	require.Zero(t, errors.Load())
	require.NotEmpty(t, latencies)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	report := map[string]any{"scenario": t.Name(), "healthy_rps": float64(completed.Load()) / 4, "p99_ms": float64(latencies[(len(latencies)-1)*99/100]) / float64(time.Millisecond), "probe_calls": calls, "probe_total_ms": float64(nanos) / float64(time.Millisecond), "errors": errors.Load()}
	data, e := json.Marshal(report)
	require.NoError(t, e)
	t.Log("BILLING_PROBE_RESULT " + string(data))
}

func TestBillingBatchUsagePoolPressure(t *testing.T) {
	if os.Getenv("SUB2API_BILLING_WRITE_BENCH") != "1" {
		t.Skip("opt-in billing diagnostics")
	}
	ctx := context.Background()
	client := testEntClient(t)
	account := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Extra: map[string]any{"quota_limit": 1000000}})
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `SELECT id FROM accounts WHERE id=$1 FOR NO KEY UPDATE`, account.ID)
	require.NoError(t, err)
	queue := NewUsageBillingBatchRepository(&usageBillingRepository{db: integrationDB})
	defer queue.Stop()
	queue.blockedMu.Lock()
	queue.blocked[billingSubject{'a', account.ID}] = time.Now().Add(billingBlockedTTL)
	queue.blockedMu.Unlock()
	pool := service.NewUsageRecordWorkerPoolWithOptions(service.UsageRecordWorkerPoolOptions{BillingWriteObservabilityEnabled: true, WorkerCount: 128, QueueSize: 16384, TaskTimeout: 5 * time.Second, OverflowPolicy: "sync"})
	defer pool.Stop()
	// Fill real production-sized usage queue while external row lock persists.
	var completed, failed atomic.Int64
	finished := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(finished)
		for i := 0; i < 16600; i++ {
			pool.Submit(func(workerCtx context.Context) {
				billingCtx, cancel := context.WithTimeout(context.WithoutCancel(workerCtx), 15*time.Second)
				defer cancel()
				_, e := queue.Apply(billingCtx, &service.UsageBillingCommand{RequestID: uuid.NewString(), APIKeyID: -1, AccountID: account.ID, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: .01})
				if e != nil {
					failed.Add(1)
				} else {
					completed.Add(1)
				}
			})
		}
	}()
	var samples []map[string]any
	for _, second := range []int{5, 16, 31} {
		time.Sleep(time.Until(start.Add(time.Duration(second) * time.Second)))
		s := pool.Stats()
		samples = append(samples, map[string]any{"second": second, "running_workers": s.RunningWorkers, "waiting_tasks": s.WaitingTasks, "oldest_pending_ms": float64(s.OldestPendingAge) / float64(time.Millisecond), "sync_fallback": s.SyncFallbackTasks, "billing_errors": failed.Load()})
	}
	require.NoError(t, tx.Rollback())
	<-finished
	pool.Stop()
	queue.Stop()
	data, e := json.Marshal(map[string]any{"samples": samples, "success": completed.Load(), "billing_errors": failed.Load(), "elapsed_s": time.Since(start).Seconds(), "final_stats": pool.Stats()})
	require.NoError(t, e)
	t.Log("BILLING_USAGE_POOL_RESULT " + string(data))
	require.EqualValues(t, 16600, completed.Load()+failed.Load())
	var charged float64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT (extra->>'quota_used')::numeric FROM accounts WHERE id=$1`, account.ID).Scan(&charged))
	require.InDelta(t, float64(completed.Load())*.01, charged, 1e-6)
}
