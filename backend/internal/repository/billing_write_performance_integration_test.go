//go:build integration

package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Opt-in, bounded comparison using only the disposable TestMain database and
// its real migrations/triggers. Never accepts an external database URL.
func TestBillingWritePerformance(t *testing.T) {
	if os.Getenv("SUB2API_BILLING_WRITE_BENCH") != "1" {
		t.Skip("GitHub-only opt-in billing comparison")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	originalMaxOpen := integrationDB.Stats().MaxOpenConnections
	integrationDB.SetMaxOpenConns(33) // 32 writers plus one lock sampler.
	integrationDB.SetMaxIdleConns(33) // Keep writer connections warm between comparison cases.
	defer func() {
		// The integration harness leaves MaxIdleConns at database/sql's default.
		integrationDB.SetMaxIdleConns(2)
		integrationDB.SetMaxOpenConns(originalMaxOpen)
	}()
	for _, users := range []int{500, 5000} {
		for _, accounts := range []int{1, 5, 20} {
			for _, size := range []int{512, 8192, 32768} {
				for _, paced := range []bool{false, true} {
					// Alternate order to reduce a systematic warm-cache advantage.
					order := []bool{false, true}
					if paced {
						order = []bool{true, false}
					}
					for _, combined := range order {
						name := fmt.Sprintf("users=%d/accounts=%d/extra=%d/paced=%t/combined=%t", users, accounts, size, paced, combined)
						t.Run(name, func(t *testing.T) { runBillingWriteComparison(t, ctx, users, accounts, size, paced, combined) })
					}
				}
			}
		}
	}
}

type billingBenchmarkLayoutKey struct{}

type billingWriteTiming struct {
	total, begin, claim, balance, key, account, commit time.Duration
	err                                                error
}

func runBillingWriteComparison(t *testing.T, ctx context.Context, users, accounts, size int, paced, combined bool, automatic ...bool) {
	operations := 5000
	if len(automatic) > 0 {
		operations = 10000
		if paced {
			operations = 33334
		}
	}
	prefix := "billing-perf-" + uuid.NewString()
	var userIDs, keyIDs, accountIDs []int64
	// Each subtest cleans only its own rows. The container is also terminated by TestMain.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, query := range []struct {
			sql string
			arg any
		}{
			{"DELETE FROM usage_billing_dedup WHERE api_key_id = ANY($1)", pq.Array(keyIDs)},
			{"DELETE FROM scheduler_outbox WHERE account_id = ANY($1)", pq.Array(accountIDs)},
			{"DELETE FROM api_keys WHERE id = ANY($1)", pq.Array(keyIDs)},
			{"DELETE FROM users WHERE id = ANY($1)", pq.Array(userIDs)},
			{"DELETE FROM accounts WHERE id = ANY($1)", pq.Array(accountIDs)},
		} {
			_, err := integrationDB.ExecContext(cleanupCtx, query.sql, query.arg)
			require.NoError(t, err)
		}
		_, err := integrationDB.ExecContext(cleanupCtx, `DELETE FROM auth_cache_invalidation_outbox WHERE cache_key IN
   (SELECT encode(sha256(convert_to($1 || i::text, 'UTF8')), 'hex') FROM generate_series(1,$2::int) i)`, prefix, users)
		require.NoError(t, err)
	})
	rows, err := integrationDB.QueryContext(ctx, `INSERT INTO users(email,password_hash,balance)
  SELECT $1 || i::text || '@example.com', 'test', 100000 FROM generate_series(1,$2::int) i RETURNING id`, prefix, users)
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		userIDs = append(userIDs, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	rows, err = integrationDB.QueryContext(ctx, `INSERT INTO api_keys(user_id,key,name,quota,rate_limit_5h,rate_limit_1d,rate_limit_7d)
  SELECT id, $1 || ordinal::text, 'billing-perf', 100000,100000,100000,100000 FROM unnest($2::bigint[]) WITH ORDINALITY AS u(id,ordinal) RETURNING id`, prefix, pq.Array(userIDs))
	require.NoError(t, err)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		keyIDs = append(keyIDs, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, keyIDs, users)
	var padding strings.Builder
	for i := 0; padding.Len() < size; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprint(i)))
		padding.WriteString(hex.EncodeToString(sum[:]))
	}
	for i := 0; i < accounts; i++ {
		if layout, _ := ctx.Value(billingBenchmarkLayoutKey{}).(string); layout == "colliding" {
			// Simulate a sparse set of active accounts, not consecutive fixture IDs.
			var id int64
			for {
				require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT nextval(pg_get_serial_sequence('accounts','id'))`).Scan(&id))
				if (id+1)%billingBatchWorkers == 0 {
					break
				}
			}
		}
		account := mustCreateAccount(t, testEntClient(t), &service.Account{Name: prefix, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Extra: map[string]any{"quota_limit": 100000, "quota_daily_limit": 100000, "quota_weekly_limit": 100000, "padding": padding.String()}})
		accountIDs = append(accountIDs, account.ID)
	}
	repo := &usageBillingRepository{db: integrationDB}
	var queue *UsageBillingBatchRepository
	if len(automatic) > 0 && automatic[0] {
		queue = NewUsageBillingBatchRepository(repo)
		defer queue.Stop()
	}
	workers := 32
	if len(automatic) > 0 {
		workers = 128
	}

	// Reserve a connection so lock observation never competes with writer slots.
	observer, err := integrationDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = observer.Close() }()
	stop, observed := make(chan struct{}), make(chan struct{})
	var lockSamples, blockedSamples, samples int
	var sampleErr error
	go func() {
		defer close(observed)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				var locks, blocked int
				err := observer.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE wait_event_type='Lock'),
     count(*) FILTER (WHERE cardinality(pg_blocking_pids(pid))>0)
     FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()`).Scan(&locks, &blocked)
				if err != nil {
					sampleErr = err
					return
				}
				samples++
				lockSamples += locks
				blockedSamples += blocked
			}
		}
	}()
	timings := make([]billingWriteTiming, operations)
	jobs := make(chan int, operations)
	start := time.Now()
	poolBefore := integrationDB.Stats()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				scheduled := start
				if paced {
					scheduled = start.Add(time.Duration(i) * 600 * time.Microsecond)
				} // 100k RPM.
				cmd := &service.UsageBillingCommand{RequestID: fmt.Sprintf("%s-%d", prefix, i), APIKeyID: keyIDs[i%users], UserID: userIDs[i%users], AccountID: accountIDs[i%accounts],
					AccountType: service.AccountTypeAPIKey, BalanceCost: 0.01, APIKeyQuotaCost: 0.01, APIKeyRateLimitCost: 0.02, AccountQuotaCost: 0.01}
				if len(automatic) == 0 {
					timings[i] = measureBillingWrite(ctx, repo, cmd, combined)
				} else {
					var result *service.UsageBillingApplyResult
					var err error
					if queue != nil {
						result, err = queue.Apply(ctx, cmd)
					} else {
						result, err = repo.apply(ctx, cmd, false)
					}
					if err == nil && (result == nil || !result.Applied) {
						err = fmt.Errorf("operation not applied")
					}
					timings[i].err = err
				}
				timings[i].total = time.Since(scheduled)
			}
		}()
	}
	for i := 0; i < operations; i++ {
		if paced {
			if delay := time.Until(start.Add(time.Duration(i) * 600 * time.Microsecond)); delay > 0 {
				time.Sleep(delay)
			}
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	poolAfter := integrationDB.Stats()
	close(stop)
	<-observed
	require.NoError(t, sampleErr)
	for _, timing := range timings {
		require.NoError(t, timing.err)
	}
	var claims int
	var deducted, used, windowUsed, accountUsed float64
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT count(*) FROM usage_billing_dedup WHERE api_key_id=ANY($1)", pq.Array(keyIDs)).Scan(&claims))
	require.Equal(t, operations, claims)
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT sum(100000-balance) FROM users WHERE id=ANY($1)", pq.Array(userIDs)).Scan(&deducted))
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT sum(quota_used),sum(usage_5h) FROM api_keys WHERE id=ANY($1)", pq.Array(keyIDs)).Scan(&used, &windowUsed))
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT sum((extra->>'quota_used')::numeric) FROM accounts WHERE id=ANY($1)", pq.Array(accountIDs)).Scan(&accountUsed))
	require.InDelta(t, float64(operations)*0.01, deducted, 1e-6)
	require.InDelta(t, float64(operations)*0.01, used, 1e-6)
	require.InDelta(t, float64(operations)*0.02, windowUsed, 1e-6)
	require.InDelta(t, float64(operations)*0.01, accountUsed, 1e-6)
	report := map[string]any{"scenario": t.Name(), "operations": operations, "elapsed_ms": float64(elapsed) / float64(time.Millisecond),
		"pool_wait_count": poolAfter.WaitCount - poolBefore.WaitCount, "pool_wait_ms": float64(poolAfter.WaitDuration-poolBefore.WaitDuration) / float64(time.Millisecond),
		"lock_wait_samples": lockSamples, "blocked_samples": blockedSamples, "samples": samples}
	for name, extract := range map[string]func(billingWriteTiming) time.Duration{
		"total": func(v billingWriteTiming) time.Duration { return v.total }, "begin": func(v billingWriteTiming) time.Duration { return v.begin },
		"dedup_and_archive": func(v billingWriteTiming) time.Duration { return v.claim }, "balance": func(v billingWriteTiming) time.Duration { return v.balance },
		"key": func(v billingWriteTiming) time.Duration { return v.key }, "account": func(v billingWriteTiming) time.Duration { return v.account }, "commit": func(v billingWriteTiming) time.Duration { return v.commit },
	} {
		values := make([]time.Duration, len(timings))
		var total time.Duration
		for i, v := range timings {
			values[i] = extract(v)
			total += values[i]
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		report[name+"_p99_ms"] = float64(values[(len(values)-1)*99/100]) / float64(time.Millisecond)
		report[name+"_avg_ms"] = float64(total) / float64(len(values)) / float64(time.Millisecond)
	}
	if len(automatic) > 0 {
		shards := make([]int, billingBatchWorkers)
		for _, id := range accountIDs {
			shards[id%billingBatchWorkers]++
		}
		report["account_modulo_shards"] = shards
		report["transactions"] = int64(operations)
		if queue != nil {
			assigned := make([]int, billingBatchWorkers)
			for _, id := range accountIDs {
				assigned[queue.accountShard(id)]++
			}
			report["assigned_account_shards"] = assigned
			report["transactions"] = queue.transactions.Load()
			report["batches"] = queue.batches.Load()
			report["batched_operations"] = queue.batchedOperations.Load()
			report["max_queue_age_ms"] = float64(queue.maxQueueAgeNanos.Load()) / float64(time.Millisecond)
		}
		data, err := json.Marshal(report)
		require.NoError(t, err)
		label := "BILLING_BATCH_RESULT "
		if ctx.Value(billingBenchmarkLayoutKey{}) != nil {
			label = "BILLING_SHARD_RESULT "
		}
		t.Log(label + string(data))
		return
	}
	data, err := json.Marshal(report)
	require.NoError(t, err)
	t.Log("BILLING_WRITE_RESULT " + string(data))
}

// Both variants use the same repository SQL and ordering; only the Key stage differs.
func measureBillingWrite(ctx context.Context, repo *usageBillingRepository, cmd *service.UsageBillingCommand, combined bool) (v billingWriteTiming) {
	cmd.Normalize()
	start := time.Now()
	tx, err := repo.db.BeginTx(ctx, nil)
	v.begin = time.Since(start)
	if err != nil {
		v.err = err
		return
	}
	defer func() { _ = tx.Rollback() }()
	start = time.Now()
	applied, err := repo.claimUsageBillingKey(ctx, tx, cmd)
	v.claim = time.Since(start)
	if err != nil || !applied {
		v.err = fmt.Errorf("claim applied=%t: %v", applied, err)
		return
	}
	start = time.Now()
	_, _, err = deductUsageBillingBalance(ctx, tx, cmd.UserID, cmd.BalanceCost)
	v.balance = time.Since(start)
	if err != nil {
		v.err = err
		return
	}
	start = time.Now()
	if combined {
		_, err = incrementUsageBillingAPIKeyQuotaAndRateLimit(ctx, tx, cmd.APIKeyID, cmd.APIKeyQuotaCost, cmd.APIKeyRateLimitCost)
	} else {
		_, err = incrementUsageBillingAPIKeyQuota(ctx, tx, cmd.APIKeyID, cmd.APIKeyQuotaCost)
		if err == nil {
			err = incrementUsageBillingAPIKeyRateLimit(ctx, tx, cmd.APIKeyID, cmd.APIKeyRateLimitCost)
		}
	}
	v.key = time.Since(start)
	if err != nil {
		v.err = err
		return
	}
	start = time.Now()
	_, err = incrementUsageBillingAccountQuota(ctx, tx, cmd.AccountID, cmd.AccountQuotaCost)
	v.account = time.Since(start)
	if err != nil {
		v.err = err
		return
	}
	start = time.Now()
	v.err = tx.Commit()
	v.commit = time.Since(start)
	return
}

// Both modes use 128 callers and 32 writer connections. All fixture
// rows and triggers are in TestMain's disposable database, never an external DSN.
func TestBillingBatchPerformance(t *testing.T) {
	if os.Getenv("SUB2API_BILLING_WRITE_BENCH") != "1" {
		t.Skip("GitHub-only opt-in billing comparison")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	original := integrationDB.Stats().MaxOpenConnections
	integrationDB.SetMaxOpenConns(33)
	integrationDB.SetMaxIdleConns(33)
	defer func() { integrationDB.SetMaxIdleConns(2); integrationDB.SetMaxOpenConns(original) }()
	for _, users := range []int{500, 5000} {
		for _, accounts := range []int{1, 5, 20} {
			for _, paced := range []bool{false, true} {
				order := []bool{false, true}
				if paced {
					order = []bool{true, false}
				}
				for _, automatic := range order {
					t.Run(fmt.Sprintf("users=%d/accounts=%d/paced=%t/automatic=%t", users, accounts, paced, automatic), func(t *testing.T) { runBillingWriteComparison(t, ctx, users, accounts, 8192, paced, true, automatic) })
				}
			}
		}
	}
}

func TestBillingBatchShardPerformance(t *testing.T) {
	if os.Getenv("SUB2API_BILLING_WRITE_BENCH") != "1" {
		t.Skip("opt-in billing diagnostics")
	}
	original := integrationDB.Stats().MaxOpenConnections
	integrationDB.SetMaxOpenConns(33)
	integrationDB.SetMaxIdleConns(33)
	defer func() { integrationDB.SetMaxIdleConns(2); integrationDB.SetMaxOpenConns(original) }()
	for repeat := 0; repeat < 3; repeat++ {
		for _, accounts := range []int{5, 20} {
			for _, layout := range []string{"consecutive", "colliding"} {
				t.Run(fmt.Sprintf("repeat=%d/accounts=%d/layout=%s", repeat, accounts, layout), func(t *testing.T) {
					ctx := context.WithValue(context.Background(), billingBenchmarkLayoutKey{}, layout)
					runBillingWriteComparison(t, ctx, 500, accounts, 8192, false, true, true)
				})
			}
		}
	}
}
