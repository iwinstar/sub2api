//go:build integration

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Execute both implementations against the same transaction timestamp and
// initial rows. This checks every per-operation snapshot, not only final sums.
func TestBillingBatchEffectsMatchSingle(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 1})
	keys := []int64{}
	for i := 0; i < 3; i++ {
		k := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "batch", Quota: 1})
		keys = append(keys, k.ID)
	}
	account := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Extra: map[string]any{"quota_limit": 1, "quota_daily_limit": 1, "quota_weekly_limit": 1}})
	repo := &usageBillingRepository{db: integrationDB}
	for _, window := range []string{"NULL", "NOW()", "NOW()-INTERVAL '8 days'", "NOW()-INTERVAL '5 hours'"} {
		t.Run(window, func(t *testing.T) {
			tx, err := integrationDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = tx.ExecContext(ctx, `UPDATE api_keys SET usage_5h=0.2,usage_1d=0.3,usage_7d=0.4,window_5h_start=`+window+`,window_1d_start=`+window+`,window_7d_start=`+window+` WHERE id=ANY($1::bigint[])`, pq.Array(keys))
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, `UPDATE accounts SET extra=extra||jsonb_build_object('quota_daily_start',to_char(NOW()-INTERVAL '8 days','YYYY-MM-DD"T"HH24:MI:SS"Z"'),'quota_weekly_start',to_char(NOW()-INTERVAL '8 days','YYYY-MM-DD"T"HH24:MI:SS"Z"'),'quota_daily_used',5,'quota_weekly_used',6) WHERE id=$1`, account.ID)
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "SAVEPOINT baseline")
			require.NoError(t, err)
			var cmds []*service.UsageBillingCommand
			for i := 0; i < 12; i++ {
				cmd := &service.UsageBillingCommand{RequestID: uuid.NewString(), UserID: user.ID, APIKeyID: keys[i%3], BalanceCost: 0.20000001, AccountID: account.ID, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: 0.20000001}
				if i%3 != 1 {
					cmd.APIKeyQuotaCost = 0.40000001
				}
				if i%3 != 0 {
					cmd.APIKeyRateLimitCost = 0.20000002
				}
				cmds = append(cmds, cmd)
			}
			expected := make([]billingBatchOutcome, len(cmds))
			for i, c := range cmds {
				expected[i].value = &service.UsageBillingApplyResult{Applied: true}
				require.NoError(t, repo.applyUsageBillingEffects(ctx, tx, c, expected[i].value))
			}
			var keyState, accountState string
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT extra::text FROM accounts WHERE id=$1`, account.ID).Scan(&accountState))
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(k) ORDER BY id)::text FROM api_keys k WHERE id=ANY($1::bigint[])`, pq.Array(keys)).Scan(&keyState))
			_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT baseline")
			require.NoError(t, err)
			actual := make([]billingBatchOutcome, len(cmds))
			for i := range actual {
				actual[i].value = &service.UsageBillingApplyResult{Applied: true}
			}
			// Normally ApplyBatch acquires these locks first. Here one transaction owns
			// both runs; billingBatchEffects is tested directly to freeze NOW().
			require.NoError(t, billingBatchEffects(ctx, tx, cmds, actual))
			for i := range expected {
				require.Equal(t, expected[i].value, actual[i].value, "operation %d", i)
			}
			var actualKeys string
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(k) ORDER BY id)::text FROM api_keys k WHERE id=ANY($1::bigint[])`, pq.Array(keys)).Scan(&actualKeys))
			require.JSONEq(t, keyState, actualKeys)
			var actualAccount string
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT extra::text FROM accounts WHERE id=$1`, account.ID).Scan(&actualAccount))
			require.JSONEq(t, accountState, actualAccount)
			var events int
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM scheduler_outbox WHERE account_id=$1`, account.ID).Scan(&events))
			require.Positive(t, events)
		})
	}
}

func TestBillingBatchDedupAndInvalidSubjects(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := &usageBillingRepository{db: integrationDB}
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 10})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "batch"})
	command := func() *service.UsageBillingCommand {
		return &service.UsageBillingCommand{RequestID: uuid.NewString(), UserID: user.ID, APIKeyID: key.ID, BalanceCost: 1}
	}
	first := command()
	duplicate := *first
	conflict := *first
	conflict.BalanceCost = 2
	missingUser := command()
	missingUser.UserID = -1
	missingAccount := command()
	missingAccount.AccountID = -1
	missingAccount.AccountType = service.AccountTypeAPIKey
	missingAccount.AccountQuotaCost = 1
	deletedKey := command()
	deletedKey.APIKeyID = -1
	deletedKey.APIKeyRateLimitCost = 3
	archive := command()
	archive.Normalize()
	_, err := integrationDB.ExecContext(ctx, `INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at) VALUES($1,$2,$3,NOW())`, archive.RequestID, archive.APIKeyID, archive.RequestFingerprint)
	require.NoError(t, err)
	cmds := []*service.UsageBillingCommand{first, &duplicate, &conflict, missingUser, missingAccount, deletedKey, archive}
	results, err := repo.ApplyBatch(ctx, cmds)
	require.NoError(t, err)
	require.True(t, results[0].value.Applied)
	require.False(t, results[1].value.Applied)
	require.ErrorIs(t, results[2].err, service.ErrUsageBillingRequestConflict)
	require.ErrorIs(t, results[3].err, service.ErrUserNotFound)
	require.ErrorIs(t, results[4].err, service.ErrAccountNotFound)
	require.True(t, results[5].value.Applied)
	require.False(t, results[6].value.Applied)
	require.Equal(t, 9.0, *results[0].value.NewBalance)
	require.Equal(t, 8.0, *results[5].value.NewBalance)
	for _, cmd := range []*service.UsageBillingCommand{missingUser, missingAccount, archive} {
		var n int
		require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID).Scan(&n))
		require.Zero(t, n)
	}
	replay, err := repo.ApplyBatch(ctx, cmds)
	require.NoError(t, err)
	for _, r := range replay {
		require.False(t, r.value.Applied)
	}
}

func TestBillingBatchLockTimeoutAllowsHealthyUser(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	base := &usageBillingRepository{db: integrationDB}
	var users []int64
	for i := 0; i < 2; i++ {
		u := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 10})
		users = append(users, u.ID)
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, users[0])
	require.NoError(t, err)
	r := newUsageBillingBatchRepository(base, 1, 8)
	defer r.Stop()
	done := make(chan error, 1)
	blockedID := uuid.NewString()
	go func() {
		_, e := r.Apply(ctx, &service.UsageBillingCommand{RequestID: blockedID, APIKeyID: -1, UserID: users[0], BalanceCost: 1})
		done <- e
	}()
	// Wait until PostgreSQL reports an actual blocked statement, not a timing guess.
	require.Eventually(t, func() bool {
		var n int
		e := integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&n)
		return e == nil && n > 0
	}, 3*time.Second, 10*time.Millisecond)
	healthy, err := r.Apply(ctx, &service.UsageBillingCommand{RequestID: uuid.NewString(), APIKeyID: -2, UserID: users[1], BalanceCost: 1})
	require.NoError(t, err)
	require.True(t, healthy.Applied)
	require.Error(t, <-done)
	var n int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, blockedID).Scan(&n))
	require.Zero(t, n)
}

func TestBillingBatchProbeIdentifiesOnlyLockedExistingRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 10})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "probe"})
	account := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey})
	base := &usageBillingRepository{db: integrationDB}
	cmd := &service.UsageBillingCommand{UserID: user.ID, BalanceCost: 1, APIKeyID: key.ID, APIKeyQuotaCost: 1, AccountID: account.ID, AccountQuotaCost: 1, AccountType: service.AccountTypeAPIKey}
	for _, subject := range []billingSubject{{'u', user.ID}, {'k', key.ID}, {'a', account.ID}} {
		table := map[byte]string{'u': "users", 'k': "api_keys", 'a': "accounts"}[subject.kind]
		t.Run(table, func(t *testing.T) {
			tx, err := integrationDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = tx.ExecContext(ctx, `SELECT id FROM `+table+` WHERE id=$1 FOR UPDATE`, subject.id)
			require.NoError(t, err)
			blocked, err := base.probeBillingLocks(ctx, []*service.UsageBillingCommand{cmd})
			require.NoError(t, err)
			require.Equal(t, []billingSubject{subject}, blocked)
			require.NoError(t, tx.Rollback())
			blocked, err = base.probeBillingLocks(ctx, []*service.UsageBillingCommand{cmd})
			require.NoError(t, err)
			require.Empty(t, blocked)
		})
	}
	// A deleted key does not block settlement and must not acquire a lock marker.
	_, err := integrationDB.ExecContext(ctx, `UPDATE api_keys SET deleted_at=NOW() WHERE id=$1`, key.ID)
	require.NoError(t, err)
	blocked, err := base.probeBillingLocks(ctx, []*service.UsageBillingCommand{cmd})
	require.NoError(t, err)
	require.Empty(t, blocked)
}

func TestBillingBatchAndProbeAllowUsageLogKeyShare(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 10})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "key-share", Quota: 10})
	account := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey})
	group := mustCreateGroup(t, client, &service.Group{Name: uuid.NewString(), Platform: service.PlatformAnthropic, SubscriptionType: service.SubscriptionTypeSubscription})
	sub := mustCreateSubscription(t, client, &service.UserSubscription{UserID: user.ID, GroupID: group.ID})
	base := &usageBillingRepository{db: integrationDB}
	cmds := []*service.UsageBillingCommand{
		{RequestID: uuid.NewString(), UserID: user.ID, BalanceCost: 1, APIKeyID: key.ID, APIKeyQuotaCost: 1, AccountID: account.ID, AccountQuotaCost: 1, AccountType: service.AccountTypeAPIKey},
		{RequestID: uuid.NewString(), SubscriptionID: &sub.ID, SubscriptionCost: 1, APIKeyID: key.ID, APIKeyRateLimitCost: 1},
	}
	// Hold exactly the parent-row locks acquired by usage_logs foreign keys.
	// They remain held until after both ApplyBatch and the probe have returned.
	logTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = logTx.Rollback() }()
	for _, parent := range []struct {
		table string
		id    int64
	}{{"users", user.ID}, {"api_keys", key.ID}, {"accounts", account.ID}, {"user_subscriptions", sub.ID}} {
		_, err = logTx.ExecContext(ctx, `SELECT id FROM `+parent.table+` WHERE id=$1 FOR KEY SHARE`, parent.id)
		require.NoError(t, err)
	}
	applyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out, err := base.ApplyBatch(applyCtx, cmds)
	require.NoError(t, err, "billing must complete before the KEY SHARE holder commits")
	for _, result := range out {
		require.NoError(t, result.err)
		require.True(t, result.value.Applied)
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, time.Second)
	defer cancelProbe()
	blocked, err := base.probeBillingLocks(probeCtx, cmds)
	require.NoError(t, err)
	require.Empty(t, blocked, "usage log parent locks must not trigger isolation")
	// The deferred last-used update must be compatible with the same locks.
	accountRepo := &accountRepository{sql: integrationDB, optimizedBillingWrites: true}
	updateCtx, cancelUpdate := context.WithTimeout(ctx, 2*time.Second)
	defer cancelUpdate()
	require.NoError(t, accountRepo.BatchUpdateLastUsed(updateCtx, map[int64]time.Time{account.ID: time.Now()}))
}

func TestBillingBatchArchiveFreshSnapshot(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := &usageBillingRepository{db: integrationDB}
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 10})
	cmd := &service.UsageBillingCommand{RequestID: uuid.NewString(), APIKeyID: -99, UserID: user.ID, BalanceCost: 1}
	first, err := repo.Apply(ctx, cmd)
	require.NoError(t, err)
	require.True(t, first.Applied)
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at) SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `DELETE FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		out, e := repo.ApplyBatch(ctx, []*service.UsageBillingCommand{cmd})
		if e == nil && out[0].value.Applied {
			e = errors.New("archive replay charged again")
		}
		done <- e
	}()
	require.Eventually(t, func() bool {
		var n int
		e := integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&n)
		return e == nil && n > 0
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, tx.Commit())
	require.NoError(t, <-done)
	var balance float64
	var claims int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balance))
	require.Equal(t, 9.0, balance)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID).Scan(&claims))
	require.Zero(t, claims)
}

func TestBillingBatchSubscriptionAndDeletionRollback(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := &usageBillingRepository{db: integrationDB}
	for _, deleted := range []string{"none", "account", "group"} {
		t.Run(deleted, func(t *testing.T) {
			user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 10})
			group := mustCreateGroup(t, client, &service.Group{Name: uuid.NewString(), Platform: service.PlatformAnthropic, SubscriptionType: service.SubscriptionTypeSubscription})
			sub := mustCreateSubscription(t, client, &service.UserSubscription{UserID: user.ID, GroupID: group.ID})
			account := mustCreateAccount(t, client, &service.Account{Name: uuid.NewString(), Type: service.AccountTypeAPIKey})
			cmds := []*service.UsageBillingCommand{
				{RequestID: uuid.NewString(), APIKeyID: -1, SubscriptionID: &sub.ID, SubscriptionCost: 0.10000001, AccountID: account.ID, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: 0.10000001},
				{RequestID: uuid.NewString(), APIKeyID: -2, UserID: user.ID, BalanceCost: 0.10000001},
				{RequestID: uuid.NewString(), APIKeyID: -3, SubscriptionID: &sub.ID, SubscriptionCost: 0.10000001},
			}
			if deleted == "none" {
				out, err := repo.ApplyBatch(ctx, cmds)
				require.NoError(t, err)
				for _, v := range out {
					require.True(t, v.value.Applied)
				}
				var daily, weekly, monthly float64
				require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT daily_usage_usd,weekly_usage_usd,monthly_usage_usd FROM user_subscriptions WHERE id=$1`, sub.ID).Scan(&daily, &weekly, &monthly))
				require.Equal(t, 0.20000002, daily)
				require.Equal(t, daily, weekly)
				require.Equal(t, daily, monthly)
				return
			}
			blocker, err := integrationDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer blocker.Rollback()
			_, err = blocker.ExecContext(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, user.ID)
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { _, e := repo.ApplyBatch(ctx, cmds); done <- e }()
			require.Eventually(t, func() bool {
				var n int
				e := integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&n)
				return e == nil && n > 0
			}, time.Second, 5*time.Millisecond)
			table, id := "accounts", account.ID
			if deleted == "group" {
				table, id = "groups", group.ID
			}
			_, err = integrationDB.ExecContext(ctx, `UPDATE `+table+` SET deleted_at=NOW() WHERE id=$1`, id)
			require.NoError(t, err)
			require.NoError(t, blocker.Commit())
			err = <-done
			if deleted == "group" {
				require.ErrorIs(t, err, service.ErrSubscriptionNotFound)
			} else {
				require.ErrorIs(t, err, service.ErrAccountNotFound)
			}
			var balance float64
			require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balance))
			require.Equal(t, 10.0, balance)
			for _, c := range cmds {
				var n int
				require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, c.RequestID).Scan(&n))
				require.Zero(t, n)
			}
		})
	}
}
