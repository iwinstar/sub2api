//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Compare the old two-statement path with the optimized path at the same NOW(),
// including the real auth invalidation trigger installed by the migration harness.
func TestUsageBillingAPIKeyCombinedMatchesSeparate(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash"})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "combined", Quota: 10})
	for _, window := range []string{"NULL", "NOW()", "NOW() - INTERVAL '8 days'", "NOW() - INTERVAL '5 hours'"} {
		for _, amounts := range [][2]float64{{2, 3}, {12, 3}, {0, 3}, {2, 0}, {0, 0}, {-1, 3}, {2, -1}} {
			for _, status := range []string{service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted} {
				t.Run(fmt.Sprintf("%s/%v/%s", window, amounts, status), func(t *testing.T) {
					tx, err := integrationDB.BeginTx(ctx, nil)
					require.NoError(t, err)
					defer func() { _ = tx.Rollback() }()
					_, err = tx.ExecContext(ctx, `UPDATE api_keys SET quota_used = 1, status = $2,
      usage_5h = 4, usage_1d = 5, usage_7d = 6,
      window_5h_start = `+window+`, window_1d_start = `+window+`, window_7d_start = `+window+`
      WHERE id = $1`, key.ID, status)
					require.NoError(t, err)
					_, err = tx.ExecContext(ctx, "SAVEPOINT original")
					require.NoError(t, err)
					_, baselineEvents := billingKeySnapshot(t, ctx, tx, key.ID, key.Key)
					var exhausted bool
					if amounts[0] > 0 {
						exhausted, err = incrementUsageBillingAPIKeyQuota(ctx, tx, key.ID, amounts[0])
						require.NoError(t, err)
					}
					if amounts[1] > 0 {
						require.NoError(t, incrementUsageBillingAPIKeyRateLimit(ctx, tx, key.ID, amounts[1]))
					}
					original, originalEvents := billingKeySnapshot(t, ctx, tx, key.ID, key.Key)
					_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT original")
					require.NoError(t, err)
					result := &service.UsageBillingApplyResult{}
					require.NoError(t, (&usageBillingRepository{optimizedWrites: true}).applyUsageBillingEffects(ctx, tx, &service.UsageBillingCommand{
						APIKeyID: key.ID, APIKeyQuotaCost: amounts[0], APIKeyRateLimitCost: amounts[1],
					}, result))
					combined, combinedEvents := billingKeySnapshot(t, ctx, tx, key.ID, key.Key)
					require.JSONEq(t, original, combined)
					require.Equal(t, exhausted, result.APIKeyQuotaExhausted)
					require.Equal(t, originalEvents, combinedEvents)
					expectedEvents := baselineEvents
					if status == service.StatusAPIKeyActive && amounts[0] >= 9 {
						expectedEvents++
					}
					require.Equal(t, expectedEvents, combinedEvents)
				})
			}
		}
	}
}

func billingKeySnapshot(t *testing.T, ctx context.Context, tx *sql.Tx, keyID int64, rawKey string) (string, int) {
	t.Helper()
	var state string
	var events int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT row_to_json(k)::text FROM api_keys k WHERE id = $1`, keyID).Scan(&state))
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM auth_cache_invalidation_outbox
  WHERE cache_key = encode(sha256(convert_to($1, 'UTF8')), 'hex')`, rawKey).Scan(&events))
	return state, events
}

func TestUsageBillingCombinedRollbackAndMissingKey(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	user := mustCreateUser(t, client, &service.User{Email: uuid.NewString() + "@example.com", PasswordHash: "hash", Balance: 100})
	key := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-" + uuid.NewString(), Name: "rollback", Quota: 1})
	repo := NewUsageBillingRepository(client, integrationDB)
	cmd := &service.UsageBillingCommand{RequestID: uuid.NewString(), UserID: user.ID, APIKeyID: key.ID,
		BalanceCost: 2, APIKeyQuotaCost: 2, APIKeyRateLimitCost: 3,
		AccountID: -1, AccountType: service.AccountTypeAPIKey, AccountQuotaCost: 2}
	_, err := repo.Apply(ctx, cmd)
	require.Error(t, err) // Failure after the Key update must roll back every effect.
	var balance, used float64
	var claims, events int
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT balance FROM users WHERE id = $1", user.ID).Scan(&balance))
	require.Equal(t, float64(100), balance)
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT quota_used FROM api_keys WHERE id = $1", key.ID).Scan(&used))
	require.Zero(t, used)
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT count(*) FROM usage_billing_dedup WHERE request_id = $1", cmd.RequestID).Scan(&claims))
	require.Zero(t, claims)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM auth_cache_invalidation_outbox WHERE cache_key = encode(sha256(convert_to($1, 'UTF8')), 'hex')`, key.Key).Scan(&events))
	require.Zero(t, events)
	_, err = integrationDB.ExecContext(ctx, "UPDATE api_keys SET deleted_at = $2 WHERE id = $1", key.ID, time.Now())
	require.NoError(t, err)
	cmd.AccountQuotaCost = 0
	result, err := repo.Apply(ctx, cmd)
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.False(t, result.APIKeyQuotaExhausted)
	require.Equal(t, float64(98), *result.NewBalance)
}
