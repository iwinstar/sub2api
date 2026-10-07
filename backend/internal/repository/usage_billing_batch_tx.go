package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
)

const billingLockTimeoutSQL = `SET LOCAL lock_timeout = '3s'`

// A commit error carries phase information: only an unknown COMMIT outcome
// needs dedup replay confirmation. Query/lock errors never enter that path.
func commitUsageBilling(ctx context.Context, tx *sql.Tx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := tx.Commit()
	if err == nil {
		return nil
	}
	var pg *pq.Error
	if errors.As(err, &pg) && !pg.Fatal() {
		return err
	}
	if errors.Is(err, pq.ErrInFailedTransaction) {
		return err
	}
	// database/sql checks ctx before invoking the driver. We have one owner and
	// have not attempted any earlier Commit on this transaction.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, sql.ErrTxDone) && ctx.Err() != nil) {
		return err
	}
	return fmt.Errorf("%w: %w", service.ErrUsageBillingOutcomeUnknown, err)
}

type billingBatchOutcome struct {
	// pendingCommit includes aliases of a fresh claim, but excludes old duplicates.
	pendingCommit bool
	value         *service.UsageBillingApplyResult
	err           error
}

type billingDedupKey struct {
	request string
	key     int64
}

// ApplyBatch keeps dedup insertion, conflict reads and archive reads in separate
// statements at READ COMMITTED. All amounts are decimal strings, never float sums.
func (r *usageBillingRepository) ApplyBatch(ctx context.Context, cmds []*service.UsageBillingCommand) ([]billingBatchOutcome, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("usage billing repository db is nil")
	}
	out := make([]billingBatchOutcome, len(cmds))
	keys := make([]billingDedupKey, 0, len(cmds))
	positions := make(map[billingDedupKey]int, len(cmds))
	for i, cmd := range cmds {
		out[i].value = &service.UsageBillingApplyResult{}
		if cmd == nil {
			continue
		}
		cmd.Normalize()
		if cmd.RequestID == "" {
			out[i].err = service.ErrUsageBillingRequestIDRequired
			continue
		}
		invalid := false
		for _, amount := range []float64{cmd.BalanceCost, cmd.SubscriptionCost, cmd.APIKeyQuotaCost, cmd.APIKeyRateLimitCost, cmd.AccountQuotaCost} {
			if math.IsNaN(amount) || math.IsInf(amount, 0) {
				invalid = true
				break
			}
		}
		if invalid {
			out[i].err = errors.New("usage billing amount is not finite")
			continue
		}
		key := billingDedupKey{cmd.RequestID, cmd.APIKeyID}
		if first, ok := positions[key]; ok {
			if strings.TrimSpace(cmd.RequestFingerprint) != strings.TrimSpace(cmds[first].RequestFingerprint) {
				out[i].err = service.ErrUsageBillingRequestConflict
			}
			continue
		}
		positions[key] = i
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return out, nil
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].request == keys[j].request {
			return keys[i].key < keys[j].key
		}
		return keys[i].request < keys[j].request
	})
	requests, keyIDs, fingerprints := make([]string, len(keys)), make([]int64, len(keys)), make([]string, len(keys))
	for i, k := range keys {
		requests[i], keyIDs[i], fingerprints[i] = k.request, k.key, cmds[positions[k]].RequestFingerprint
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, billingLockTimeoutSQL); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `INSERT INTO usage_billing_dedup (request_id,api_key_id,request_fingerprint)
 SELECT request_id,api_key_id,fingerprint FROM unnest($1::text[],$2::bigint[],$3::text[]) AS d(request_id,api_key_id,fingerprint)
 ORDER BY request_id,api_key_id ON CONFLICT (request_id,api_key_id) DO NOTHING RETURNING id,request_id,api_key_id`, pq.Array(requests), pq.Array(keyIDs), pq.Array(fingerprints))
	if err != nil {
		return nil, err
	}
	claimed := make(map[billingDedupKey]int64)
	for rows.Next() {
		var id int64
		var k billingDedupKey
		if err = rows.Scan(&id, &k.request, &k.key); err != nil {
			_ = rows.Close()
			return nil, err
		}
		claimed[k] = id
	}
	if err = closeBillingRows(rows); err != nil {
		return nil, err
	}
	// Independent snapshots are essential when INSERT waited for another transaction.
	for _, table := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		rows, err = tx.QueryContext(ctx, `SELECT t.request_id,t.api_key_id,t.request_fingerprint FROM `+table+` t
  JOIN unnest($1::text[],$2::bigint[]) d(request_id,api_key_id) USING(request_id,api_key_id)`, pq.Array(requests), pq.Array(keyIDs))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var k billingDedupKey
			var fingerprint string
			if err = rows.Scan(&k.request, &k.key, &fingerprint); err != nil {
				_ = rows.Close()
				return nil, err
			}
			i := positions[k]
			if table == "usage_billing_dedup_archive" || claimed[k] == 0 {
				if strings.TrimSpace(fingerprint) != strings.TrimSpace(cmds[i].RequestFingerprint) {
					out[i].err = service.ErrUsageBillingRequestConflict
				}
				out[i].value.Applied = false
			} else {
				out[i].value.Applied = true
			}
		}
		if err = closeBillingRows(rows); err != nil {
			return nil, err
		}
	}
	// Precheck accounts without locking: accounts are always locked last.
	accounts, err := billingExistingIDs(ctx, tx, "accounts", billingIDs(cmds, out, "accounts"), false)
	if err != nil {
		return nil, err
	}
	for i, c := range cmds {
		if out[i].value.Applied && billingAccountCost(c) > 0 && !accounts[c.AccountID] {
			out[i].value.Applied = false
			out[i].err = service.ErrAccountNotFound
		}
	}
	// Explicit sorted row locks precede updates; subscription joins lock only us.
	for _, table := range []string{"user_subscriptions", "users", "api_keys"} {
		ids := billingIDs(cmds, out, table)
		exists, e := billingExistingIDs(ctx, tx, table, ids, true)
		if e != nil {
			return nil, e
		}
		for i, c := range cmds {
			if !out[i].value.Applied {
				continue
			}
			if table == "user_subscriptions" && c.SubscriptionCost > 0 && c.SubscriptionID != nil && !exists[*c.SubscriptionID] {
				out[i].value.Applied = false
				out[i].err = service.ErrSubscriptionNotFound
			}
			if table == "users" && c.BalanceCost > 0 && !exists[c.UserID] {
				out[i].value.Applied = false
				out[i].err = service.ErrUserNotFound
			}
			// Deleted keys intentionally do not invalidate settlement.
		}
	}
	var release []int64
	for k, id := range claimed {
		if !out[positions[k]].value.Applied {
			release = append(release, id)
		}
	}
	if len(release) > 0 {
		if _, err = tx.ExecContext(ctx, `DELETE FROM usage_billing_dedup WHERE id=ANY($1::bigint[])`, pq.Array(release)); err != nil {
			return nil, err
		}
	}
	if err = billingBatchEffects(ctx, tx, cmds, out); err != nil {
		return nil, err
	}
	for i, c := range cmds {
		if c == nil || c.RequestID == "" {
			continue
		}
		first, ok := positions[billingDedupKey{c.RequestID, c.APIKeyID}]
		if !ok || out[i].err != nil {
			continue
		}
		if i != first && out[first].err != nil {
			out[i].err = out[first].err
		}
		out[i].pendingCommit = out[first].value.Applied
	}
	if err = commitUsageBilling(ctx, tx); err != nil {
		return out, err
	}
	return out, nil
}

func closeBillingRows(rows *sql.Rows) error {
	err := rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func billingAccountCost(c *service.UsageBillingCommand) float64 {
	if c != nil && c.AccountQuotaCost > 0 && (strings.EqualFold(c.AccountType, service.AccountTypeAPIKey) || strings.EqualFold(c.AccountType, service.AccountTypeBedrock)) {
		return c.AccountQuotaCost
	}
	return 0
}

func billingIDs(cmds []*service.UsageBillingCommand, out []billingBatchOutcome, table string) []int64 {
	set := map[int64]bool{}
	for i, c := range cmds {
		if !out[i].value.Applied {
			continue
		}
		switch table {
		case "user_subscriptions":
			if c.SubscriptionCost > 0 && c.SubscriptionID != nil {
				set[*c.SubscriptionID] = true
			}
		case "users":
			if c.BalanceCost > 0 {
				set[c.UserID] = true
			}
		case "api_keys":
			if c.APIKeyQuotaCost > 0 || c.APIKeyRateLimitCost > 0 {
				set[c.APIKeyID] = true
			}
		case "accounts":
			if billingAccountCost(c) > 0 {
				set[c.AccountID] = true
			}
		}
	}
	ids := make([]int64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func billingExistingIDs(ctx context.Context, tx *sql.Tx, table string, ids []int64, lock bool) (map[int64]bool, error) {
	found := map[int64]bool{}
	if len(ids) == 0 {
		return found, nil
	}
	query := `SELECT id FROM ` + table + ` WHERE id=ANY($1::bigint[]) AND deleted_at IS NULL ORDER BY id`
	if table == "user_subscriptions" {
		query = `SELECT us.id FROM user_subscriptions us JOIN groups g ON g.id=us.group_id WHERE us.id=ANY($1::bigint[]) AND us.deleted_at IS NULL AND g.deleted_at IS NULL ORDER BY us.id`
	}
	if lock {
		if table == "user_subscriptions" {
			query += ` FOR NO KEY UPDATE OF us`
		} else {
			query += ` FOR NO KEY UPDATE`
		}
	}
	rows, err := tx.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		found[id] = true
	}
	return found, closeBillingRows(rows)
}

// Each entry is a per-subject decimal sum. The stable command order is retained
// separately to reconstruct per-operation balances and threshold crossings.
type billingAmounts map[int64]decimal.Decimal

func (a billingAmounts) add(id int64, v float64) {
	if v > 0 {
		a[id] = a[id].Add(decimal.NewFromFloat(v))
	}
}
func (a billingAmounts) arrays() ([]int64, []string) {
	ids := make([]int64, 0, len(a))
	for id := range a {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	amounts := make([]string, len(ids))
	for i, id := range ids {
		amounts[i] = a[id].String()
	}
	return ids, amounts
}

func billingBatchEffects(ctx context.Context, tx *sql.Tx, cmds []*service.UsageBillingCommand, out []billingBatchOutcome) error {
	subs, users, quota, rate, accounts := billingAmounts{}, billingAmounts{}, billingAmounts{}, billingAmounts{}, billingAmounts{}
	for i, c := range cmds {
		if !out[i].value.Applied {
			continue
		}
		if c.SubscriptionID != nil {
			subs.add(*c.SubscriptionID, c.SubscriptionCost)
		}
		users.add(c.UserID, c.BalanceCost)
		quota.add(c.APIKeyID, c.APIKeyQuotaCost)
		rate.add(c.APIKeyID, c.APIKeyRateLimitCost)
		accounts.add(c.AccountID, billingAccountCost(c))
	}
	if len(subs) > 0 {
		ids, amounts := subs.arrays()
		res, err := tx.ExecContext(ctx, `UPDATE user_subscriptions us SET daily_usage_usd=us.daily_usage_usd+d.amount,weekly_usage_usd=us.weekly_usage_usd+d.amount,monthly_usage_usd=us.monthly_usage_usd+d.amount,updated_at=NOW()
   FROM unnest($1::bigint[],$2::numeric[]) d(id,amount),groups g WHERE us.id=d.id AND us.deleted_at IS NULL AND us.group_id=g.id AND g.deleted_at IS NULL`, pq.Array(ids), pq.Array(amounts))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != int64(len(ids)) {
			return service.ErrSubscriptionNotFound
		}
	}
	if len(users) > 0 {
		balances := map[int64]decimal.Decimal{}
		ids, amounts := users.arrays()
		rows, err := tx.QueryContext(ctx, `UPDATE users u SET balance=u.balance-d.amount,updated_at=NOW() FROM unnest($1::bigint[],$2::numeric[]) d(id,amount) WHERE u.id=d.id AND u.deleted_at IS NULL RETURNING u.id,u.balance`, pq.Array(ids), pq.Array(amounts))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var b decimal.Decimal
			if err = rows.Scan(&id, &b); err != nil {
				_ = rows.Close()
				return err
			}
			balances[id] = b
		}
		if err = closeBillingRows(rows); err != nil {
			return err
		}
		if len(balances) != len(users) {
			return service.ErrUserNotFound
		}
		for i := len(cmds) - 1; i >= 0; i-- {
			c := cmds[i]
			if !out[i].value.Applied || c.BalanceCost <= 0 {
				continue
			}
			b := balances[c.UserID]
			v, _ := b.Float64()
			out[i].value.NewBalance = &v
			out[i].value.BalanceOverdrafted = b.IsNegative()
			balances[c.UserID] = b.Add(decimal.NewFromFloat(c.BalanceCost))
		}
	}
	if err := billingBatchKeys(ctx, tx, cmds, out, quota, rate); err != nil {
		return err
	}
	if len(accounts) > 0 {
		ids, amounts := accounts.arrays()
		found, err := billingExistingIDs(ctx, tx, "accounts", ids, true)
		if err != nil {
			return err
		}
		if len(found) != len(ids) {
			return service.ErrAccountNotFound
		}
		// Reuse the exact original JSONB/window expressions, changing only input and
		// output cardinality. No per-account UPDATE loop or float aggregation.
		query := strings.ReplaceAll(usageBillingAccountQuotaSQL, "$1", "d.amount")
		query = strings.Replace(query, "WHERE id = $2 AND deleted_at IS NULL", `FROM unnest($1::bigint[],$2::numeric[]) d(id,amount) WHERE accounts.id=d.id AND deleted_at IS NULL`, 1)
		query = strings.Replace(query, "RETURNING", "RETURNING accounts.id,", 1)
		rows, err := tx.QueryContext(ctx, query, pq.Array(ids), pq.Array(amounts))
		if err != nil {
			return err
		}
		states := map[int64][6]decimal.Decimal{}
		for rows.Next() {
			var id int64
			var s [6]decimal.Decimal
			if err = rows.Scan(&id, &s[0], &s[1], &s[2], &s[3], &s[4], &s[5]); err != nil {
				_ = rows.Close()
				return err
			}
			states[id] = s
		}
		if err = closeBillingRows(rows); err != nil {
			return err
		}
		if len(states) != len(accounts) {
			return service.ErrAccountNotFound
		}
		for id, s := range states {
			crossed := false
			for _, n := range []int{0, 2, 4} {
				if s[n+1].IsPositive() && s[n].GreaterThanOrEqual(s[n+1]) && s[n].Sub(accounts[id]).LessThan(s[n+1]) {
					crossed = true
				}
			}
			if crossed {
				accountID := id
				if err = enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountChanged, &accountID, nil, nil); err != nil {
					return err
				}
			}
		}
		for i := len(cmds) - 1; i >= 0; i-- {
			c := cmds[i]
			amount := billingAccountCost(c)
			if !out[i].value.Applied || amount <= 0 {
				continue
			}
			s := states[c.AccountID]
			v := [6]float64{}
			for n := range s {
				v[n], _ = s[n].Float64()
			}
			out[i].value.QuotaState = &service.AccountQuotaState{TotalUsed: v[0], TotalLimit: v[1], DailyUsed: v[2], DailyLimit: v[3], WeeklyUsed: v[4], WeeklyLimit: v[5]}
			a := decimal.NewFromFloat(amount)
			s[0] = s[0].Sub(a)
			if s[3].IsPositive() {
				s[2] = s[2].Sub(a)
			}
			if s[5].IsPositive() {
				s[4] = s[4].Sub(a)
			}
			states[c.AccountID] = s
		}
	}
	return nil
}
