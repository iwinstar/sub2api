package repository

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

// Probe only existing rows, so deleted subjects are not mistaken for locks.
// Each pass rolls back before the 50ms gap; no probe locks survive the call.
func (r *usageBillingRepository) probeBillingLocks(ctx context.Context, cmds []*service.UsageBillingCommand) ([]billingSubject, error) {
	set := make(map[billingSubject]bool)
	for _, cmd := range cmds {
		for _, subject := range billingSubjects(cmd) {
			set[subject] = true
		}
	}
	first, err := r.probeBillingLocksOnce(ctx, set)
	if err != nil || len(first) == 0 {
		return nil, err
	}
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	// Only rows skipped twice count as blocked. A new lock in the second
	// snapshot cannot turn a healthy row from the first snapshot into a ban.
	second, err := r.probeBillingLocksOnce(ctx, first)
	if err != nil {
		return nil, err
	}
	blocked := make([]billingSubject, 0, len(second))
	for subject := range second {
		blocked = append(blocked, subject)
	}
	return blocked, nil
}

func (r *usageBillingRepository) probeBillingLocksOnce(ctx context.Context, subjects map[billingSubject]bool) (map[billingSubject]bool, error) {
	blocked := make(map[billingSubject]bool)
	if len(subjects) == 0 {
		return blocked, nil
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	// SKIP LOCKED skips row locks, not relation locks. Keep the probe short
	// even when DDL is pending; the outer context also bounds pool admission.
	if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout = '100ms'`); err != nil {
		return nil, err
	}
	for _, resource := range []struct {
		kind  byte
		table string
	}{{'s', "user_subscriptions"}, {'u', "users"}, {'k', "api_keys"}, {'a', "accounts"}} {
		var ids []int64
		for subject := range subjects {
			if subject.kind == resource.kind {
				ids = append(ids, subject.id)
			}
		}
		if len(ids) == 0 {
			continue
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		// The unlocked and existing sets use the same statement snapshot. The
		// table name comes only from the fixed list above, never from a request.
		rows, err := tx.QueryContext(ctx, `WITH unlocked AS MATERIALIZED (
 SELECT id FROM `+resource.table+` WHERE id=ANY($1::bigint[]) AND deleted_at IS NULL
 ORDER BY id FOR NO KEY UPDATE SKIP LOCKED
)
SELECT t.id FROM `+resource.table+` t
WHERE t.id=ANY($1::bigint[]) AND t.deleted_at IS NULL
AND NOT EXISTS (SELECT 1 FROM unlocked u WHERE u.id=t.id)`, pq.Array(ids))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				_ = rows.Close()
				return nil, err
			}
			blocked[billingSubject{resource.kind, id}] = true
		}
		if err = closeBillingRows(rows); err != nil {
			return nil, err
		}
	}
	if err = tx.Rollback(); err != nil {
		return nil, err
	}
	return blocked, nil
}

func (r *UsageBillingBatchRepository) accountGateIndex(cmd *service.UsageBillingCommand) int {
	return int(r.accountOrdinal(cmd.AccountID) % uint64(len(r.accountGates)))
}

func (r *UsageBillingBatchRepository) accountGateIndexes(batch []*usageBillingBatchItem) []int {
	set := make(map[int]bool)
	for _, item := range batch {
		if billingAccountCost(item.cmd) > 0 {
			set[r.accountGateIndex(item.cmd)] = true
		}
	}
	indexes := make([]int, 0, len(set))
	for index := range set {
		indexes = append(indexes, index)
	}
	// Sort physical gate IDs, not account IDs: distinct accounts may hash to
	// one gate, and every multi-gate holder must acquire in the same order.
	sort.Ints(indexes)
	return indexes
}

func (r *UsageBillingBatchRepository) tryAccountGates(batch []*usageBillingBatchItem) (ready, waiting []*usageBillingBatchItem, release func()) {
	acquired := make(map[int]bool)
	for _, index := range r.accountGateIndexes(batch) {
		select {
		case r.accountGates[index] <- struct{}{}:
			acquired[index] = true
		default:
		}
	}
	for _, item := range batch {
		if billingAccountCost(item.cmd) > 0 && !acquired[r.accountGateIndex(item.cmd)] {
			waiting = append(waiting, item)
		} else {
			ready = append(ready, item)
		}
	}
	return ready, waiting, func() {
		for index := range acquired {
			<-r.accountGates[index]
		}
	}
}
