package repository

import (
	"context"
	"database/sql"
	"sort"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
)

func billingBatchKeys(ctx context.Context, tx *sql.Tx, cmds []*service.UsageBillingCommand, out []billingBatchOutcome, quota, rate billingAmounts) error {
	ids := make([]int64, 0, len(quota)+len(rate))
	for id := range quota {
		ids = append(ids, id)
	}
	for id := range rate {
		if _, ok := quota[id]; !ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	qa, ra := make([]string, len(ids)), make([]string, len(ids))
	for i, id := range ids {
		qa[i] = quota[id].String()
		ra[i] = rate[id].String()
	}
	rows, err := tx.QueryContext(ctx, `UPDATE api_keys k SET
 quota_used=CASE WHEN d.quota_amt>0 THEN k.quota_used+d.quota_amt ELSE k.quota_used END,
 status=CASE WHEN d.quota_amt>0 AND k.quota>0 AND k.status=$4 AND k.quota_used<k.quota AND k.quota_used+d.quota_amt>=k.quota THEN $5 ELSE k.status END,
 usage_5h=CASE WHEN d.rate_amt>0 THEN CASE WHEN window_5h_start IS NOT NULL AND window_5h_start+INTERVAL '5 hours'<=NOW() THEN d.rate_amt ELSE usage_5h+d.rate_amt END ELSE usage_5h END,
 usage_1d=CASE WHEN d.rate_amt>0 THEN CASE WHEN window_1d_start IS NOT NULL AND window_1d_start+INTERVAL '24 hours'<=NOW() THEN d.rate_amt ELSE usage_1d+d.rate_amt END ELSE usage_1d END,
 usage_7d=CASE WHEN d.rate_amt>0 THEN CASE WHEN window_7d_start IS NOT NULL AND window_7d_start+INTERVAL '7 days'<=NOW() THEN d.rate_amt ELSE usage_7d+d.rate_amt END ELSE usage_7d END,
 window_5h_start=CASE WHEN d.rate_amt>0 AND (window_5h_start IS NULL OR window_5h_start+INTERVAL '5 hours'<=NOW()) THEN NOW() ELSE window_5h_start END,
 window_1d_start=CASE WHEN d.rate_amt>0 AND (window_1d_start IS NULL OR window_1d_start+INTERVAL '24 hours'<=NOW()) THEN date_trunc('day',NOW()) ELSE window_1d_start END,
 window_7d_start=CASE WHEN d.rate_amt>0 AND (window_7d_start IS NULL OR window_7d_start+INTERVAL '7 days'<=NOW()) THEN date_trunc('day',NOW()) ELSE window_7d_start END,
 updated_at=NOW()
 FROM unnest($1::bigint[],$2::numeric[],$3::numeric[]) d(id,quota_amt,rate_amt)
 WHERE k.id=d.id AND k.deleted_at IS NULL RETURNING k.id,k.quota_used,k.quota`, pq.Array(ids), pq.Array(qa), pq.Array(ra), service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted)
	if err != nil {
		return err
	}
	states := map[int64][2]decimal.Decimal{}
	for rows.Next() {
		var id int64
		var s [2]decimal.Decimal
		if err = rows.Scan(&id, &s[0], &s[1]); err != nil {
			_ = rows.Close()
			return err
		}
		states[id] = s
	}
	if err = closeBillingRows(rows); err != nil {
		return err
	}
	for i := len(cmds) - 1; i >= 0; i-- {
		c := cmds[i]
		if !out[i].value.Applied || c.APIKeyQuotaCost <= 0 {
			continue
		}
		s, ok := states[c.APIKeyID]
		if !ok {
			continue
		}
		before := s[0].Sub(decimal.NewFromFloat(c.APIKeyQuotaCost))
		out[i].value.APIKeyQuotaExhausted = s[1].IsPositive() && s[0].GreaterThanOrEqual(s[1]) && before.LessThan(s[1])
		s[0] = before
		states[c.APIKeyID] = s
	}
	return nil
}
