package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

func lockBillingDedupMaintenance(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('usage_billing_dedup_maintenance'))`)
	return err
}

// DeleteBillingDedupPage commits a bounded page before publishing scan progress.
// Table names are selected here, never taken from request/config input.
func (r *dashboardAggregationRepository) DeleteBillingDedupPage(ctx context.Context, cursor *service.BillingDedupCursor, cutoff, videoCutoff time.Time, archive, image bool) (service.BillingDedupPageStats, error) {
	var stats service.BillingDedupPageStats
	if (image && cutoff.IsZero()) || (!image && cutoff.IsZero() && videoCutoff.IsZero()) {
		stats.Done = true
		return stats, nil
	}
	// NULL makes the corresponding predicate false, including for ancient rows.
	ordinaryDate := sql.NullTime{Time: cutoff, Valid: !cutoff.IsZero()}
	videoDate := sql.NullTime{Time: videoCutoff, Valid: !videoCutoff.IsZero()}
	db, ok := r.sql.(*sql.DB)
	if !ok {
		return stats, errors.New("billing dedup deletion requires sql.DB")
	}
	table := "usage_billing_dedup"
	if archive {
		table = "usage_billing_dedup_archive"
	}
	next := *cursor
	pageSize := next.PageSize
	maximum := 10000
	if image {
		maximum = 1000
	}
	if pageSize <= 0 || pageSize > maximum {
		pageSize = maximum
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return stats, err
	}
	defer func() { _ = tx.Rollback() }()
	if image {
		if err := lockBillingDedupMaintenance(ctx, tx); err != nil {
			return stats, err
		}
		if !next.Started {
			// Only bytewise collations guarantee that this lexical range covers
			// every batch_image_ prefix. Other collations use the full index.
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(CASE
				WHEN c.oid = 'default'::regcollation THEN (SELECT datlocprovider = 'c' AND datcollate IN ('C', 'POSIX') FROM pg_database WHERE datname = current_database())
				ELSE c.collprovider = 'c' AND c.collcollate IN ('C', 'POSIX') END, false)
				FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation
				WHERE a.attrelid = $1::regclass AND a.attname = 'request_id'`, table).Scan(&next.PrefixRange)
			if err != nil {
				return stats, err
			}
		}
	}
	prefixPredicate := "true"
	if image && next.PrefixRange {
		prefixPredicate = "request_id >= 'batch_image_' AND request_id < 'batch_image`'"
	}
	if !next.Started {
		if !archive && !image {
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(max(id), 0) FROM usage_billing_dedup`).Scan(&next.EndID)
		} else {
			err = tx.QueryRowContext(ctx, `SELECT request_id, api_key_id FROM `+table+` WHERE `+prefixPredicate+` ORDER BY request_id DESC, api_key_id DESC LIMIT 1`).Scan(&next.EndRequestID, &next.EndAPIKeyID)
		}
		if errors.Is(err, sql.ErrNoRows) || (err == nil && !archive && !image && next.EndID == 0) {
			stats.Done = true
			*cursor = service.BillingDedupCursor{PageSize: cursor.PageSize, ConsecutiveFastPages: cursor.ConsecutiveFastPages}
			return stats, nil
		}
		if err != nil {
			return stats, err
		}
		next.Started = true
	}
	if !archive && !image {
		var pageMax sql.NullInt64
		err = tx.QueryRowContext(ctx, `WITH page AS MATERIALIZED (
			SELECT id, created_at, request_id FROM usage_billing_dedup
			WHERE id > $1 AND id <= $2
			  AND NOT starts_with(request_id, 'batch_image_')
			ORDER BY id
			LIMIT $4
		), deleted AS (
			DELETE FROM usage_billing_dedup d USING page p
			WHERE d.id = p.id AND (
			  (NOT starts_with(p.request_id, 'grok-video:') AND $3::timestamptz IS NOT NULL AND p.created_at < $3)
			  OR (starts_with(p.request_id, 'grok-video:') AND $5::timestamptz IS NOT NULL AND p.created_at < $5))
			AND NOT starts_with(p.request_id, 'batch_image_') RETURNING d.id
		) SELECT (SELECT max(id) FROM page), (SELECT count(*) FROM page), (SELECT count(*) FROM deleted)`, next.ID, next.EndID, ordinaryDate, pageSize, videoDate).Scan(&pageMax, &stats.Scanned, &stats.Deleted)
		if err != nil {
			return stats, err
		}
		if pageMax.Valid {
			next.ID = pageMax.Int64
		}
		// Advance through every row in this bounded round, including retained
		// rows. A new round retries rows whose created_at/id order is unusual.
		stats.Done = stats.Scanned < int64(pageSize)
	} else if !image {
		var lastRequest sql.NullString
		var lastKey sql.NullInt64
		err = tx.QueryRowContext(ctx, `WITH page AS MATERIALIZED (
			SELECT request_id, api_key_id, created_at FROM usage_billing_dedup_archive
			WHERE (request_id, api_key_id) > ($1, $2) AND (request_id, api_key_id) <= ($3, $4)
			ORDER BY request_id, api_key_id LIMIT $6
		), deleted AS (
			DELETE FROM usage_billing_dedup_archive d USING page p
			WHERE d.request_id = p.request_id AND d.api_key_id = p.api_key_id
			AND ((NOT starts_with(p.request_id, 'grok-video:') AND p.created_at < $5)
			  OR (starts_with(p.request_id, 'grok-video:') AND p.created_at < $7))
			AND NOT starts_with(p.request_id, 'batch_image_') RETURNING d.request_id
		) SELECT (SELECT request_id FROM page ORDER BY request_id DESC, api_key_id DESC LIMIT 1),
			(SELECT api_key_id FROM page ORDER BY request_id DESC, api_key_id DESC LIMIT 1),
			(SELECT count(*) FROM page), (SELECT count(*) FROM deleted)`, next.RequestID, next.APIKeyID, next.EndRequestID, next.EndAPIKeyID, ordinaryDate, pageSize, videoDate).Scan(&lastRequest, &lastKey, &stats.Scanned, &stats.Deleted)
		if err != nil {
			return stats, err
		}
		if lastRequest.Valid {
			next.RequestID, next.APIKeyID = lastRequest.String, lastKey.Int64
		}
		stats.Done = !lastRequest.Valid || (next.RequestID == next.EndRequestID && next.APIKeyID == next.EndAPIKeyID)
	} else {
		// Unknown collations fall back to a full composite-index scan.
		rows, err := tx.QueryContext(ctx, `SELECT request_id, api_key_id, created_at FROM `+table+`
			WHERE `+prefixPredicate+` AND (request_id, api_key_id) > ($1, $2) AND (request_id, api_key_id) <= ($3, $4)
			ORDER BY request_id, api_key_id LIMIT $5`, next.RequestID, next.APIKeyID, next.EndRequestID, next.EndAPIKeyID, pageSize)
		if err != nil {
			return stats, err
		}
		type candidate struct {
			batch string
			key   int64
		}
		candidates := make([]candidate, 0)
		seen := make(map[candidate]bool)
		for rows.Next() {
			var request string
			var key int64
			var created time.Time
			if err := rows.Scan(&request, &key, &created); err != nil {
				_ = rows.Close()
				return stats, err
			}
			next.RequestID, next.APIKeyID = request, key
			stats.Scanned++
			batch := billingDedupBatchID(request)
			c := candidate{batch, key}
			if batch != "" && created.Before(cutoff) && !seen[c] {
				seen[c] = true
				candidates = append(candidates, c)
				// Bound per-page job/credential lookups as well as scanned rows.
				if len(candidates) >= 100 {
					break
				}
			}
		}
		rowErr := rows.Err()
		_ = rows.Close()
		if rowErr != nil {
			return stats, rowErr
		}
		for _, c := range candidates {
			deleted, retained, anomaly, err := deleteSettledBatchImageDedup(ctx, tx, c.batch, c.key, cutoff)
			if err != nil {
				return stats, err
			}
			stats.Deleted += deleted
			if retained {
				stats.Retained++
			}
			if anomaly {
				stats.Anomalies++
			}
		}
		stats.Done = stats.Scanned == 0 || (next.RequestID == next.EndRequestID && next.APIKeyID == next.EndAPIKeyID)
	}
	if err := tx.Commit(); err != nil {
		return service.BillingDedupPageStats{}, err
	}
	if stats.Done {
		next = service.BillingDedupCursor{PageSize: cursor.PageSize, ConsecutiveFastPages: cursor.ConsecutiveFastPages}
	}
	*cursor = next
	return stats, nil
}

func billingDedupBatchID(request string) string {
	for _, prefix := range []string{"batch_image_hold:", "batch_image_capture:", "batch_image_release:"} {
		if strings.HasPrefix(request, prefix) {
			return strings.TrimPrefix(request, prefix)
		}
	}
	return ""
}

func deleteSettledBatchImageDedup(ctx context.Context, tx *sql.Tx, batch string, candidateKey int64, cutoff time.Time) (deleted int64, retained, anomaly bool, err error) {
	var status string
	var jobKey sql.NullInt64
	var updated time.Time
	err = tx.QueryRowContext(ctx, `SELECT status, api_key_id, updated_at FROM batch_image_jobs WHERE batch_id = $1 FOR UPDATE`, batch).Scan(&status, &jobKey, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, true, true, nil
	}
	if err != nil {
		return 0, false, false, err
	}
	if !jobKey.Valid || jobKey.Int64 != candidateKey {
		return 0, true, true, nil
	}
	apiKeyID := jobKey.Int64
	if !service.IsTerminalBatchImageJobStatus(status) {
		return 0, true, false, nil
	}
	keys := []string{service.BatchImageHoldRequestID(batch), service.BatchImageCaptureRequestID(batch), service.BatchImageReleaseRequestID(batch)}
	var hold, capture, release bool
	var newest sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT
		COALESCE(bool_or(request_id = $1), false), COALESCE(bool_or(request_id = $2), false),
		COALESCE(bool_or(request_id = $3), false), max(created_at)
		FROM (
			SELECT request_id, created_at FROM usage_billing_dedup WHERE api_key_id = $4 AND request_id = ANY($5)
			UNION ALL
			SELECT request_id, created_at FROM usage_billing_dedup_archive WHERE api_key_id = $4 AND request_id = ANY($5)
		) credentials`, keys[0], keys[1], keys[2], apiKeyID, pq.Array(keys)).Scan(&hold, &capture, &release, &newest)
	if err != nil {
		return 0, false, false, err
	}
	if hold && !capture && !release {
		return 0, true, true, nil
	}
	if !newest.Valid {
		return 0, false, false, nil
	}
	if !updated.Before(cutoff) || !newest.Time.Before(cutoff) {
		return 0, true, false, nil
	}
	for _, table := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		res, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE api_key_id = $1 AND request_id = ANY($2)`, apiKeyID, pq.Array(keys))
		if err != nil {
			return 0, false, false, fmt.Errorf("delete batch image credentials: %w", err)
		}
		count, err := res.RowsAffected()
		if err != nil {
			return 0, false, false, err
		}
		deleted += count
	}
	return deleted, false, false, nil
}
