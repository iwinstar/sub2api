//go:build integration || dedup_sql

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Optional local verification against a disposable PostgreSQL instance. Never
// point this at an application database: the suite intentionally runs deletion.
func TestBillingDedupDisposablePostgres(t *testing.T) {
	dsn := os.Getenv("SUB2API_DEDUP_TEST_DSN")
	if dsn == "" {
		t.Skip("set SUB2API_DEDUP_TEST_DSN to a disposable test database")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	testBillingDedupRetentionSQL(t, db)
}

func testBillingDedupRetentionSQL(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	// Verify the actual connection, not a name inferred from the DSN. This
	// guard runs before any mutation, including through the integration harness.
	var databaseName string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT current_database()`).Scan(&databaseName))
	require.True(t, strings.HasSuffix(databaseName, "_test"), "refusing destructive tests outside a database ending in _test: %s", databaseName)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	now := time.Now().UTC()
	old := now.Add(-10 * 24 * time.Hour)
	cutoff := now.Add(-3 * 24 * time.Hour)
	key := int64(987654321)
	id := uuid.NewString()
	insert := func(t *testing.T, table, request string, created time.Time) int64 {
		t.Helper()
		var rowID int64
		query := `INSERT INTO ` + table + ` (request_id, api_key_id, request_fingerprint, created_at) VALUES ($1,$2,'test',$3)`
		if table == "usage_billing_dedup" {
			require.NoError(t, db.QueryRowContext(ctx, query+` RETURNING id`, request, key, created).Scan(&rowID))
		} else {
			_, err := db.ExecContext(ctx, query, request, key, created)
			require.NoError(t, err)
		}
		return rowID
	}
	count := func(t *testing.T, request string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2) + (SELECT count(*) FROM usage_billing_dedup_archive WHERE request_id=$1 AND api_key_id=$2)`, request, key).Scan(&n))
		return n
	}
	run := func(t *testing.T, archive, image bool) {
		t.Helper()
		var cursor service.BillingDedupCursor
		cursor.PageSize = 1 // Exercise continuation and upper-bound handling across pages.
		for i := 0; i < 1000; i++ {
			stats, err := repo.DeleteBillingDedupPage(ctx, &cursor, cutoff, time.Time{}, archive, image)
			require.NoError(t, err)
			if stats.Done {
				return
			}
		}
		t.Fatal("scan did not complete")
	}
	t.Run("ordinary_both_tables_and_nonmonotonic_time", func(t *testing.T) {
		fresh := "client:fresh:" + id
		expired := "client:expired:" + id
		image := "batch_image_hold:missing:" + id
		freshID := insert(t, "usage_billing_dedup", fresh, now)
		expiredID := insert(t, "usage_billing_dedup", expired, old)
		require.Greater(t, expiredID, freshID)
		insert(t, "usage_billing_dedup", image, old)
		insert(t, "usage_billing_dedup_archive", "archive:old:"+id, old)
		insert(t, "usage_billing_dedup_archive", "archive:new:"+id, now)
		run(t, false, false)
		run(t, true, false)
		require.Equal(t, 0, count(t, expired))
		require.Equal(t, 1, count(t, fresh))
		require.Equal(t, 1, count(t, image))
		require.Equal(t, 0, count(t, "archive:old:"+id))
		require.Equal(t, 1, count(t, "archive:new:"+id))
	})
	t.Run("one_hour_boundary_unordered_candidates", func(t *testing.T) {
		boundary := now.Add(-time.Hour).Truncate(time.Microsecond)
		for _, archive := range []bool{false, true} {
			table := "usage_billing_dedup"
			if archive {
				table = "usage_billing_dedup_archive"
			}
			suffix := uuid.NewString()
			fresh := "hour:fresh:" + suffix
			exact := "hour:exact:" + suffix
			expired := "hour:expired:" + suffix
			video := "grok-video:hour:" + suffix
			image := "batch_image_hold:hour:" + suffix
			insert(t, table, fresh, now)
			insert(t, table, expired, boundary.Add(-time.Microsecond))
			insert(t, table, exact, boundary)
			insert(t, table, video, old)
			insert(t, table, image, old)
			cursor := service.BillingDedupCursor{PageSize: 1}
			done := false
			for n := 0; n < 1000; n++ {
				stats, err := repo.DeleteBillingDedupPage(ctx, &cursor, boundary, time.Time{}, archive, false)
				require.NoError(t, err)
				if stats.Done {
					done = true
					break
				}
			}
			require.True(t, done)
			require.Zero(t, count(t, expired))
			for _, request := range []string{fresh, exact, video, image} {
				require.Equal(t, 1, count(t, request), request)
			}
		}
	})
	t.Run("independent_ordinary_video_retention", func(t *testing.T) {
		for _, archive := range []bool{false, true} {
			table := "usage_billing_dedup"
			if archive {
				table = "usage_billing_dedup_archive"
			}
			for _, mode := range []string{"both", "ordinary_only", "video_only"} {
				suffix := uuid.NewString()
				ordinary := "client:" + suffix
				videoRecent := "grok-video:recent:" + suffix
				videoOld := "grok-video:old:" + suffix
				insert(t, table, ordinary, now.Add(-48*time.Hour))
				insert(t, table, videoRecent, now.Add(-48*time.Hour))
				insert(t, table, videoOld, old)
				ordinaryCutoff, videoCutoff := now.Add(-24*time.Hour), now.Add(-7*24*time.Hour)
				if mode == "ordinary_only" {
					videoCutoff = time.Time{}
				}
				if mode == "video_only" {
					ordinaryCutoff = time.Time{}
				}
				cursor := service.BillingDedupCursor{PageSize: 1}
				for {
					stats, err := repo.DeleteBillingDedupPage(ctx, &cursor, ordinaryCutoff, videoCutoff, archive, false)
					require.NoError(t, err)
					if stats.Done {
						break
					}
				}
				wantOrdinary, wantVideo := 0, 0
				if mode == "video_only" {
					wantOrdinary = 1
				}
				if mode == "ordinary_only" {
					wantVideo = 1
				}
				require.Equal(t, wantOrdinary, count(t, ordinary), mode)
				require.Equal(t, wantVideo, count(t, videoOld), mode)
				require.Equal(t, 1, count(t, videoRecent), mode)
			}
		}
	})
	t.Run("settled_image_keys_split_across_tables", func(t *testing.T) {
		for _, tc := range []struct {
			status           string
			capture, release bool
			keep             bool
		}{
			{service.BatchImageJobStatusCompleted, true, false, false},
			{service.BatchImageJobStatusCancelled, false, true, false},
			{service.BatchImageJobStatusFailed, false, false, true},
			{service.BatchImageJobStatusSettling, true, false, true},
			{service.BatchImageJobStatusOutputDeleted, false, false, true},
		} {
			batch := "dedup-test-" + uuid.NewString()
			_, err := db.ExecContext(ctx, `INSERT INTO batch_image_jobs (batch_id,user_id,api_key_id,provider,model,status,item_count,updated_at) VALUES ($1,1,$2,'gemini_api','test',$3,1,$4)`, batch, key, tc.status, old)
			require.NoError(t, err)
			insert(t, "usage_billing_dedup", service.BatchImageHoldRequestID(batch), old)
			if tc.capture {
				insert(t, "usage_billing_dedup_archive", service.BatchImageCaptureRequestID(batch), old)
			}
			if tc.release {
				insert(t, "usage_billing_dedup_archive", service.BatchImageReleaseRequestID(batch), old)
			}
			run(t, false, true)
			run(t, true, true)
			want := 0
			if tc.keep {
				want = 1
			}
			require.Equal(t, want, count(t, service.BatchImageHoldRequestID(batch)), tc.status)
			if !tc.keep {
				require.Equal(t, 0, count(t, service.BatchImageCaptureRequestID(batch)))
				require.Equal(t, 0, count(t, service.BatchImageReleaseRequestID(batch)))
				// A later release may recreate its own key, but must not touch money.
				billing := &usageBillingRepository{db: db}
				result, err := billing.ReleaseBatchImageBalance(ctx, &service.BatchImageBalanceHoldCommand{BatchID: batch, APIKeyID: key, UserID: 1, RequestID: service.BatchImageReleaseRequestID(batch), HoldAmount: 10})
				require.NoError(t, err)
				require.Nil(t, result.NewBalance)
				_, err = billing.CaptureBatchImageBalance(ctx, &service.BatchImageBalanceHoldCommand{BatchID: batch, APIKeyID: key, UserID: 1, RequestID: service.BatchImageCaptureRequestID(batch), HoldAmount: 10, ActualAmount: 5})
				require.ErrorIs(t, err, service.ErrBatchImageSettlementInvalidStatus)
			}
		}
	})
	t.Run("archival_and_deletion_serialize", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, lockBillingDedupMaintenance(ctx, tx))
		blockedCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err = repo.archiveUsageBillingDedupBatch(blockedCtx, cutoff)
		require.Error(t, err, "archival must wait for the deletion maintenance lock")
		require.NoError(t, tx.Rollback())
		_, err = repo.archiveUsageBillingDedupBatch(ctx, cutoff)
		require.NoError(t, err)
	})
	t.Run("capture_waits_for_job_lock_and_rechecks_status", func(t *testing.T) {
		batch := "dedup-test-" + uuid.NewString()
		_, err := db.ExecContext(ctx, `INSERT INTO batch_image_jobs (batch_id,user_id,api_key_id,provider,model,status,item_count,updated_at) VALUES ($1,1,$2,'gemini_api','test','settling',1,$3)`, batch, key, old)
		require.NoError(t, err)
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = tx.ExecContext(ctx, `UPDATE batch_image_jobs SET status='completed' WHERE batch_id=$1`, batch)
		require.NoError(t, err)
		billing := &usageBillingRepository{db: db}
		cmd := &service.BatchImageBalanceHoldCommand{BatchID: batch, APIKeyID: key, UserID: 1, RequestID: service.BatchImageCaptureRequestID(batch), HoldAmount: 10, ActualAmount: 5}
		blockedCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err = billing.CaptureBatchImageBalance(blockedCtx, cmd)
		require.Error(t, err)
		require.Contains(t, fmt.Sprint(err), "cancel")
		require.NoError(t, tx.Commit())
		_, err = billing.CaptureBatchImageBalance(ctx, cmd)
		require.ErrorIs(t, err, service.ErrBatchImageSettlementInvalidStatus)
		require.Equal(t, 0, count(t, cmd.RequestID))
		_, err = db.ExecContext(ctx, `UPDATE batch_image_jobs SET retry_count=4 WHERE batch_id=$1`, batch)
		require.NoError(t, err)
		jobs := NewBatchImageRepository(db)
		_, err = jobs.SetBatchImageJobSettlementFailed(ctx, batch, "SETTLEMENT_PRICING_MISSING", "stale worker")
		require.ErrorIs(t, err, service.ErrBatchImageSettlementInvalidStatus)
		var retries int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT retry_count FROM batch_image_jobs WHERE batch_id=$1`, batch).Scan(&retries))
		require.Equal(t, 4, retries)
	})
}
