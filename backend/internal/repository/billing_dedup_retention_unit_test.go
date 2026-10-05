//go:build unit

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestBillingDedupPageProgress(t *testing.T) {
	for _, tc := range []struct {
		name      string
		maxID     any
		commitErr bool
		done      bool
	}{
		{"no_deletions_still_advances", int64(20), false, false},
		{"deleted_boundary_empty_page_finishes", nil, false, true},
		{"commit_failure_preserves_cursor", int64(20), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			r := newDashboardAggregationRepositoryWithSQL(db)
			cursor := service.BillingDedupCursor{Started: true, ID: 10, EndID: 30, PageSize: 10}
			before := cursor
			mock.ExpectBegin()
			scanned := 10
			if tc.maxID == nil {
				scanned = 0
			}
			mock.ExpectQuery("WITH page AS MATERIALIZED").WillReturnRows(sqlmock.NewRows([]string{"max", "scanned", "deleted"}).AddRow(tc.maxID, scanned, scanned))
			if tc.commitErr {
				mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
			} else {
				mock.ExpectCommit()
			}
			stats, err := r.DeleteBillingDedupPage(context.Background(), &cursor, time.Now(), time.Time{}, false, false)
			if tc.commitErr {
				require.Error(t, err)
				require.Equal(t, before, cursor)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.done, stats.Done)
				if tc.done {
					require.False(t, cursor.Started)
				} else {
					require.EqualValues(t, 20, cursor.ID)
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBillingDedupCaptureRejectsTerminalBeforeClaim(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT status FROM batch_image_jobs").WithArgs("batch", int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(service.BatchImageJobStatusCompleted))
	mock.ExpectRollback()
	r := &usageBillingRepository{db: db}
	_, err = r.CaptureBatchImageBalance(context.Background(), &service.BatchImageBalanceHoldCommand{BatchID: "batch", APIKeyID: 2, RequestID: service.BatchImageCaptureRequestID("batch"), ActualAmount: 1})
	require.ErrorIs(t, err, service.ErrBatchImageSettlementInvalidStatus)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBillingDedupImageRetention(t *testing.T) {
	old := time.Now().Add(-10 * 24 * time.Hour)
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	for _, tc := range []struct {
		name, status                                      string
		hold, capture, release, recent, retained, anomaly bool
	}{
		{"settling", service.BatchImageJobStatusSettling, true, true, false, false, true, false},
		{"captured", service.BatchImageJobStatusCompleted, true, true, false, false, false, false},
		{"released", service.BatchImageJobStatusCancelled, true, false, true, false, false, false},
		{"unsettled", service.BatchImageJobStatusFailed, true, false, false, false, true, true},
		{"deleted_output_unsettled", service.BatchImageJobStatusOutputDeleted, true, false, false, false, true, true},
		{"recent_key", service.BatchImageJobStatusCompleted, true, true, false, true, true, false},
		{"release_without_hold", service.BatchImageJobStatusFailed, false, false, true, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			mock.ExpectQuery("SELECT status, api_key_id, updated_at").WillReturnRows(sqlmock.NewRows([]string{"status", "key", "updated"}).AddRow(tc.status, 2, old))
			if tc.status != service.BatchImageJobStatusSettling {
				created := old
				if tc.recent {
					created = time.Now()
				}
				mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{"hold", "capture", "release", "newest"}).AddRow(tc.hold, tc.capture, tc.release, created))
			}
			if !tc.retained {
				mock.ExpectExec("DELETE FROM usage_billing_dedup WHERE").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("DELETE FROM usage_billing_dedup_archive WHERE").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			mock.ExpectRollback()
			_, retained, anomaly, err := deleteSettledBatchImageDedup(context.Background(), tx, "batch", 2, cutoff)
			require.NoError(t, err)
			require.Equal(t, tc.retained, retained)
			require.Equal(t, tc.anomaly, anomaly)
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
