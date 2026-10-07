//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestApplyUsageBillingEffects_KeyUpdateSelection(t *testing.T) {
	for _, costs := range [][2]float64{{2, 3}, {2, 0}, {0, 3}, {0, 0}, {-1, 3}, {2, -1}} {
		t.Run(fmt.Sprint(costs), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			mock.ExpectBegin()
			tx, err := db.Begin()
			require.NoError(t, err)
			switch {
			case costs[0] > 0 && costs[1] > 0:
				mock.ExpectQuery(`(?s)UPDATE api_keys.*usage_5h.*RETURNING`).WithArgs(costs[0], int64(7), service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted, costs[1]).WillReturnRows(sqlmock.NewRows([]string{"exhausted"}).AddRow(true))
			case costs[0] > 0:
				mock.ExpectQuery(apiKeyQuotaIncrementSQL).WithArgs(costs[0], int64(7), service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted).WillReturnRows(sqlmock.NewRows([]string{"exhausted"}).AddRow(true))
			case costs[1] > 0:
				mock.ExpectExec(apiKeyRateLimitIncrementSQL).WithArgs(costs[1], int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			result := &service.UsageBillingApplyResult{}
			err = (&usageBillingRepository{}).applyUsageBillingEffects(context.Background(), tx, &service.UsageBillingCommand{APIKeyID: 7, APIKeyQuotaCost: costs[0], APIKeyRateLimitCost: costs[1]}, result)
			require.NoError(t, err)
			require.Equal(t, costs[0] > 0, result.APIKeyQuotaExhausted)
			mock.ExpectRollback()
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestApplyUsageBillingEffects_CombinedKeyError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	mock.ExpectQuery(`(?s)UPDATE api_keys.*usage_5h.*RETURNING`).WillReturnError(sql.ErrConnDone)
	err = (&usageBillingRepository{}).applyUsageBillingEffects(context.Background(), tx, &service.UsageBillingCommand{APIKeyID: 7, APIKeyQuotaCost: 2, APIKeyRateLimitCost: 3}, &service.UsageBillingApplyResult{})
	require.ErrorIs(t, err, sql.ErrConnDone)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
