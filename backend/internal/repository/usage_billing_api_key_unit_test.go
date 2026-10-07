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
	for _, optimized := range []bool{false, true} {
		for _, tc := range []struct {
			costs    [2]float64
			affected int64
		}{
			{[2]float64{2, 3}, 1},
			{[2]float64{2, 0}, 1},
			{[2]float64{0, 3}, 1},
			{[2]float64{0, 0}, 1},
			{[2]float64{-1, 3}, 1},
			{[2]float64{2, -1}, 1},
			{[2]float64{0, 3}, 0}, // Deleted Key: window-only billing still settles the balance.
		} {
			costs := tc.costs
			t.Run(fmt.Sprintf("optimized=%t/%v/affected=%d", optimized, costs, tc.affected), func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				mock.ExpectBegin()
				tx, err := db.Begin()
				require.NoError(t, err)
				mock.ExpectQuery(conditionalBalanceDeductSQL).WithArgs(1.0, int64(42)).
					WillReturnRows(sqlmock.NewRows([]string{"balance"}).AddRow(9.0))
				if optimized && costs[0] > 0 && costs[1] > 0 {
					mock.ExpectQuery(`(?s)UPDATE api_keys.*usage_5h.*RETURNING`).WithArgs(costs[0], int64(7), service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted, costs[1]).WillReturnRows(sqlmock.NewRows([]string{"exhausted"}).AddRow(true))
				} else {
					if costs[0] > 0 {
						mock.ExpectQuery(apiKeyQuotaIncrementSQL).WithArgs(costs[0], int64(7), service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted).WillReturnRows(sqlmock.NewRows([]string{"exhausted"}).AddRow(true))
					}
					if costs[1] > 0 {
						mock.ExpectExec(apiKeyRateLimitIncrementSQL).WithArgs(costs[1], int64(7)).WillReturnResult(sqlmock.NewResult(0, tc.affected))
					}
				}
				result := &service.UsageBillingApplyResult{}
				err = (&usageBillingRepository{optimizedWrites: optimized}).applyUsageBillingEffects(context.Background(), tx, &service.UsageBillingCommand{UserID: 42, BalanceCost: 1, APIKeyID: 7, APIKeyQuotaCost: costs[0], APIKeyRateLimitCost: costs[1]}, result)
				require.NoError(t, err)
				require.Equal(t, costs[0] > 0, result.APIKeyQuotaExhausted)
				require.NotNil(t, result.NewBalance)
				require.Equal(t, 9.0, *result.NewBalance)
				mock.ExpectCommit()
				require.NoError(t, tx.Commit())
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
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
	err = (&usageBillingRepository{optimizedWrites: true}).applyUsageBillingEffects(context.Background(), tx, &service.UsageBillingCommand{APIKeyID: 7, APIKeyQuotaCost: 2, APIKeyRateLimitCost: 3}, &service.UsageBillingApplyResult{})
	require.ErrorIs(t, err, sql.ErrConnDone)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
