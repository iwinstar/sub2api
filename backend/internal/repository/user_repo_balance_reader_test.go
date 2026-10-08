package repository

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUserRepositoryBalanceReaderCapability(t *testing.T) {
	_, ok := NewUserRepository(nil, nil).(service.UserBalanceReader)
	require.True(t, ok)
}

func TestUserRepositoryGetBalance(t *testing.T) {
	for _, value := range []float64{10, 0, -2} {
		repo, mock := newRedeemAdjustmentRepoMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT balance FROM users WHERE id = $1 AND deleted_at IS NULL`)).WithArgs(int64(42)).WillReturnRows(sqlmock.NewRows([]string{"balance"}).AddRow(value))
		got, err := repo.GetBalance(context.Background(), 42)
		require.NoError(t, err)
		require.Equal(t, value, got)
		require.NoError(t, mock.ExpectationsWereMet())
	}
	repo, mock := newRedeemAdjustmentRepoMock(t)
	mock.ExpectQuery(`SELECT balance FROM users`).WithArgs(int64(42)).WillReturnRows(sqlmock.NewRows([]string{"balance"}))
	_, err := repo.GetBalance(context.Background(), 42)
	require.ErrorIs(t, err, service.ErrUserNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}
