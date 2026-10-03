//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise the production SQL against the migrated table shapes, with temporary
// copies so fixtures and legacy cleanup never touch another test's rows.
func TestChannelMonitorV2DirectFiveMinuteAggregation(t *testing.T) {
	tx := testTx(t)
	ctx := context.Background()
	_, tzErr := tx.ExecContext(ctx, `SET LOCAL TIME ZONE 'Asia/Hong_Kong'`)
	require.NoError(t, tzErr)
	for _, table := range append([]string{
		"usage_logs", "ops_error_logs", "groups", "accounts",
		"channel_monitor_v2_metrics_rollup", "channel_monitor_v2_user_metrics_rollup",
		"channel_monitor_v2_error_metrics_rollup", "channel_monitor_v2_latency_histograms_rollup",
	}, channelMonitorV2LegacyTables...) {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (LIKE public.%s INCLUDING ALL) ON COMMIT DROP", table, table))
		require.NoError(t, err)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO groups (id,name,platform) VALUES (7,'v2-test','openai')`)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Minute)
	base := now.Add(-10 * 24 * time.Hour).Truncate(24 * time.Hour)
	insertUsage := func(id int, at time.Time) {
		_, err := tx.ExecContext(ctx, `INSERT INTO usage_logs (id,user_id,api_key_id,account_id,group_id,model,request_id,actual_cost,input_tokens,output_tokens,first_token_ms,duration_ms,created_at)
   VALUES ($1,7,7,7,7,'test-model',$2,1,10,20,100,500,$3)`, id, fmt.Sprintf("usage-%d", id), at)
		require.NoError(t, err)
	}
	for hour := 0; hour < 2; hour++ {
		start := base.Add(time.Duration(hour) * time.Hour)
		insertUsage(hour*2+1, start.Add(time.Minute))
		insertUsage(hour*2+2, start.Add(4*time.Minute))
		// Two attempts with the same request ID still count as one failed request.
		for attempt := 0; attempt < 2; attempt++ {
			_, err = tx.ExecContext(ctx, `INSERT INTO ops_error_logs (id,request_id,user_id,group_id,platform,model,error_phase,error_type,status_code,created_at)
    VALUES ($1,$2,7,7,'openai','test-model','request','internal',500,$3)`, hour*2+attempt+1, fmt.Sprintf("error-%d", hour), start.Add(time.Duration(attempt+2)*time.Minute))
			require.NoError(t, err)
		}
	}
	r := &channelMonitorV2Repository{retentionPeriod: "m"}
	assertMetrics := func(seconds int, start time.Time, wantSuccess, wantErrors int64) {
		for _, table := range []string{"channel_monitor_v2_metrics_rollup", "channel_monitor_v2_user_metrics_rollup"} {
			var success, failures, tokens int64
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT success_requests,error_requests,input_tokens FROM "+table+" WHERE bucket_seconds=$1 AND bucket_start=$2", seconds, start).Scan(&success, &failures, &tokens))
			require.Equal(t, wantSuccess, success, table)
			require.Equal(t, wantErrors, failures, table)
			require.Equal(t, wantSuccess*10, tokens, table)
		}
		var failures, samples int64
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT SUM(error_requests) FROM channel_monitor_v2_error_metrics_rollup WHERE bucket_seconds=$1 AND bucket_start=$2`, seconds, start).Scan(&failures))
		require.Equal(t, wantErrors, failures)
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT SUM(sample_count) FROM channel_monitor_v2_latency_histograms_rollup WHERE bucket_seconds=$1 AND bucket_start=$2 AND user_id=0 AND metric='ttft'`, seconds, start).Scan(&samples))
		require.Equal(t, wantSuccess, samples)
		var personalRows int
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_monitor_v2_latency_histograms_rollup WHERE user_id > 0`).Scan(&personalRows))
		require.Zero(t, personalRows)
	}
	// Walk backwards, pruning historical 5m data after each full-hour chunk.
	for hour := 1; hour >= 0; hour-- {
		start := base.Add(time.Duration(hour) * time.Hour)
		end := start.Add(time.Hour)
		require.NoError(t, r.recomputeFiveMinuteBuckets(ctx, tx, start, end))
		assertMetrics(300, start, 2, 1)
		require.NoError(t, r.recomputeFixedRollups(ctx, tx, start, end))
		require.NoError(t, r.pruneChannelMonitorV2Retention(ctx, tx, now))
		assertMetrics(3600, start, 2, 1)
		assertMetrics(43200, base, int64((2-hour)*2), int64(2-hour))
		assertMetrics(86400, base, int64((2-hour)*2), int64(2-hour))
	}
	// A late request in an already built bucket is included once; neighboring
	// hourly totals survive even though their 5m source rows have been pruned.
	insertUsage(5, base.Add(3*time.Minute))
	for repeat := 0; repeat < 2; repeat++ {
		require.NoError(t, r.recomputeFiveMinuteBuckets(ctx, tx, base, base.Add(time.Hour)))
		assertMetrics(300, base, 3, 1)
		require.NoError(t, r.recomputeFixedRollups(ctx, tx, base, base.Add(time.Hour)))
		assertMetrics(43200, base, 5, 2)
		assertMetrics(86400, base, 5, 2)
	}
	for _, table := range channelMonitorV2LegacyTables {
		var count int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count))
		require.Zero(t, count, "must not write legacy 1m rows: %s", table)
	}
	// Upgrade cleanup clears all legacy rows, including recently created ones.
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_monitor_v2_metrics_1m (bucket_start,platform,group_id,model)
 SELECT $1,'openai',n,'legacy' FROM generate_series(1,$2::integer) n`, now, 5001)
	require.NoError(t, err)
	require.NoError(t, r.pruneChannelMonitorV2Retention(ctx, tx, now))
	var remaining int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_monitor_v2_metrics_1m`).Scan(&remaining))
	require.Zero(t, remaining)
	require.NoError(t, r.pruneChannelMonitorV2Retention(ctx, tx, now))
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_monitor_v2_metrics_1m`).Scan(&remaining))
	require.Zero(t, remaining)
}
