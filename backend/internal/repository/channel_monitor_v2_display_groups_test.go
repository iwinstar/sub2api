package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestChannelMonitorV2DisplayGroupMatrix(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		allowed, selected, monitored, want []int64
		mode                               service.ChannelMonitorV2GroupBy
		unrestricted                       bool
	}{
		{name: "all authorized", allowed: []int64{1, 2, 3}, want: []int64{1, 2, 3}},
		{name: "only member 1", allowed: []int64{1}, want: []int64{1, 2}},
		{name: "only member 2", allowed: []int64{2}, want: []int64{1, 2}},
		{name: "group-only view", allowed: []int64{1}, want: []int64{1, 2}, mode: service.ChannelMonitorV2GroupByPlatformGroup},
		{name: "selected authorized member", allowed: []int64{1, 3}, selected: []int64{1}, want: []int64{1, 2}},
		{name: "selected standalone group", allowed: []int64{1, 3}, selected: []int64{3}, want: []int64{3}},
		{name: "unauthorized guessed member", allowed: []int64{1}, selected: []int64{2}},
		{name: "no groups"},
		{name: "unrelated standalone", allowed: []int64{3}, want: []int64{3}},
		{name: "configured monitoring scope", allowed: []int64{1}, monitored: []int64{1, 3}, want: []int64{1}},
		{name: "model view unchanged", allowed: []int64{1}, want: []int64{1}, mode: service.ChannelMonitorV2GroupByPlatformModel},
		{name: "platform view unchanged", allowed: []int64{1}, want: []int64{1}, mode: service.ChannelMonitorV2GroupByPlatform},
		{name: "admin selected member", selected: []int64{2}, want: []int64{1, 2}, unrestricted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			repo := &channelMonitorV2Repository{db: db}
			start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			end := start.Add(5 * time.Minute)
			cfg := service.ChannelMonitorV2Config{
				Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}},
				GroupIDs:  tc.monitored,
				DisplayGroups: []service.ChannelMonitorV2DisplayGroup{
					{ID: "gpt", Name: "GPT channel", GroupIDs: []int64{1, 2}},
					{ID: "hidden", Name: "Hidden channel", GroupIDs: []int64{4, 5}},
				},
				IgnoredErrorCategories: []string{"client_cancelled"},
			}
			mode := tc.mode
			if mode == "" {
				mode = service.ChannelMonitorV2GroupByPlatformGroupModel
			}
			filter := service.ChannelMonitorV2Filter{Start: start, End: end, Bucket: 5 * time.Minute, RestrictGroups: !tc.unrestricted, AllowedGroupIDs: tc.allowed, GroupIDs: tc.selected}
			original := filter
			original.AllowedGroupIDs = append([]int64(nil), filter.AllowedGroupIDs...)
			original.GroupIDs = append([]int64(nil), filter.GroupIDs...)
			if len(tc.want) == 0 {
				result, err := repo.GetMatrix(context.Background(), filter, cfg, mode, false)
				require.NoError(t, err)
				require.Empty(t, result.Items)
				require.NoError(t, mock.ExpectationsWereMet())
				return
			}
			mock.ExpectQuery("SELECT usage_coverage_start").WillReturnRows(sqlmock.NewRows([]string{"u", "e", "through", "computed", "cursor"}).AddRow(start, start, end, end, start))
			facts := sqlmock.NewRows([]string{"bucket", "platform", "group", "name", "model", "success", "error", "affected", "attempts", "in", "out", "creation", "read", "ttft_sum", "ttft_count", "duration_sum", "duration_count"})
			hist := sqlmock.NewRows([]string{"bucket", "platform", "group", "model", "user", "metric", "upper", "count"})
			groups := sqlmock.NewRows([]string{"id", "name", "platform"})
			ignored := sqlmock.NewRows([]string{"bucket", "platform", "group", "model", "count"})
			for _, id := range tc.want {
				success, failures, cache, latency := int64(90), int64(10), int64(10), int64(100)
				if id == 2 {
					success, failures, cache, latency = 900, 0, 900, 1000
				}
				facts.AddRow(start, "openai", id, fmt.Sprint(id), "gpt-A", success, failures, 0, 0, 100, 100, 0, cache, latency*success, success, 2000*success, success)
				hist.AddRow(start, "openai", id, "gpt-A", 0, "ttft", latency, success)
				groups.AddRow(id, fmt.Sprint(id), "openai")
				if id == 1 {
					ignored.AddRow(start, "openai", id, "gpt-A", 5)
				}
			}
			// Facts, histograms, ignored errors and seeds must share the exact scope.
			mock.ExpectQuery(`SELECT .* FROM channel_monitor_v2_metrics_rollup m .*m.group_id = ANY`).WithArgs(start, end, pq.Array([]string{"openai"}), pq.Array(tc.want), 300).WillReturnRows(facts)
			mock.ExpectQuery(`SELECT .* FROM channel_monitor_v2_latency_histograms_rollup h .*h.group_id = ANY`).WithArgs(start, end, pq.Array([]string{"openai"}), pq.Array(tc.want), 300, 0).WillReturnRows(hist)
			mock.ExpectQuery(`SELECT id, COALESCE`).WithArgs(pq.Array(tc.want)).WillReturnRows(groups)
			mock.ExpectQuery(`SELECT .* FROM channel_monitor_v2_error_metrics_rollup e .*e.group_id = ANY`).WithArgs(start, end, pq.Array([]string{"openai"}), pq.Array(tc.want), 300, pq.Array([]string{"client_cancelled"}), 1).WillReturnRows(ignored)
			result, err := repo.GetMatrix(context.Background(), filter, cfg, mode, false)
			require.NoError(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
			require.Equal(t, original, filter, "must not expand scope for subsequent queries")
			expectedRows := 1
			if len(tc.want) == 3 {
				expectedRows = 2
			}
			require.Len(t, result.Items, expectedRows)
			for _, row := range result.Items {
				require.NotEqual(t, "hidden", row.DisplayGroupID)
				require.Len(t, row.Buckets, 1)
				require.Nil(t, row.Metrics.UpstreamAttemptCount)
				if row.DisplayGroupID == "gpt" {
					require.Equal(t, "GPT channel", row.GroupName)
					require.Nil(t, row.GroupID)
					if len(tc.want) >= 2 {
						require.EqualValues(t, 1000, row.Metrics.RequestCount)
						require.InDelta(t, 0.99, row.Metrics.SuccessRate, 0.00001)
						require.InDelta(t, 0.005, row.Metrics.ErrorRate, 0.00001)
						require.InDelta(t, 910.0/1110, row.Metrics.CacheRate, 0.00001)
						require.EqualValues(t, 1000, *row.Metrics.TTFT.P50Ms)
						require.NotNil(t, row.Health.Score)
					} else {
						require.EqualValues(t, 100, row.Metrics.RequestCount)
					}
				} else {
					require.EqualValues(t, 100, row.Metrics.RequestCount)
					if mode == service.ChannelMonitorV2GroupByPlatformGroupModel {
						require.EqualValues(t, 3, *row.GroupID)
					}
				}
				require.Equal(t, row.Metrics.SuccessRate, row.Buckets[0].Metrics.SuccessRate)
				require.Equal(t, row.Metrics.ErrorRate, row.Buckets[0].Metrics.ErrorRate)
			}
		})
	}
}

func TestChannelMonitorV2DisplayGroupDimensionsStaySeparate(t *testing.T) {
	cfg := service.ChannelMonitorV2Config{DisplayGroups: []service.ChannelMonitorV2DisplayGroup{{ID: "a", Name: "A", GroupIDs: []int64{1, 2}}, {ID: "b", Name: "B", GroupIDs: []int64{3}}}}
	key := func(platform string, id int64, model string) channelMonitorV2MatrixKey {
		return channelMonitorV2MatrixDimensionKey(service.ChannelMonitorV2GroupByPlatformGroupModel, cfg, channelMonitorV2DisplayGroupIndex(cfg), platform, id, model)
	}
	require.Equal(t, key("openai", 1, "gpt"), key("openai", 2, "gpt"))
	require.NotEqual(t, key("openai", 1, "gpt"), key("anthropic", 2, "gpt"))
	require.NotEqual(t, key("openai", 1, "gpt"), key("openai", 2, "other"))
	require.NotEqual(t, key("openai", 1, "gpt"), key("openai", 3, "gpt"))
	require.EqualValues(t, 4, key("openai", 4, "gpt").groupID)
	require.Empty(t, channelMonitorV2MatrixDimensionKey(service.ChannelMonitorV2GroupByPlatformModel, cfg, channelMonitorV2DisplayGroupIndex(cfg), "openai", 1, "gpt").displayGroupID)
}

var displayConfigColumns = []string{"version", "enabled", "refresh", "platforms", "groups", "ignored", "thresholds", "updated", "actor"}

func displayConfigValues() []driver.Value {
	return []driver.Value{2, true, 300, `[]`, `{}`, `{}`, `{}`, time.Now(), nil}
}

func TestChannelMonitorV2DisplayGroupsConfigAtomicSave(t *testing.T) {
	for _, scenario := range []string{"save", "write failure", "conflict", "missing group", "name collision", "clear"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			repo := &channelMonitorV2Repository{db: db}
			cfg := service.ChannelMonitorV2Config{Enabled: true, RefreshIntervalSeconds: 300, DisplayGroups: []service.ChannelMonitorV2DisplayGroup{{ID: "gpt", Name: "GPT", GroupIDs: []int64{1, 2}}}}
			if scenario == "clear" {
				cfg.DisplayGroups = []service.ChannelMonitorV2DisplayGroup{}
			}
			mock.ExpectBegin()
			q := mock.ExpectQuery("UPDATE channel_monitor_v2_config")
			if scenario == "conflict" {
				q.WillReturnError(sql.ErrNoRows)
				mock.ExpectRollback()
			} else {
				q.WillReturnRows(sqlmock.NewRows(displayConfigColumns).AddRow(displayConfigValues()...))
				if scenario != "clear" {
					count := 2
					if scenario == "missing group" {
						count = 1
					}
					mock.ExpectQuery("SELECT COUNT.*FROM groups").WithArgs(pq.Array([]int64{1, 2})).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
				}
				if scenario != "missing group" && scenario != "clear" {
					mock.ExpectQuery("SELECT EXISTS").WithArgs(pq.Array([]int64{1, 2}), pq.Array([]string{"GPT"})).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(scenario == "name collision"))
				}
				if scenario == "missing group" || scenario == "name collision" {
					mock.ExpectRollback()
				} else {
					payload, _ := json.Marshal(cfg.DisplayGroups)
					write := mock.ExpectExec("INSERT INTO settings").WithArgs(string(payload))
					if scenario == "write failure" {
						write.WillReturnError(errors.New("storage failure"))
						mock.ExpectRollback()
					} else {
						write.WillReturnResult(sqlmock.NewResult(0, 1))
						mock.ExpectCommit()
					}
				}
			}
			result, err := repo.UpdateConfig(context.Background(), cfg, 1)
			switch scenario {
			case "save", "clear":
				require.NoError(t, err)
				require.Equal(t, cfg.DisplayGroups, result.DisplayGroups)
			case "conflict":
				require.ErrorIs(t, err, service.ErrChannelMonitorV2ConfigConflict)
			case "missing group", "name collision":
				require.ErrorIs(t, err, service.ErrChannelMonitorV2InvalidConfig)
			default:
				require.Error(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestChannelMonitorV2DisplayGroupsLoad(t *testing.T) {
	for _, payload := range []string{`[]`, `[{"id":"gpt","name":"GPT","group_ids":[1,2]}]`, `broken`} {
		t.Run(payload, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			mock.ExpectQuery("SELECT version.*SELECT value FROM settings").WillReturnRows(sqlmock.NewRows(append(append([]string{}, displayConfigColumns...), "display_groups")).AddRow(append(displayConfigValues(), payload)...))
			cfg, err := (&channelMonitorV2Repository{db: db}).GetConfig(context.Background())
			if payload == "broken" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				var expected []service.ChannelMonitorV2DisplayGroup
				require.NoError(t, json.Unmarshal([]byte(payload), &expected))
				require.Equal(t, expected, cfg.DisplayGroups)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
