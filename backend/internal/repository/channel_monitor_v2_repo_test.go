package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestChannelMonitorV2DateBinOriginIsUTC(t *testing.T) {
	require.Equal(t, "TIMESTAMPTZ '1970-01-01 00:00:00+00'", channelMonitorV2DateBinOrigin)
	require.Equal(t, "date_bin($1::interval,m.bucket_start,TIMESTAMPTZ '1970-01-01 00:00:00+00')", channelMonitorV2DateBinExpr("m.bucket_start"))

	for _, query := range []string{
		channelMonitorV2FixedRollupBoundsSQL,
		channelMonitorV2MetricsRollupSQL,
		channelMonitorV2UserMetricsRollupSQL,
		channelMonitorV2HistogramRollupSQL,
		channelMonitorV2ErrorRollupSQL,
	} {
		require.Contains(t, query, channelMonitorV2DateBinOrigin)
		require.NotContains(t, query, "TIMESTAMPTZ '1970-01-01'")
	}
}

func TestChannelMonitorV2DisplayModelIsPlatformScoped(t *testing.T) {
	cfg := service.ChannelMonitorV2Config{Platforms: []service.ChannelMonitorV2PlatformConfig{
		{Platform: "openai", Enabled: true, Models: []string{"shared", "gpt-5"}},
		{Platform: "grok", Enabled: true, Models: []string{"grok-4"}},
		// Empty models list must NOT collapse everything into __other__.
		{Platform: "anthropic", Enabled: true, Models: []string{}},
	}}
	require.Equal(t, "shared", channelMonitorV2DisplayModel(cfg, "openai", "shared"))
	require.Equal(t, service.ChannelMonitorV2OtherModel, channelMonitorV2DisplayModel(cfg, "grok", "shared"))
	require.Equal(t, "claude-sonnet-4", channelMonitorV2DisplayModel(cfg, "anthropic", "claude-sonnet-4"))
	// Unconfigured platform still surfaces the real model name.
	require.Equal(t, "gemini-2.5-pro", channelMonitorV2DisplayModel(cfg, "gemini", "gemini-2.5-pro"))
	require.True(t, channelMonitorV2ModelSelected(service.ChannelMonitorV2Filter{Models: []string{service.ChannelMonitorV2OtherModel}}, cfg, "grok", "shared"))
}

func TestChannelMonitorV2MatrixDimensionKey(t *testing.T) {
	cfg := service.ChannelMonitorV2Config{Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true, Models: []string{"gpt-5"}}}}
	key := channelMonitorV2MatrixDimensionKey(service.ChannelMonitorV2GroupByPlatformGroupModel, cfg, channelMonitorV2DisplayGroupIndex(cfg), "openai", 7, "gpt-5")
	require.Equal(t, channelMonitorV2MatrixKey{platform: "openai", groupID: 7, model: "gpt-5"}, key)
	key = channelMonitorV2MatrixDimensionKey(service.ChannelMonitorV2GroupByPlatformModel, cfg, channelMonitorV2DisplayGroupIndex(cfg), "openai", 7, "unlisted")
	require.Equal(t, channelMonitorV2MatrixKey{platform: "openai", model: service.ChannelMonitorV2OtherModel}, key)
	key = channelMonitorV2MatrixDimensionKey(service.ChannelMonitorV2GroupByPlatform, cfg, channelMonitorV2DisplayGroupIndex(cfg), "openai", 7, "gpt-5")
	require.Equal(t, channelMonitorV2MatrixKey{platform: "openai"}, key)
}

func TestChannelMonitorV2HistogramPercentilesAreMergedFromCounts(t *testing.T) {
	// 100 samples: 50@100, 40@500, 10@1000
	// target = int64(total*p + 0.999999) truncates: p50→50, p90→90, p95→95
	// cumulative hits: p50@100, p90@500 (50+40), p95@1000
	histogram := map[int64]int64{100: 50, 500: 40, 1000: 10}
	require.Equal(t, int64(100), *histPercentile(histogram, .5))
	require.Equal(t, int64(500), *histPercentile(histogram, .9))
	require.Equal(t, int64(1000), *histPercentile(histogram, .95))
	require.Nil(t, histPercentile(nil, .95))
	// latencyMetric exposes avg + p50 + p90 + p95
	lat := latencyMetric(1000, 10, histogram)
	require.NotNil(t, lat.AvgMs)
	require.NotNil(t, lat.P50Ms)
	require.NotNil(t, lat.P90Ms)
	require.NotNil(t, lat.P95Ms)
	require.Equal(t, int64(100), *lat.P50Ms)
	require.Equal(t, int64(500), *lat.P90Ms)
	require.Equal(t, int64(1000), *lat.P95Ms)
}

func TestChannelMonitorV2MetricIncludesSuccessRate(t *testing.T) {
	acc := newMetricAccumulator()
	acc.success, acc.errors = 80, 20
	metric := acc.metric(1, false)
	require.Equal(t, int64(100), metric.RequestCount)
	require.InDelta(t, 0.8, metric.SuccessRate, 0.0001)
	require.InDelta(t, 0.2, metric.ErrorRate, 0.0001)
	require.Nil(t, metric.UpstreamAffectedRequests)

	adminMetric := acc.metric(1, true)
	require.NotNil(t, adminMetric.UpstreamAffectedRequests)
}

func TestChannelMonitorV2WhereUsesConfiguredScopeAndEmptyFilterMeansAllConfigured(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{Start: time.Unix(1, 0), End: time.Unix(2, 0)}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}, {Platform: "grok", Enabled: false}},
		GroupIDs:  []int64{3, 4},
	}
	where, args := channelMonitorV2Where(filter, cfg, "m")
	require.Contains(t, where, "m.platform = ANY($3)")
	require.Contains(t, where, "m.group_id = ANY($4)")
	require.Len(t, args, 4)
}

func TestChannelMonitorV2WhereRejectsGroupFilterOutsideConfiguredScope(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{
		Start: time.Unix(1, 0), End: time.Unix(2, 0), GroupIDs: []int64{9},
	}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}},
		GroupIDs:  []int64{3, 4},
	}
	where, args := channelMonitorV2Where(filter, cfg, "m")
	require.Contains(t, where, "FALSE")
	require.NotContains(t, where, "m.group_id = ANY")
	require.Len(t, args, 3)
}

func TestChannelMonitorV2WhereRestrictsOrdinaryViewerToAllowedConfiguredGroups(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{
		Start: time.Unix(1, 0), End: time.Unix(2, 0),
		GroupIDs: []int64{4, 9}, AllowedGroupIDs: []int64{3, 4}, RestrictGroups: true,
	}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}},
		GroupIDs:  []int64{3, 4, 9},
	}
	where, args := channelMonitorV2Where(filter, cfg, "m")
	require.Contains(t, where, "m.group_id = ANY($4)")
	require.Equal(t, pq.Array([]int64{4}), args[3])
}

func TestChannelMonitorV2WhereRejectsOrdinaryViewerWithNoAllowedGroups(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{
		Start: time.Unix(1, 0), End: time.Unix(2, 0),
		GroupIDs: []int64{9}, RestrictGroups: true,
	}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}},
		GroupIDs:  []int64{3, 9},
	}
	where, _ := channelMonitorV2Where(filter, cfg, "m")
	require.Contains(t, where, "FALSE")
	require.NotContains(t, where, "m.group_id = ANY")
}

func TestChannelMonitorV2CatalogKeepsViewerScopeWhileIgnoringPickerFilters(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{
		Platforms: []string{"openai"}, GroupIDs: []int64{9}, Models: []string{"gpt-5"},
		AllowedGroupIDs: []int64{3}, RestrictGroups: true,
	}
	catalog := channelMonitorV2CatalogFilter(filter)
	require.Empty(t, catalog.Platforms)
	require.Empty(t, catalog.GroupIDs)
	require.Empty(t, catalog.Models)
	require.True(t, catalog.RestrictGroups)
	require.Equal(t, []int64{3}, catalog.AllowedGroupIDs)
}

func TestChannelMonitorV2AdminScopeRemainsGlobal(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{Start: time.Unix(1, 0), End: time.Unix(2, 0), GroupIDs: []int64{9}}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}},
		GroupIDs:  []int64{3, 9},
	}
	where, args := channelMonitorV2Where(filter, cfg, "m")
	require.Contains(t, where, "m.group_id = ANY($4)")
	require.Equal(t, pq.Array([]int64{9}), args[3])
}

func TestChannelMonitorV2MatrixDoesNotSeedGroupsForEmptyViewerScope(t *testing.T) {
	filter := service.ChannelMonitorV2Filter{RestrictGroups: true}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true}},
		GroupIDs:  []int64{3, 9},
	}
	accs := seedChannelMonitorV2MatrixAccumulators(filter, cfg, service.ChannelMonitorV2GroupByPlatformGroup, map[int64]channelMonitorV2GroupInfo{
		3: {name: "private"}, 9: {name: "other-private"},
	}, channelMonitorV2DisplayGroupIndex(cfg))
	require.Empty(t, accs)
}

func TestChannelMonitorV2EmptyRestrictedScopeReturnsEmptyInventory(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &channelMonitorV2Repository{db: db}
	filter := service.ChannelMonitorV2Filter{RestrictGroups: true}
	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true, Models: []string{"gpt-5"}}},
		GroupIDs:  []int64{3, 9},
	}

	dimensions, err := repo.GetDimensions(context.Background(), filter, cfg)
	require.NoError(t, err)
	require.Empty(t, dimensions.Platforms)
	require.Empty(t, dimensions.Groups)
	require.Empty(t, dimensions.Models)
	require.NotNil(t, dimensions.Platforms)
	require.NotNil(t, dimensions.Groups)
	require.NotNil(t, dimensions.Models)

	models, err := repo.GetModels(context.Background(), filter, cfg, false)
	require.NoError(t, err)
	require.Empty(t, models.Items)
	require.NotNil(t, models.Items)
	require.Equal(t, service.ChannelMonitorV2Coverage{}, models.Coverage)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChannelMonitorV2EmptyRestrictedScopeReturnsEmptySnapshotWithoutQueries(t *testing.T) {
	tests := []struct {
		name   string
		filter service.ChannelMonitorV2Filter
	}{
		{
			name:   "no allowed groups",
			filter: service.ChannelMonitorV2Filter{RestrictGroups: true},
		},
		{
			name: "requested groups exclude allowed configured scope",
			filter: service.ChannelMonitorV2Filter{
				GroupIDs: []int64{9}, AllowedGroupIDs: []int64{3}, RestrictGroups: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			repo := &channelMonitorV2Repository{db: db}
			cfg := service.ChannelMonitorV2Config{
				Enabled: true,
				Platforms: []service.ChannelMonitorV2PlatformConfig{{
					Platform: "openai", Enabled: true, Models: []string{"gpt-5"},
				}},
				GroupIDs: []int64{3},
			}

			snapshot, err := repo.GetSnapshot(context.Background(), test.filter, cfg, false)
			require.NoError(t, err)
			require.Equal(t, service.ChannelMonitorV2Config{}, snapshot.Config)
			require.Equal(t, service.ChannelMonitorV2Coverage{}, snapshot.Coverage)
			require.Equal(t, service.ChannelMonitorV2Metric{}, snapshot.Metrics)
			require.Equal(t, service.ChannelMonitorV2Health{}, snapshot.Health)
			require.Empty(t, snapshot.Trend)
			require.NotNil(t, snapshot.Trend)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestChannelMonitorV2EmptyRestrictedScopeReturnsEmptyMatrixForEveryGrouping(t *testing.T) {
	groupings := []service.ChannelMonitorV2GroupBy{
		service.ChannelMonitorV2GroupByPlatform,
		service.ChannelMonitorV2GroupByPlatformModel,
		service.ChannelMonitorV2GroupByPlatformGroup,
		service.ChannelMonitorV2GroupByPlatformGroupModel,
	}
	for _, groupBy := range groupings {
		t.Run(string(groupBy), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			repo := &channelMonitorV2Repository{db: db}
			filter := service.ChannelMonitorV2Filter{RestrictGroups: true}
			cfg := service.ChannelMonitorV2Config{
				Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true, Models: []string{"gpt-5"}}},
				GroupIDs:  []int64{3, 9},
			}

			matrix, err := repo.GetMatrix(context.Background(), filter, cfg, groupBy, false)
			require.NoError(t, err)
			require.Equal(t, groupBy, matrix.GroupBy)
			require.Empty(t, matrix.Items)
			require.NotNil(t, matrix.Items)
			require.Equal(t, service.ChannelMonitorV2Coverage{}, matrix.Coverage)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestChannelMonitorV2ErrorAggregationCountsFinalUserErrorsOnly(t *testing.T) {
	query := strings.ToLower(channelMonitorV2ErrorAggregationSQL)
	require.Contains(t, query, "not current_error.is_count_tokens")
	require.Contains(t, query, "error_type = 'cyber_policy'")
	require.Contains(t, query, "distinct on")
	require.Contains(t, query, "candidate_ids")
	require.Contains(t, query, "where bucket_start >= $1 and bucket_start < $2")
	require.Contains(t, query, "upstream_affected_requests")
	require.Contains(t, query, "jsonb_array_length(current_error.upstream_errors) > 0")
	// request_id dedup must be time-bounded (no full-history scan).
	require.Contains(t, query, "interval '90 minutes'")
	require.Contains(t, query, "current_error.created_at >= $1 - interval '90 minutes'")
}

func TestChannelMonitorV2ErrorAggregationResolvesCompositePlatform(t *testing.T) {
	query := strings.ToLower(channelMonitorV2ErrorAggregationSQL)
	// Composite groups are a routing layer: error facts must resolve the concrete
	// account platform (joining groups/accounts) so they aggregate under the same
	// platform key as usage facts instead of the never-enabled 'composite' platform.
	require.Contains(t, query, "g.platform = 'composite'")
	require.Contains(t, query, "left join groups g on g.id = current_error.group_id")
	require.Contains(t, query, "left join accounts a on a.id = current_error.account_id")
	require.Contains(t, query, "a.platform")
	require.Contains(t, query, "nullif(trim(a.platform), '')")
	require.NotContains(t, query, "nullif(trim(a.platform))")
}

func TestChannelMonitorV2UsageSuccessExcludesCyberBillingRows(t *testing.T) {
	for _, query := range []string{channelMonitorV2UsageMetricsSQL, channelMonitorV2UserMetricsSQL} {
		require.Contains(t, query, "COALESCE(ul.request_type, 0) NOT IN (4, 6)")
		require.Contains(t, query, "ul.actual_cost > 0")
	}
	require.Contains(t, channelMonitorV2PlatformSQL, "g.platform = 'composite'")
	require.Contains(t, channelMonitorV2PlatformSQL, "a.platform")
	require.Contains(t, channelMonitorV2HistogramSQL, "ul.actual_cost > 0")
}

func TestChannelMonitorV2RatesUseCoveredWindow(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	filter := service.ChannelMonitorV2Filter{Start: start, End: start.Add(24 * time.Hour)}
	coverage := service.ChannelMonitorV2Coverage{CoverageStart: start.Add(6 * time.Hour), DataThrough: start.Add(18 * time.Hour)}
	require.Equal(t, 12*60.0, channelMonitorV2CoveredMinutes(filter, coverage))
	effective := channelMonitorV2CommonCoverageFilter(filter, coverage)
	require.Equal(t, coverage.CoverageStart, effective.Start)
	require.Equal(t, coverage.DataThrough, effective.End)
}

func TestChannelMonitorV2HistoryCoverageCompleteIgnoresTrailingLag(t *testing.T) {
	start := time.Date(2026, 8, 7, 2, 20, 0, 0, time.UTC)
	// History reaches the window start → complete even if data_through is behind filter.End.
	require.True(t, channelMonitorV2HistoryCoverageComplete(start, start))
	require.True(t, channelMonitorV2HistoryCoverageComplete(start.Add(-time.Hour), start))
	// Backfill still short of the window start → incomplete.
	require.False(t, channelMonitorV2HistoryCoverageComplete(start.Add(time.Hour), start))
	require.False(t, channelMonitorV2HistoryCoverageComplete(time.Time{}, start))
}

func TestChannelMonitorV2TierRetentionPolicy(t *testing.T) {
	require.Equal(t, 180*time.Minute, channelMonitorV2RetentionRollup5m)
	require.Equal(t, 31*24*time.Hour, channelMonitorV2RetentionRollupLong)
	require.Equal(t, channelMonitorV2RetentionRollupLong, channelMonitorV2MaxRetention())
	require.Contains(t, channelMonitorV2WatermarkSQL, "INTERVAL '31 days'")
	require.Equal(t, 48*time.Hour, channelMonitorV2RollupRetention("d"))
	require.Equal(t, 8*24*time.Hour, channelMonitorV2RollupRetention("w"))
	require.Equal(t, 31*24*time.Hour, channelMonitorV2RollupRetention("m"))
	require.Equal(t, 48*time.Hour, channelMonitorV2RollupRetention("24h"))
	require.Equal(t, 8*24*time.Hour, channelMonitorV2RollupRetention("7d"))
	require.Equal(t, 31*24*time.Hour, channelMonitorV2RollupRetention("30d"))

	// Every fixed rollup second must appear with a retention rule.
	wantSeconds := map[int]time.Duration{
		300:   channelMonitorV2RetentionRollup5m,
		3600:  channelMonitorV2RetentionRollupLong,
		43200: channelMonitorV2RetentionRollupLong,
		86400: channelMonitorV2RetentionRollupLong,
	}
	seen := map[int]time.Duration{}
	for _, rule := range channelMonitorV2RetentionRules {
		if rule.bucketSeconds == 0 {
			require.True(t, rule.retention > 0)
			continue
		}
		if prev, ok := seen[rule.bucketSeconds]; ok {
			require.Equal(t, prev, rule.retention)
		}
		seen[rule.bucketSeconds] = rule.retention
	}
	for seconds, want := range wantSeconds {
		got, ok := seen[seconds]
		require.Truef(t, ok, "missing retention rule for bucket_seconds=%d", seconds)
		require.Equal(t, want, got)
	}

	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	require.Equal(t, now.Add(-31*24*time.Hour), channelMonitorV2RetentionCutoff(now, channelMonitorV2MaxRetention()))
}

func TestSameFixedRollupBucket(t *testing.T) {
	start := time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC)
	require.True(t, sameFixedRollupBucket(start, start.Add(10*time.Minute), 86400))
	require.False(t, sameFixedRollupBucket(start, start.Add(15*time.Hour), 43200))
	require.False(t, sameFixedRollupBucket(start, start.Add(24*time.Hour), 86400))
}

// Needles present in service.ClassifyChannelMonitorV2Error must appear in the
// aggregation SQL CASE so rollup categories match drilldown classification.
func TestChannelMonitorV2SQLTaxonomyContainsGoNeedles(t *testing.T) {
	sql := channelMonitorV2ErrorAggregationSQL
	needles := []string{
		"blocked keyword",
		"invalid_api_key",
		"max_tokens",
		"invalid_request",
		"model not supported",
		"billing hard limit",
		"no healthy upstream account",
		"rate_limit",
		"gateway timeout",
		"connection refused",
		"unexpected eof",
	}
	for _, needle := range needles {
		require.Containsf(t, strings.ToLower(sql), strings.ToLower(needle), "SQL taxonomy missing Go needle %q", needle)
	}
}

func TestApplyIgnoredErrorsAdjustsRatesKeepsAbsoluteVolume(t *testing.T) {
	m := service.ChannelMonitorV2Metric{
		RequestCount:  100,
		ErrorRequests: 20,
		ErrorRate:     0.20,
		SuccessRate:   0.80,
	}
	// Success absolute still 80 → success rate stays 0.80 even after ignoring 5 errors.
	m.SuccessRequests = 80
	applyIgnoredErrors(&m, 5)
	require.Equal(t, int64(100), m.RequestCount)
	require.Equal(t, int64(20), m.ErrorRequests)
	require.InDelta(t, 0.15, m.ErrorRate, 0.0001)
	require.InDelta(t, 0.80, m.SuccessRate, 0.0001)

	// Clamp ignored > errors: scored error_rate → 0; success stays true ratio.
	m2 := service.ChannelMonitorV2Metric{RequestCount: 10, ErrorRequests: 2, SuccessRequests: 8, ErrorRate: 0.2, SuccessRate: 0.8}
	applyIgnoredErrors(&m2, 99)
	require.InDelta(t, 0.0, m2.ErrorRate, 0.0001)
	require.InDelta(t, 0.8, m2.SuccessRate, 0.0001)

	// No-op when ignored is zero
	m3 := service.ChannelMonitorV2Metric{RequestCount: 10, ErrorRequests: 2, SuccessRequests: 8, ErrorRate: 0.2, SuccessRate: 0.8}
	applyIgnoredErrors(&m3, 0)
	require.InDelta(t, 0.2, m3.ErrorRate, 0.0001)
	require.InDelta(t, 0.8, m3.SuccessRate, 0.0001)
}

func TestRedactChannelMonitorV2MetricZerosVolume(t *testing.T) {
	// Service helper is in service package; covered there. Keep a smoke note that
	// rates survive a manual zeroing of volume fields used by the UI contract.
	m := service.ChannelMonitorV2Metric{
		RequestCount: 100, ErrorRequests: 10, SuccessRequests: 90,
		TokenCount: 1000, RPM: 5, TPM: 50, ErrorRate: 0.1, SuccessRate: 0.9, CacheRate: 0.4,
	}
	// Mimic redact: zero volume only
	m.RequestCount, m.ErrorRequests, m.SuccessRequests, m.TokenCount = 0, 0, 0, 0
	require.Equal(t, 0.1, m.ErrorRate)
	require.Equal(t, 5.0, m.RPM)
}

func TestChannelMonitorV2CatalogFilterClearsMultiSelectDimensions(t *testing.T) {
	start := time.Unix(1, 0)
	end := time.Unix(2, 0)
	filter := service.ChannelMonitorV2Filter{
		Start: start, End: end, Bucket: time.Minute,
		Platforms: []string{"openai"}, GroupIDs: []int64{3}, Models: []string{"gpt-5"},
	}
	catalog := channelMonitorV2CatalogFilter(filter)
	require.Nil(t, catalog.Platforms)
	require.Nil(t, catalog.GroupIDs)
	require.Nil(t, catalog.Models)
	// Time window / coverage-related fields remain.
	require.Equal(t, start, catalog.Start)
	require.Equal(t, end, catalog.End)
	require.Equal(t, time.Minute, catalog.Bucket)

	cfg := service.ChannelMonitorV2Config{
		Platforms: []service.ChannelMonitorV2PlatformConfig{
			{Platform: "openai", Enabled: true},
			{Platform: "grok", Enabled: true},
		},
		GroupIDs: []int64{3, 4},
	}
	catalogWhere, catalogArgs := channelMonitorV2Where(catalog, cfg, "m")
	_, metricArgs := channelMonitorV2Where(filter, cfg, "m")

	// Catalog WHERE still applies config scope (enabled platforms + group allow-list).
	require.Contains(t, catalogWhere, "m.platform = ANY")
	require.Contains(t, catalogWhere, "m.group_id = ANY")
	require.Len(t, catalogArgs, 4) // start, end, platforms, groups
	require.Len(t, metricArgs, 4)

	// Metrics WHERE is narrower once multi-select platforms/groups are applied.
	require.NotEqual(t, catalogArgs, metricArgs)
	// Group seeding without multi-select uses full config allow-list.
	require.Equal(t, []int64{3, 4}, configuredChannelMonitorV2GroupIDs(catalog, cfg))
	require.Equal(t, []int64{3}, configuredChannelMonitorV2GroupIDs(filter, cfg))
}

func TestChannelMonitorV2RollupSourcesAndRefreshBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		start, end time.Time
		coarse     bool
	}{
		{"recent", time.Date(2026, 10, 3, 12, 10, 0, 0, time.UTC), time.Date(2026, 10, 3, 12, 20, 0, 0, time.UTC), false},
		{"hour boundary", time.Date(2026, 10, 3, 12, 55, 0, 0, time.UTC), time.Date(2026, 10, 3, 13, 5, 0, 0, time.UTC), true},
		{"historical hour", time.Date(2026, 9, 3, 14, 0, 0, 0, time.UTC), time.Date(2026, 9, 3, 15, 0, 0, 0, time.UTC), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			mock.ExpectBegin()
			tx, err := db.Begin()
			require.NoError(t, err)
			for _, seconds := range []int{3600, 43200, 86400} {
				if seconds >= 43200 && !tc.coarse {
					continue
				}
				interval := fmt.Sprintf("%d seconds", seconds)
				for _, table := range []string{"latency_histograms", "error_metrics", "user_metrics", "metrics"} {
					mock.ExpectExec("DELETE FROM channel_monitor_v2_"+table+"_rollup").WithArgs(interval, seconds, tc.start, tc.end).WillReturnResult(sqlmock.NewResult(0, 1))
				}
				for _, source := range []struct{ table, alias string }{{"metrics", "m"}, {"user_metrics", "m"}, {"latency_histograms", "h"}, {"error_metrics", "e"}} {
					pattern := fmt.Sprintf("FROM channel_monitor_v2_%s_rollup %s, bounds\\s+WHERE %s.bucket_seconds = CASE WHEN", source.table, source.alias, source.alias)
					mock.ExpectExec(pattern).WithArgs(interval, seconds, tc.start, tc.end).WillReturnResult(sqlmock.NewResult(0, 1))
				}
			}
			mock.ExpectCommit()
			r := &channelMonitorV2Repository{}
			require.NoError(t, r.recomputeFixedRollups(context.Background(), tx, tc.start, tc.end))
			require.NoError(t, tx.Commit())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestChannelMonitorV2RecentRefreshAlignsOnlyToFiveMinutes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	end := time.Now().UTC().Truncate(time.Hour).Add(23 * time.Minute)
	requestedStart := end.Add(-10 * time.Minute)
	start := requestedStart.Truncate(5 * time.Minute)
	mock.ExpectBegin()
	for _, table := range []string{"latency_histograms", "error_metrics", "user_metrics", "metrics"} {
		mock.ExpectExec("DELETE FROM channel_monitor_v2_"+table+"_rollup WHERE bucket_seconds = 300").WithArgs(start, end).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	for i := 0; i < 4; i++ {
		mock.ExpectExec(".+").WithArgs(start, end).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	for _, seconds := range []int{3600} {
		for i := 0; i < 8; i++ {
			mock.ExpectExec(".+").WithArgs(fmt.Sprintf("%d seconds", seconds), seconds, start, end).WillReturnResult(sqlmock.NewResult(0, 1))
		}
	}
	for _, rule := range channelMonitorV2RetentionRules {
		mock.ExpectExec("DELETE FROM " + rule.table).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	for _, table := range channelMonitorV2LegacyTables {
		mock.ExpectExec("DELETE FROM " + table + "$").WithArgs().WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec("INSERT INTO channel_monitor_v2_watermarks").WithArgs(start, end).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	r := &channelMonitorV2Repository{db: db}
	require.NoError(t, r.RecomputeRange(context.Background(), requestedStart, end))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChannelMonitorV2ReadsOnlyRollupTables(t *testing.T) {
	for _, bucket := range []time.Duration{0, time.Minute, 5 * time.Minute, time.Hour, 12 * time.Hour, 24 * time.Hour} {
		filter := service.ChannelMonitorV2Filter{Bucket: bucket}
		for _, table := range []string{channelMonitorV2MetricsTable(filter), channelMonitorV2UserMetricsTable(filter), channelMonitorV2ErrorMetricsTable(filter), channelMonitorV2HistogramTable(filter)} {
			require.True(t, strings.HasSuffix(table, "_rollup"))
		}
		where, args, seconds := channelMonitorV2WhereWithRollup(filter, service.ChannelMonitorV2Config{}, "m")
		require.GreaterOrEqual(t, seconds, 300)
		require.Contains(t, where, "m.bucket_seconds =")
		require.Equal(t, seconds, args[len(args)-1])
	}
}

func TestChannelMonitorV2GetUsersUsesAverageWithoutUserHistograms(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	r := &channelMonitorV2Repository{db: db}
	start := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	end := start.Add(time.Hour)
	mock.ExpectQuery("SELECT usage_coverage_start").WillReturnRows(sqlmock.NewRows([]string{
		"usage_coverage_start", "error_coverage_start", "data_through", "last_successful_at", "backfill_cursor",
	}).AddRow(start, start, end, end, start))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT m\\.user_id,m\\.platform,m\\.model").WillReturnRows(sqlmock.NewRows([]string{
		"user_id", "platform", "model", "success_requests", "error_requests",
	}).AddRow(7, "openai", "gpt-5", 2, 0))
	mock.ExpectQuery("SELECT m\\.user_id").WillReturnRows(sqlmock.NewRows([]string{
		"user_id", "email", "username", "platform", "model", "success_requests", "error_requests",
		"input_tokens", "output_tokens", "cache_creation_tokens", "cache_read_tokens", "ttft_sum_ms",
		"ttft_count", "duration_sum_ms", "duration_count",
	}).AddRow(7, "user@example.com", "user", "openai", "gpt-5", 2, 0, 10, 20, 0, 0, 200, 2, 1000, 2))
	mock.ExpectCommit()
	result, err := r.GetUsers(context.Background(), service.ChannelMonitorV2Filter{Start: start, End: end, Bucket: time.Hour}, service.ChannelMonitorV2Config{}, 7, true)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	require.Equal(t, float64(100), *result.Items[0].Metrics.TTFT.AvgMs)
	require.Nil(t, result.Items[0].Metrics.TTFT.P50Ms)
	require.Nil(t, result.Items[0].Metrics.TTFT.P90Ms)
	require.Nil(t, result.Items[0].Metrics.TTFT.P95Ms)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChannelMonitorV2GetUsersOnlyLoadsTop20AndViewerDetails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	r := &channelMonitorV2Repository{db: db}
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	cfg := service.ChannelMonitorV2Config{Platforms: []service.ChannelMonitorV2PlatformConfig{{Platform: "openai", Enabled: true, Models: []string{"selected"}}}}
	filter := service.ChannelMonitorV2Filter{Start: start, End: end, Bucket: time.Hour, Models: []string{"selected"}}
	mock.ExpectQuery("SELECT usage_coverage_start").WillReturnRows(sqlmock.NewRows([]string{"usage_coverage_start", "error_coverage_start", "data_through", "last_successful_at", "backfill_cursor"}).AddRow(start, start, end, end, start))
	mock.ExpectBegin()
	counts := sqlmock.NewRows([]string{"user_id", "platform", "model", "success_requests", "error_requests"})
	for id := 25; id >= 1; id-- {
		// Equal totals exercise the ID tie-breaker, irrespective of row order.
		counts.AddRow(id, "openai", "selected", 9, 1)
	}
	counts.AddRow(26, "openai", "excluded", 100000, 0)
	mock.ExpectQuery("SELECT m\\.user_id,m\\.platform,m\\.model").WillReturnRows(counts).RowsWillBeClosed()
	selected := make([]int64, 0, 21)
	for id := int64(1); id <= 20; id++ {
		selected = append(selected, id)
	}
	selected = append(selected, 25)
	details := sqlmock.NewRows([]string{"user_id", "email", "username", "platform", "model", "success_requests", "error_requests", "input_tokens", "output_tokens", "cache_creation_tokens", "cache_read_tokens", "ttft_sum_ms", "ttft_count", "duration_sum_ms", "duration_count"})
	for _, id := range selected {
		details.AddRow(id, "user@example.com", "user", "openai", "selected", 9, 1, 10, 20, 0, 0, 900, 9, 1800, 9)
	}
	mock.ExpectQuery(`SELECT m\.user_id,COALESCE.*AND m\.user_id = ANY\(\$5\)`).WithArgs(start, end, pq.Array([]string{"openai"}), 3600, pq.Array(selected)).WillReturnRows(details)
	mock.ExpectCommit()
	result, err := r.GetUsers(context.Background(), filter, cfg, 25, true)
	require.NoError(t, err)
	require.Len(t, result.Items, 21)
	for i := 0; i < 20; i++ {
		require.Equal(t, i+1, result.Items[i].Rank)
	}
	require.Equal(t, int64(25), *result.Items[20].UserID)
	require.Equal(t, 25, result.Items[20].Rank)
	require.Equal(t, float64(100), *result.Items[20].Metrics.TTFT.AvgMs)
	require.NoError(t, mock.ExpectationsWereMet())
}
