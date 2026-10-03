package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChannelMonitorV2DisplayGroupsValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		groups    []ChannelMonitorV2DisplayGroup
		monitored []int64
		bad       bool
	}{
		{name: "empty"},
		{name: "valid", groups: []ChannelMonitorV2DisplayGroup{{ID: "gpt", Name: " GPT ", GroupIDs: []int64{2, 1}}}, monitored: []int64{1, 2}},
		{name: "empty name", groups: []ChannelMonitorV2DisplayGroup{{ID: "gpt", GroupIDs: []int64{1}}}, bad: true},
		{name: "empty members", groups: []ChannelMonitorV2DisplayGroup{{ID: "gpt", Name: "GPT"}}, bad: true},
		{name: "overlap", groups: []ChannelMonitorV2DisplayGroup{{ID: "a", Name: "A", GroupIDs: []int64{1}}, {ID: "b", Name: "B", GroupIDs: []int64{1}}}, bad: true},
		{name: "duplicate name", groups: []ChannelMonitorV2DisplayGroup{{ID: "a", Name: "A", GroupIDs: []int64{1}}, {ID: "b", Name: " A ", GroupIDs: []int64{2}}}, bad: true},
		{name: "duplicate id", groups: []ChannelMonitorV2DisplayGroup{{ID: "a", Name: "A", GroupIDs: []int64{1}}, {ID: "a", Name: "B", GroupIDs: []int64{2}}}, bad: true},
		{name: "outside monitoring", groups: []ChannelMonitorV2DisplayGroup{{ID: "gpt", Name: "GPT", GroupIDs: []int64{1, 2}}}, monitored: []int64{1}, bad: true},
		{name: "invalid member", groups: []ChannelMonitorV2DisplayGroup{{ID: "gpt", Name: "GPT", GroupIDs: []int64{-1}}}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ChannelMonitorV2Config{GroupIDs: tc.monitored, DisplayGroups: tc.groups}
			err := normalizeChannelMonitorV2Config(&cfg)
			if tc.bad {
				require.ErrorIs(t, err, ErrChannelMonitorV2InvalidConfig)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestChannelMonitorV2DisplayGroupMembersAreNotPublic(t *testing.T) {
	cfg := ChannelMonitorV2Config{DisplayGroups: []ChannelMonitorV2DisplayGroup{{ID: "gpt", Name: "Private", GroupIDs: []int64{1, 2}}}}
	redactChannelMonitorV2PublicConfig(&cfg)
	require.Nil(t, cfg.DisplayGroups)
}

func TestChannelMonitorV2SharedDisplayMatrixRetainsPublicRedaction(t *testing.T) {
	metric := ChannelMonitorV2Metric{RequestCount: 1000, SuccessRequests: 990, ErrorRequests: 10, InputTokens: 5000, RPM: 200, TPM: 1000, SuccessRate: .99, ErrorRate: .01, CacheRate: .5, TTFT: ChannelMonitorV2Latency{SampleCount: 990}}
	score := 95.0
	repo := &channelMonitorV2RepoStub{
		config: ChannelMonitorV2Config{Enabled: true},
		matrix: &ChannelMonitorV2Matrix{Items: []ChannelMonitorV2MatrixRow{{
			DisplayGroupID: "gpt", GroupName: "GPT", Metrics: metric,
			Health:  ChannelMonitorV2Health{Score: &score},
			Buckets: []ChannelMonitorV2TrendPoint{{Metrics: metric}},
		}}},
	}
	result, err := NewChannelMonitorV2Service(repo).Matrix(context.Background(), ChannelMonitorV2Filter{RestrictGroups: true, AllowedGroupIDs: []int64{1}}, ChannelMonitorV2GroupByPlatformGroup, false)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	row := result.Items[0]
	require.Equal(t, "gpt", row.DisplayGroupID)
	require.Nil(t, row.GroupID)
	require.Equal(t, score, *row.Health.Score)
	for _, m := range []ChannelMonitorV2Metric{row.Metrics, row.Buckets[0].Metrics} {
		require.Zero(t, m.RequestCount)
		require.Zero(t, m.SuccessRequests)
		require.Zero(t, m.ErrorRequests)
		require.Zero(t, m.InputTokens)
		require.Zero(t, m.RPM)
		require.Zero(t, m.TPM)
		require.Zero(t, m.TTFT.SampleCount)
		require.Equal(t, .99, m.SuccessRate)
		require.Equal(t, .5, m.CacheRate)
	}
}
