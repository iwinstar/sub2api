package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelMonitorV2PublicCacheAbsentStillLimitsLoad(t *testing.T) {
	want := errors.New("limited")
	ctx := WithChannelMonitorV2LoadLimit(context.Background(), func(context.Context) error { return want })
	called := false
	_, err := channelMonitorV2CachedResponse(ctx, nil, "uncached", func(context.Context) (*ChannelMonitorV2Dimensions, error) {
		called = true
		return &ChannelMonitorV2Dimensions{}, nil
	})
	require.ErrorIs(t, err, want)
	require.False(t, called)
}

func TestChannelMonitorV2PublicCacheLimitsOnlyLoads(t *testing.T) {
	cache := newChannelMonitorV2PublicCache()
	require.NotNil(t, cache)
	defer cache.entries.Close()
	var checks atomic.Int32
	var loads atomic.Int32
	ctx := WithChannelMonitorV2LoadLimit(context.Background(), func(context.Context) error {
		checks.Add(1)
		return nil
	})
	load := func(context.Context) (*ChannelMonitorV2Dimensions, error) {
		loads.Add(1)
		return &ChannelMonitorV2Dimensions{}, nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := channelMonitorV2CachedResponse(ctx, cache, "same-key", load)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, checks.Load())
	require.EqualValues(t, 1, loads.Load())
	_, err := channelMonitorV2CachedResponse(ctx, cache, "second-key", load)
	require.NoError(t, err)
	require.EqualValues(t, 2, checks.Load())
	require.EqualValues(t, 2, loads.Load())
}

func TestChannelMonitorV2PublicCacheKeyScopesAndVersion(t *testing.T) {
	filter := ChannelMonitorV2Filter{
		Range: "120m", RestrictGroups: true, AllowedGroupIDs: []int64{3, 2, 3},
		Start: time.Unix(900, 0).UTC(), End: time.Unix(8100, 0).UTC(), Bucket: 5 * time.Minute,
	}
	cfg := ChannelMonitorV2Config{Version: 1}
	key := channelMonitorV2PublicCacheKey("matrix", filter, cfg, ChannelMonitorV2GroupByPlatformGroup, true)
	filter.AllowedGroupIDs = []int64{2, 3}
	require.Equal(t, key, channelMonitorV2PublicCacheKey("matrix", filter, cfg, ChannelMonitorV2GroupByPlatformGroup, true))
	filter.Start = filter.Start.Add(5 * time.Minute)
	filter.End = filter.End.Add(5 * time.Minute)
	require.NotEqual(t, key, channelMonitorV2PublicCacheKey("matrix", filter, cfg, ChannelMonitorV2GroupByPlatformGroup, true))
	filter.Start = filter.Start.Add(-5 * time.Minute)
	filter.End = filter.End.Add(-5 * time.Minute)
	filter.AllowedGroupIDs = []int64{2}
	require.NotEqual(t, key, channelMonitorV2PublicCacheKey("matrix", filter, cfg, ChannelMonitorV2GroupByPlatformGroup, true))
	filter.AllowedGroupIDs = []int64{2, 3}
	cfg.Version++
	require.NotEqual(t, key, channelMonitorV2PublicCacheKey("matrix", filter, cfg, ChannelMonitorV2GroupByPlatformGroup, true))
}

func TestChannelMonitorV2PublicCacheReturnsIndependentResults(t *testing.T) {
	cache := newChannelMonitorV2PublicCache()
	require.NotNil(t, cache)
	defer cache.entries.Close()
	calls := 0
	load := func(context.Context) (*ChannelMonitorV2Dimensions, error) {
		calls++
		return &ChannelMonitorV2Dimensions{Groups: []ChannelMonitorV2GroupDimension{{ID: 1, Name: "one"}}}, nil
	}
	first, err := channelMonitorV2Cached(context.Background(), cache, "dimensions", load)
	require.NoError(t, err)
	first.Groups[0].Name = "changed"
	second, err := channelMonitorV2Cached(context.Background(), cache, "dimensions", load)
	require.NoError(t, err)
	require.Equal(t, "one", second.Groups[0].Name)
	require.Equal(t, 1, calls)
}

func TestChannelMonitorV2PublicCacheReturnsCompleteResponse(t *testing.T) {
	cache := newChannelMonitorV2PublicCache()
	require.NotNil(t, cache)
	defer cache.entries.Close()
	calls := 0
	load := func(context.Context) (*ChannelMonitorV2Dimensions, error) {
		calls++
		return &ChannelMonitorV2Dimensions{Groups: []ChannelMonitorV2GroupDimension{{ID: 1, Name: "one"}}}, nil
	}
	first, err := channelMonitorV2CachedResponse(context.Background(), cache, "dimensions", load)
	require.NoError(t, err)
	second, err := channelMonitorV2CachedResponse(context.Background(), cache, "dimensions", load)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 1, calls)
	require.Contains(t, string(second), `"code":0,"message":"success","data":{`)
	result, err := channelMonitorV2DecodeResponse[ChannelMonitorV2Dimensions](second, nil)
	require.NoError(t, err)
	require.Equal(t, "one", result.Groups[0].Name)
}

func TestChannelMonitorV2PublicCacheNilResultMatchesUncachedResponse(t *testing.T) {
	cache := newChannelMonitorV2PublicCache()
	require.NotNil(t, cache)
	defer cache.entries.Close()
	load := func(context.Context) (*ChannelMonitorV2Dimensions, error) { return nil, nil }
	want, err := channelMonitorV2CachedResponse(context.Background(), nil, "nil", load)
	require.NoError(t, err)
	got, err := channelMonitorV2CachedResponse(context.Background(), cache, "nil", load)
	require.NoError(t, err)
	require.JSONEq(t, `{"code":0,"message":"success","data":null}`, string(got))
	require.Equal(t, want, got)
	again, err := channelMonitorV2CachedResponse(context.Background(), cache, "nil", load)
	require.NoError(t, err)
	require.Equal(t, got, again)
}

func TestChannelMonitorV2PublicCacheRecoversLoaderPanic(t *testing.T) {
	cache := newChannelMonitorV2PublicCache()
	require.NotNil(t, cache)
	defer cache.entries.Close()
	_, err := channelMonitorV2Cached(context.Background(), cache, "panic", func(context.Context) (*ChannelMonitorV2Dimensions, error) {
		panic("query failed")
	})
	require.ErrorContains(t, err, "channel monitor v2 public cache load panicked: query failed")
	result, err := channelMonitorV2Cached(context.Background(), cache, "panic", func(context.Context) (*ChannelMonitorV2Dimensions, error) {
		return &ChannelMonitorV2Dimensions{}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestChannelMonitorV2DimensionsPublicCacheDoesNotRedactAdmin(t *testing.T) {
	repo := &channelMonitorV2RepoStub{
		config:     ChannelMonitorV2Config{Enabled: true, Version: 1},
		dimensions: &ChannelMonitorV2Dimensions{Platforms: []ChannelMonitorV2Dimension{{Value: "openai", RequestCount: 42}}},
	}
	svc := NewChannelMonitorV2Service(repo)
	require.NotNil(t, svc.publicCache)
	defer svc.publicCache.entries.Close()
	filter := ChannelMonitorV2Filter{Range: "24h", RestrictGroups: true, AllowedGroupIDs: []int64{1}, Start: time.Unix(0, 0), End: time.Unix(86400, 0), Bucket: time.Hour}
	public, err := svc.DimensionsForViewer(context.Background(), filter, false)
	require.NoError(t, err)
	require.Equal(t, int64(0), public.Platforms[0].RequestCount)
	public, err = svc.DimensionsForViewer(context.Background(), filter, false)
	require.NoError(t, err)
	require.Equal(t, int64(0), public.Platforms[0].RequestCount)
	require.Equal(t, 1, repo.dimensionCalls)
	admin, err := svc.DimensionsForViewer(context.Background(), filter, true)
	require.NoError(t, err)
	require.Equal(t, int64(42), admin.Platforms[0].RequestCount)
	require.Equal(t, 2, repo.dimensionCalls)
}
