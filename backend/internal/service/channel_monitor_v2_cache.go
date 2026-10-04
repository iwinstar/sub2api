package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/dgraph-io/ristretto"
	"golang.org/x/sync/singleflight"
)

const (
	channelMonitorV2CacheTTL     = 15 * time.Second
	channelMonitorV2CacheMaxCost = 16 << 20
	channelMonitorV2LoadTimeout  = 30 * time.Second
)

type channelMonitorV2PublicCache struct {
	entries *ristretto.Cache
	flight  singleflight.Group
}

type channelMonitorV2LoadLimitKey struct{}

// WithChannelMonitorV2LoadLimit runs the deferred heavy limit before a real cache load.
func WithChannelMonitorV2LoadLimit(ctx context.Context, check func(context.Context) error) context.Context {
	return context.WithValue(ctx, channelMonitorV2LoadLimitKey{}, check)
}

func newChannelMonitorV2PublicCache() *channelMonitorV2PublicCache {
	entries, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        10000,
		MaxCost:            channelMonitorV2CacheMaxCost,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		return nil
	}
	return &channelMonitorV2PublicCache{entries: entries}
}

func channelMonitorV2PublicCacheKey(kind string, filter ChannelMonitorV2Filter, cfg ChannelMonitorV2Config, groupBy ChannelMonitorV2GroupBy, hideThroughput bool) string {
	allowed := append([]int64(nil), filter.AllowedGroupIDs...)
	sort.Slice(allowed, func(i, j int) bool { return allowed[i] < allowed[j] })
	unique := allowed[:0]
	for _, id := range allowed {
		if len(unique) == 0 || unique[len(unique)-1] != id {
			unique = append(unique, id)
		}
	}
	key, _ := json.Marshal(struct {
		Kind           string                  `json:"kind"`
		Version        int                     `json:"version"`
		Range          string                  `json:"range"`
		Platforms      []string                `json:"platforms"`
		Models         []string                `json:"models"`
		Groups         []int64                 `json:"groups"`
		AllowedGroups  []int64                 `json:"allowed_groups"`
		RestrictGroups bool                    `json:"restrict_groups"`
		Start          time.Time               `json:"start"`
		End            time.Time               `json:"end"`
		Bucket         time.Duration           `json:"bucket"`
		GroupBy        ChannelMonitorV2GroupBy `json:"group_by"`
		HideThroughput bool                    `json:"hide_throughput"`
	}{kind, cfg.Version, filter.Range, filter.Platforms, filter.Models, filter.GroupIDs, unique,
		filter.RestrictGroups, filter.Start, filter.End, filter.Bucket, groupBy, hideThroughput})
	return string(key)
}

func channelMonitorV2Cached[T any](ctx context.Context, cache *channelMonitorV2PublicCache, key string, load func(context.Context) (*T, error)) (*T, error) {
	data, err := channelMonitorV2CachedResponse(ctx, cache, key, load)
	return channelMonitorV2DecodeResponse[T](data, err)
}

func channelMonitorV2DecodeResponse[T any](data []byte, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	var response struct {
		Data *T `json:"data"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	return response.Data, nil
}

func channelMonitorV2CachedResponse[T any](ctx context.Context, cache *channelMonitorV2PublicCache, key string, load func(context.Context) (*T, error)) ([]byte, error) {
	if cache == nil {
		if check, ok := ctx.Value(channelMonitorV2LoadLimitKey{}).(func(context.Context) error); ok {
			if err := check(ctx); err != nil {
				return nil, err
			}
		}
		result, err := load(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    *T     `json:"data"`
		}{0, "success", result})
	}
	if value, ok := cache.entries.Get(key); ok {
		data, valid := value.([]byte)
		if !valid {
			return nil, fmt.Errorf("channel monitor v2 public cache contains an invalid response type")
		}
		return data, nil
	}
	ch := cache.flight.DoChan(key, func() (value any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("channel monitor v2 public cache load panicked: %v", recovered)
			}
		}()
		if value, ok := cache.entries.Get(key); ok {
			return value, nil
		}
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), channelMonitorV2LoadTimeout)
		defer cancel()
		if check, ok := ctx.Value(channelMonitorV2LoadLimitKey{}).(func(context.Context) error); ok {
			if err := check(loadCtx); err != nil {
				return nil, err
			}
		}
		result, err := load(loadCtx)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    *T     `json:"data"`
		}{0, "success", result})
		if err != nil {
			return nil, err
		}
		if cache.entries.SetWithTTL(key, data, int64(len(data)), channelMonitorV2CacheTTL) {
			cache.entries.Wait()
		}
		return data, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		if result.Err != nil || result.Val == nil {
			return nil, result.Err
		}
		data, ok := result.Val.([]byte)
		if !ok {
			return nil, fmt.Errorf("channel monitor v2 public cache load returned an invalid response type")
		}
		return data, nil
	}
}
