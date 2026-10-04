package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type channelMonitorV2GroupAuthorizerStub struct {
	groups []service.Group
	err    error
	calls  []int64
}

func (s *channelMonitorV2GroupAuthorizerStub) GetAvailableGroups(_ context.Context, userID int64) ([]service.Group, error) {
	s.calls = append(s.calls, userID)
	return s.groups, s.err
}

func TestChannelMonitorV2QueryListSupportsRepeatedAndCommaValues(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	c.Request = httptest.NewRequest("GET", "/?platform=openai,grok&platform=anthropic", nil)
	require.Equal(t, []string{"openai", "grok", "anthropic"}, queryList(c, "platform"))
}

func TestChannelMonitorV2GroupByQueryDefaultsAndRejectsInvalid(t *testing.T) {
	groupBy, err := service.ParseChannelMonitorV2GroupBy("")
	require.NoError(t, err)
	require.Equal(t, service.ChannelMonitorV2GroupByPlatformGroup, groupBy)
	_, err = service.ParseChannelMonitorV2GroupBy("invalid")
	require.Error(t, err)
}

func TestChannelMonitorV2MatrixHandlerRejectsInvalidGroupBy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/channel-monitor-v2/matrix?group_by=invalid", nil)
	h := NewChannelMonitorV2Handler(service.NewChannelMonitorV2Service(nil), nil)
	h.Matrix(c)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestChannelMonitorV2ScopeFilterUsesAvailableGroupsForOrdinaryUser(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/channel-monitor-v2/snapshot", nil)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 42})
	authorizer := &channelMonitorV2GroupAuthorizerStub{groups: []service.Group{{ID: 3}, {ID: 7}}}
	h := &ChannelMonitorV2Handler{apiKeyService: authorizer}
	filter := service.ChannelMonitorV2Filter{GroupIDs: []int64{7, 9}}

	require.True(t, h.scopeFilter(c, &filter, false))
	require.True(t, filter.RestrictGroups)
	require.Equal(t, []int64{3, 7}, filter.AllowedGroupIDs)
	require.Equal(t, []int64{42}, authorizer.calls)
}

func TestChannelMonitorV2ScopeFilterPreservesEmptyOrdinaryUserScope(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/channel-monitor-v2/snapshot", nil)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 42})
	h := &ChannelMonitorV2Handler{apiKeyService: &channelMonitorV2GroupAuthorizerStub{}}
	filter := service.ChannelMonitorV2Filter{}

	require.True(t, h.scopeFilter(c, &filter, false))
	require.True(t, filter.RestrictGroups)
	require.Empty(t, filter.AllowedGroupIDs)
}

func TestChannelMonitorV2ScopeFilterLeavesAdminUnrestricted(t *testing.T) {
	authorizer := &channelMonitorV2GroupAuthorizerStub{}
	h := &ChannelMonitorV2Handler{apiKeyService: authorizer}
	filter := service.ChannelMonitorV2Filter{GroupIDs: []int64{9}}

	require.True(t, h.scopeFilter(nil, &filter, true))
	require.False(t, filter.RestrictGroups)
	require.Nil(t, filter.AllowedGroupIDs)
	require.Empty(t, authorizer.calls)
}

type ranking20Repo struct {
	service.ChannelMonitorV2Repository
}

func (r *ranking20Repo) GetConfig(context.Context) (*service.ChannelMonitorV2Config, error) {
	return &service.ChannelMonitorV2Config{Enabled: true}, nil
}
func (r *ranking20Repo) GetUsers(context.Context, service.ChannelMonitorV2Filter, service.ChannelMonitorV2Config, int64, bool) (*service.ChannelMonitorV2List[service.ChannelMonitorV2UserRow], error) {
	out := &service.ChannelMonitorV2List[service.ChannelMonitorV2UserRow]{}
	for i := int64(1); i <= 25; i++ {
		id := i
		out.Items = append(out.Items, service.ChannelMonitorV2UserRow{UserID: &id, DisplayLabel: "user", Metrics: service.ChannelMonitorV2Metric{RequestCount: 100 - i}})
	}
	return out, nil
}
func TestChannelMonitorV2Top20AndAdminLookup(t *testing.T) {
	for _, tc := range []struct {
		name, query   string
		admin         bool
		status, count int
		wantID        int64
		self          bool
	}{
		{"top20 plus self", "", true, 200, 21, 25, true},
		{"lookup outside top20", "?username=24", true, 200, 1, 24, false},
		{"lookup self", "?username=25", true, 200, 1, 25, true},
		{"no traffic", "?username=99", true, 200, 1, 99, false},
		{"ordinary lookup forbidden", "?username=24", false, 403, 0, 0, false},
		{"invalid ID", "?username=-1", true, 404, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("GET", "/users"+tc.query, nil)
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 25})
			h := &ChannelMonitorV2Handler{service: service.NewChannelMonitorV2Service(&ranking20Repo{}), apiKeyService: &channelMonitorV2GroupAuthorizerStub{}}
			h.users(c, tc.admin)
			require.Equal(t, tc.status, rec.Code)
			if tc.status != 200 {
				return
			}
			var body struct {
				Data service.ChannelMonitorV2List[service.ChannelMonitorV2UserRow] `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Data.Items, tc.count)
			row := body.Data.Items[len(body.Data.Items)-1]
			require.Equal(t, tc.wantID, *row.UserID)
			require.Equal(t, tc.self, row.IsSelf)
			if tc.wantID == 99 {
				require.Zero(t, row.Rank)
				require.Equal(t, "99", row.DisplayLabel)
			}
		})
	}
}

func (r *ranking20Repo) FindUserIDByUsernameOrEmail(_ context.Context, name string) (int64, error) {
	id, err := strconv.ParseInt(name, 10, 64)
	if err != nil || id <= 0 {
		return 0, service.ErrUserNotFound
	}
	return id, nil
}
