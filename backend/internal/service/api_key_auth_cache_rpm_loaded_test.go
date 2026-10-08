//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type rpmTargetRepoStub struct {
	rpmOverrideRepoStub
	groupID int64
}

func (s *rpmTargetRepoStub) GetRPMOverrideByUserAndGroup(ctx context.Context, userID, groupID int64) (*int, error) {
	s.groupID = groupID
	return s.rpmOverrideRepoStub.GetRPMOverrideByUserAndGroup(ctx, userID, groupID)
}

func TestRPMOverrideSnapshotLoadedGroup(t *testing.T) {
	positive, zero := 2, 0
	for _, tc := range []struct {
		name  string
		value *int
		err   error
	}{
		{"absent", nil, nil}, {"zero", &zero, nil}, {"positive", &positive, nil}, {"failed", nil, errors.New("unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &rpmOverrideRepoStub{override: tc.value, err: tc.err}
			auth := &APIKeyService{userGroupRateRepo: repo}
			key := profitAuthTestAPIKey()
			snap := auth.snapshotFromAPIKey(context.Background(), key)
			payload, err := json.Marshal(snap)
			require.NoError(t, err)
			var restored APIKeyAuthSnapshot
			require.NoError(t, json.Unmarshal(payload, &restored))
			user := auth.snapshotToAPIKey(key.Key, &restored).User
			expected := *key.GroupID
			if tc.err != nil {
				expected = 0
			}
			require.Equal(t, expected, user.UserGroupRPMOverrideGroupID)
			require.Equal(t, tc.value, user.UserGroupRPMOverride)
			cache := &userRPMCacheStub{}
			billing := newBillingServiceForRPM(t, cache, repo)
			require.NoError(t, billing.checkRPM(context.Background(), user, &Group{ID: *key.GroupID}))
			require.NoError(t, billing.checkRPM(context.Background(), user, &Group{ID: *key.GroupID}))
			if tc.err == nil {
				require.EqualValues(t, 1, repo.calls)
			} else {
				require.EqualValues(t, 3, repo.calls)
			}
		})
	}
}

func TestRPMOverrideSnapshotLegacyAndFallback(t *testing.T) {
	positive, zero, target := 100, 0, 1
	for _, value := range []*int{nil, &zero, &positive} {
		for _, loadedGroup := range []int64{0, 10} {
			repo := &rpmTargetRepoStub{rpmOverrideRepoStub: rpmOverrideRepoStub{override: &target}}
			cache := &userRPMCacheStub{userGroupCounts: []int{2}}
			billing := newBillingServiceForRPM(t, cache, repo)
			// A legacy snapshot or a snapshot for another group must load the target policy.
			user := &User{ID: 1, UserGroupRPMOverride: value, UserGroupRPMOverrideGroupID: loadedGroup}
			before := *user
			require.ErrorIs(t, billing.checkRPM(context.Background(), user, &Group{ID: 20}), ErrGroupRPMExceeded)
			require.EqualValues(t, 1, repo.calls)
			require.EqualValues(t, 20, repo.groupID)
			require.Equal(t, before, *user)
		}
	}
	var old APIKeyAuthUserSnapshot
	require.NoError(t, json.Unmarshal([]byte(`{"user_group_rpm_override":3}`), &old))
	require.Zero(t, old.UserGroupRPMOverrideGroupID)
}
