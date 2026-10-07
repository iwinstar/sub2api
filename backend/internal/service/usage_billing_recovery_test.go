//go:build unit

package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestGatewayServiceRecordUsage_BillingUnknownRequiresReconciliation(t *testing.T) {
	usageRepo := &openAIRecordUsageLogRepoStub{}
	billingErr := ErrUsageBillingOutcomeUnknown
	billingRepo := &openAIRecordUsageBillingRepoStub{err: billingErr}
	userRepo := &openAIRecordUsageUserRepoStub{}
	subRepo := &openAIRecordUsageSubRepoStub{}
	svc := newGatewayRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, userRepo, subRepo)

	err := svc.RecordUsage(context.Background(), &RecordUsageInput{
		Result: &ForwardResult{
			RequestID: "gateway_billing_fail",
			Usage: ClaudeUsage{
				InputTokens:  10,
				OutputTokens: 6,
			},
			Model:    "claude-sonnet-4",
			Duration: time.Second,
		},
		APIKey:  &APIKey{ID: 505},
		User:    &User{ID: 605},
		Account: &Account{ID: 705},
	})

	require.ErrorIs(t, err, billingErr)
	require.Equal(t, 1, billingRepo.calls)
	require.Zero(t, usageRepo.calls)
	require.Nil(t, usageRepo.lastLog)
}

func TestGatewayServiceRecordUsage_BillingConfirmedWritesRealCost(t *testing.T) {
	usageRepo := &openAIRecordUsageLogRepoStub{}
	billingRepo := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{ConfirmedExisting: true}}
	userRepo := &openAIRecordUsageUserRepoStub{}
	subRepo := &openAIRecordUsageSubRepoStub{}
	svc := newGatewayRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, userRepo, subRepo)

	err := svc.RecordUsage(context.Background(), &RecordUsageInput{
		Result: &ForwardResult{
			RequestID: "gateway_billing_fail",
			Usage: ClaudeUsage{
				InputTokens:  10,
				OutputTokens: 6,
			},
			Model:    "claude-sonnet-4",
			Duration: time.Second,
		},
		APIKey:  &APIKey{ID: 505},
		User:    &User{ID: 605},
		Account: &Account{ID: 705},
	})

	require.NoError(t, err)
	require.Equal(t, 1, billingRepo.calls)
	require.Equal(t, 1, usageRepo.calls)
	require.NotNil(t, usageRepo.lastLog)
	require.Equal(t, 10, usageRepo.lastLog.InputTokens)
	require.Equal(t, 6, usageRepo.lastLog.OutputTokens)
	require.Greater(t, usageRepo.lastLog.InputCost, 0.0)
	require.Greater(t, usageRepo.lastLog.OutputCost, 0.0)
	require.Greater(t, usageRepo.lastLog.TotalCost, 0.0)
	require.Greater(t, usageRepo.lastLog.ActualCost, 0.0)
}

func TestOpenAIGatewayServiceRecordUsage_BillingUnknownRequiresReconciliation(t *testing.T) {
	usageRepo := &openAIRecordUsageLogRepoStub{}
	billingErr := ErrUsageBillingOutcomeUnknown
	billingRepo := &openAIRecordUsageBillingRepoStub{err: billingErr}
	userRepo := &openAIRecordUsageUserRepoStub{}
	subRepo := &openAIRecordUsageSubRepoStub{}
	svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, userRepo, subRepo, nil)

	err := svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
		Result: &OpenAIForwardResult{
			RequestID: "resp_billing_fail",
			Usage: OpenAIUsage{
				InputTokens:  8,
				OutputTokens: 4,
			},
			Model:    "gpt-5.1",
			Duration: time.Second,
		},
		APIKey:  &APIKey{ID: 10048},
		User:    &User{ID: 20048},
		Account: &Account{ID: 30048},
	})

	require.ErrorIs(t, err, billingErr)
	require.Equal(t, 1, billingRepo.calls)
	require.Zero(t, usageRepo.calls)
	require.Nil(t, usageRepo.lastLog)
}

func TestOpenAIGatewayServiceRecordUsage_BillingConfirmedWritesRealCost(t *testing.T) {
	usageRepo := &openAIRecordUsageLogRepoStub{}
	billingRepo := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{ConfirmedExisting: true}}
	userRepo := &openAIRecordUsageUserRepoStub{}
	subRepo := &openAIRecordUsageSubRepoStub{}
	svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, userRepo, subRepo, nil)

	err := svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
		Result: &OpenAIForwardResult{
			RequestID: "resp_billing_fail",
			Usage: OpenAIUsage{
				InputTokens:  8,
				OutputTokens: 4,
			},
			Model:    "gpt-5.1",
			Duration: time.Second,
		},
		APIKey:  &APIKey{ID: 10048},
		User:    &User{ID: 20048},
		Account: &Account{ID: 30048},
	})

	require.NoError(t, err)
	require.Equal(t, 1, billingRepo.calls)
	require.Equal(t, 1, usageRepo.calls)
	require.NotNil(t, usageRepo.lastLog)
	require.Equal(t, 8, usageRepo.lastLog.InputTokens)
	require.Equal(t, 4, usageRepo.lastLog.OutputTokens)
	require.Greater(t, usageRepo.lastLog.InputCost, 0.0)
	require.Greater(t, usageRepo.lastLog.OutputCost, 0.0)
	require.Greater(t, usageRepo.lastLog.TotalCost, 0.0)
	require.Greater(t, usageRepo.lastLog.ActualCost, 0.0)
}

type recoveryInvalidationCache struct {
	BillingCache
	invalidated []string
}

func (c *recoveryInvalidationCache) InvalidateUserBalance(context.Context, int64) error {
	c.invalidated = append(c.invalidated, "balance")
	return nil
}
func (c *recoveryInvalidationCache) InvalidateSubscriptionCache(context.Context, int64, int64) error {
	c.invalidated = append(c.invalidated, "subscription")
	return nil
}
func (c *recoveryInvalidationCache) InvalidateAPIKeyRateLimit(context.Context, int64) error {
	c.invalidated = append(c.invalidated, "windows")
	return nil
}
func TestBillingConfirmedInvalidatesWithoutIncrements(t *testing.T) {
	cache := &recoveryInvalidationCache{}
	writes := make(chan cacheWriteTask, 4)
	groupID := int64(5)
	p := &postUsageBillingParams{Cost: &CostBreakdown{ActualCost: 3}, User: &User{ID: 7}, APIKey: &APIKey{ID: 13, GroupID: &groupID, RateLimit5h: 10}, Account: &Account{ID: 9}}
	repo := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{ConfirmedExisting: true}}
	applied, err := applyUsageBilling(context.Background(), "recovery", nil, p, &billingDeps{billingCacheService: &BillingCacheService{cache: cache, cacheWriteChan: writes}, deferredService: &DeferredService{}}, repo)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, []string{"balance", "subscription", "windows"}, cache.invalidated)
	require.Empty(t, writes)
}

func TestBillingUsageAuditPreservesCostsWithoutCredentials(t *testing.T) {
	usage := &UsageLog{RequestID: "audit-request", InputTokens: 123, ActualCost: 1.25, APIKey: &APIKey{Key: "secret-api-key"}, User: &User{PasswordHash: "secret-password"}}
	audit := billingUsageAudit(usage)
	require.Contains(t, audit, `"InputTokens":123`)
	require.Contains(t, audit, `"ActualCost":1.25`)
	require.Contains(t, audit, `"RequestID":"audit-request"`)
	require.NotContains(t, audit, "secret-api-key")
	require.NotContains(t, audit, "secret-password")
	require.NotNil(t, usage.APIKey)
	require.NotNil(t, usage.User)
}
