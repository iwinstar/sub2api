package config

import "testing"

func TestBillingDedupRetentionValidation(t *testing.T) {
	resetViperWithJWTSecret(t)
	base, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if base.DashboardAgg.Retention.UsageBillingDedupOrdinaryAutoDelete || base.DashboardAgg.Retention.UsageBillingDedupVideoDeleteDays != 0 || base.DashboardAgg.Retention.UsageBillingDedupBatchImageDeleteDays != 0 {
		t.Fatal("deletion must default to disabled")
	}
	for _, tc := range []struct {
		name                    string
		days, imageDays, sticky int
		invalid                 bool
	}{
		{"disabled", 0, 0, 3600, false},
		{"negative", -1, 0, 3600, true},
		{"negative_image", 0, -1, 3600, true},
		{"too_short", 1, 0, 3600, true},
		{"minimum", 2, 1, 3600, false},
		{"sticky_boundary", 2, 0, 86400, false},
		{"sticky_extra_second", 2, 0, 86401, true},
		{"long_sticky", 4, 7, 3 * 86400, false},
		{"long_sticky_short_retention", 3, 7, 3 * 86400, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := *base
			cfg.DashboardAgg.Enabled = false // validation must not depend on aggregation
			cfg.DashboardAgg.Retention.UsageLogsDays = 0
			cfg.DashboardAgg.Retention.UsageBillingDedupOrdinaryAutoDelete = true
			cfg.DashboardAgg.Retention.UsageBillingDedupVideoDeleteDays = tc.days
			cfg.DashboardAgg.Retention.UsageBillingDedupBatchImageDeleteDays = tc.imageDays
			cfg.Gateway.OpenAIWS.StickySessionTTLSeconds = tc.sticky
			if err := cfg.Validate(); (err != nil) != tc.invalid {
				t.Fatalf("Validate() = %v, want invalid=%v", err, tc.invalid)
			}
		})
	}
}
