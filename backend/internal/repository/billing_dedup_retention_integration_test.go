//go:build integration

package repository

import "testing"

func TestBillingDedupRetentionIntegration(t *testing.T) {
	testBillingDedupRetentionSQL(t, integrationDB)
}
