package main

import (
	"strings"
	"testing"

	"github.com/sponez/job-manager/config"
)

func TestCreateJobRepositoryPostgresFailure(t *testing.T) {
	repository, cleanup, err := createJobRepository(t.Context(), config.AppConfig{
		UsePostgres: true,
		Database:    config.DatabaseConfig{URL: "postgres://%"},
	})
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
	if err == nil || !strings.Contains(err.Error(), "initialize postgres") {
		t.Fatalf("error = %v, want PostgreSQL initialization error", err)
	}
	if repository != nil {
		t.Fatal("PostgreSQL failure must not fall back to an in-memory repository")
	}
}
