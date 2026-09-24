package config

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoadDatabaseFromVault(t *testing.T) {
	cleanEnvironment(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/app/data/job-manager" || r.Header.Get("X-Vault-Token") != "test-token" {
			t.Error("unexpected Vault request")
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"db_user":"worker","db_password":"p@ss"},"metadata":{"version":1}}}`))
	}))
	defer server.Close()
	t.Setenv("VAULT_ADDR", server.URL)
	t.Setenv("VAULT_TOKEN", "test-token")
	t.Setenv("VAULT_AGENT_ADDR", "")
	cfg, err := LoadDatabase(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !strings.HasPrefix(cfg.URL, "postgres://worker:p%40ss@localhost:5432/job_manager") {
		t.Fatal("expected one KV v2 read and a URL containing escaped Vault credentials")
	}
}
