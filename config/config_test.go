package config

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const testTemplate = "postgres://{db_user}:{db_password}@localhost:5432/job_manager?sslmode=disable"

func cleanEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"USE_POSTGRES", "HTTP_ADDR", "DB_URL", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_LIFETIME", "PGSERVICE", "PGSERVICEFILE", "PGSSLMODE", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY", "PGCONNECT_TIMEOUT", "PGPORT"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DB_URL", testTemplate)
}

func testSecrets(context.Context) (map[string]any, error) {
	return map[string]any{"db_user": "worker", "db_password": "password"}, nil
}

func TestLoadAppDefaultsAndOverrides(t *testing.T) {
	for _, override := range []bool{false, true} {
		name := "defaults"
		if override {
			name = "overrides"
		}
		t.Run(name, func(t *testing.T) {
			cleanEnvironment(t)
			wantPool := PoolConfig{MaxConns: 10, MinConns: 0, MaxConnLifetime: time.Hour}
			wantAddr := ":8080"
			if override {
				t.Setenv("USE_POSTGRES", "true")
				t.Setenv("HTTP_ADDR", "[::1]:9090")
				t.Setenv("DB_MAX_CONNS", "20")
				t.Setenv("DB_MIN_CONNS", "2")
				t.Setenv("DB_MAX_CONN_LIFETIME", "30m")
				wantPool = PoolConfig{MaxConns: 20, MinConns: 2, MaxConnLifetime: 30 * time.Minute}
				wantAddr = "[::1]:9090"
			}
			cfg, err := loadApp(context.Background(), testSecrets)
			if err != nil {
				t.Fatal(err)
			}
			if !cfg.UsePostgres || cfg.Pool != wantPool || cfg.HTTPAddr != wantAddr {
				t.Fatalf("unexpected pool or HTTP settings: %v, %s", cfg.Pool, cfg.HTTPAddr)
			}
			db, err := loadDatabase(context.Background(), testSecrets)
			if err != nil || cfg.Database != db {
				t.Fatal("application and migration database configurations differ")
			}
		})
	}
}

func TestLoadAppMemoryDoesNotReadDatabaseSettingsOrVault(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("USE_POSTGRES", "false")
	t.Setenv("HTTP_ADDR", ":9090")
	for _, name := range []string{"DB_URL", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_LIFETIME"} {
		t.Setenv(name, "invalid")
	}
	cfg, err := loadApp(context.Background(), func(context.Context) (map[string]any, error) {
		t.Fatal("memory mode must not read Vault")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UsePostgres || cfg.HTTPAddr != ":9090" || cfg.Database != (DatabaseConfig{}) || cfg.Pool != (PoolConfig{}) {
		t.Fatal("unexpected configuration for memory mode")
	}
}

func TestLoadAppMemoryStillValidatesHTTPAddress(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("USE_POSTGRES", "false")
	t.Setenv("HTTP_ADDR", "invalid")
	_, err := loadApp(context.Background(), testSecrets)
	if err == nil || !strings.Contains(err.Error(), "HTTP_ADDR") {
		t.Fatalf("error = %v, want HTTP_ADDR validation error", err)
	}
}

func TestLoadAppRejectsInvalidSettingsBeforeVault(t *testing.T) {
	cases := map[string][]string{
		"USE_POSTGRES":         {"", "invalid"},
		"HTTP_ADDR":            {"", "localhost", "http://localhost:8080", "localhost:abc", ":65536", ":-1", "bad host:80"},
		"DB_MAX_CONNS":         {"", "0", "-1", "2147483648", "nope"},
		"DB_MIN_CONNS":         {"", "-1", "11", "2147483648", "nope"},
		"DB_MAX_CONN_LIFETIME": {"", "3600", "0s", "-1h", "99999999999999999h"},
	}
	for key, values := range cases {
		for _, value := range values {
			t.Run(key+"/"+value, func(t *testing.T) {
				cleanEnvironment(t)
				t.Setenv(key, value)
				_, err := loadApp(context.Background(), func(context.Context) (map[string]any, error) {
					t.Fatal("invalid settings should fail before reading Vault")
					return nil, nil
				})
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("error = %v, want %s validation error", err, key)
				}
			})
		}
	}
}

func TestLoadDatabaseEscapesCredentialsAndIgnoresAppSettings(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("USE_POSTGRES", "false")
	for _, name := range []string{"HTTP_ADDR", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_LIFETIME"} {
		t.Setenv(name, "invalid")
	}
	user := "user@:/%?# +{db_password}ю"
	password := "pass@:/%?# +{db_user}ю"
	cfg, err := loadDatabase(context.Background(), func(context.Context) (map[string]any, error) {
		return map[string]any{"db_user": user, "db_password": password}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := pgx.ParseConfig(cfg.URL)
	if err != nil {
		t.Fatal("assembled URL is not accepted by pgx")
	}
	if parsed.User != user || parsed.Password != password || parsed.Host != "localhost" || parsed.Port != 5432 || parsed.Database != "job_manager" || parsed.TLSConfig != nil {
		t.Fatal("credentials or connection options changed during substitution")
	}
}

func TestLoadDatabaseRejectsInvalidTemplates(t *testing.T) {
	for _, template := range []string{
		"", "postgres://user:password@localhost/db", "mysql://{db_user}:{db_password}@localhost/db",
		"postgres://{db_password}:{db_user}@localhost/db", "postgres://{db_user}:{db_password}@/db",
		"postgres://{db_user}:{db_password}@localhost", "postgres://{db_user}:{db_password}@localhost:65536/db",
		"postgres://{db_user}:{db_password}@localhost:abc/db", "postgres://{db_user}:{db_password}@localhost/db#fragment",
		testTemplate + "&unknown={other}", testTemplate + "&password=secret-marker",
		testTemplate + "&user=secret-marker", testTemplate + "&again={db_user}",
		"postgres://{db_user}:{db_password}@localhost/db?sslmode=invalid", testTemplate + "&bad=%zz",
		"postgres://template_user:template_password@localhost/{db_user}/{db_password}",
	} {
		t.Run(template, func(t *testing.T) {
			cleanEnvironment(t)
			t.Setenv("DB_URL", template)
			_, err := loadDatabase(context.Background(), func(context.Context) (map[string]any, error) {
				t.Fatal("invalid template should fail before reading Vault")
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), "DB_URL") || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("expected a safe DB_URL validation error, got %v", err)
			}
		})
	}
}

func TestLoadDatabaseRejectsInvalidSecrets(t *testing.T) {
	for _, key := range []string{"db_user", "db_password"} {
		for _, value := range []any{nil, "", 123, true, []string{"secret-marker"}} {
			t.Run(key, func(t *testing.T) {
				cleanEnvironment(t)
				_, err := loadDatabase(context.Background(), func(ctx context.Context) (map[string]any, error) {
					secrets, _ := testSecrets(ctx)
					if value == nil {
						delete(secrets, key)
					} else {
						secrets[key] = value
					}
					return secrets, nil
				})
				if err == nil || !strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "secret-marker") {
					t.Fatalf("expected safe %s error, got %v", key, err)
				}
			})
		}
	}
}

func TestLoadDatabaseProviderErrorAndCancellation(t *testing.T) {
	cleanEnvironment(t)
	reader := func(context.Context) (map[string]any, error) {
		return nil, errors.New("secret-marker")
	}
	_, err := loadDatabase(context.Background(), reader)
	if err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("provider error must not expose response contents")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = loadDatabase(ctx, reader)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
}

func TestDatabaseTemplatePreservesOptions(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("DB_URL", "postgresql://{db_user}:{db_password}@[::1]:5433/jobs%20db?sslmode=disable&application_name=job%20manager")
	cfg, err := loadDatabase(context.Background(), testSecrets)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host != "[::1]:5433" || u.Path != "/jobs db" || u.RawQuery != "sslmode=disable&application_name=job%20manager" {
		t.Fatal("database URL components were not preserved")
	}
}
