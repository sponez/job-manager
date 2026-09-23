package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type AppConfig struct {
	UsePostgres bool
	Database    DatabaseConfig
	Pool        PoolConfig
	HTTPAddr    string
}

type DatabaseConfig struct {
	URL string
}

type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
}

type secretReader func(context.Context) (map[string]any, error)

func LoadApp(ctx context.Context) (AppConfig, error) {
	return loadApp(ctx, readVaultSecrets)
}

func LoadDatabase(ctx context.Context) (DatabaseConfig, error) {
	return loadDatabase(ctx, readVaultSecrets)
}

func loadApp(ctx context.Context, readSecrets secretReader) (AppConfig, error) {
	var cfg AppConfig
	cfg.HTTPAddr = envOrDefault("HTTP_ADDR", ":8080")
	host, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil || strings.ContainsAny(host, " \t\r\n/?#@") || !validPort(port, true) {
		return AppConfig{}, errors.New("HTTP_ADDR must be a host:port address with a port between 0 and 65535")
	}
	cfg.UsePostgres, err = strconv.ParseBool(envOrDefault("USE_POSTGRES", "true"))
	if err != nil {
		return AppConfig{}, errors.New("USE_POSTGRES must be a boolean, such as true or false")
	}
	if !cfg.UsePostgres {
		return cfg, nil
	}

	cfg.Pool.MaxConns, err = envInt32("DB_MAX_CONNS", "10")
	if err != nil {
		return AppConfig{}, err
	}
	if cfg.Pool.MaxConns <= 0 {
		return AppConfig{}, errors.New("DB_MAX_CONNS must be positive")
	}
	cfg.Pool.MinConns, err = envInt32("DB_MIN_CONNS", "0")
	if err != nil {
		return AppConfig{}, err
	}
	if cfg.Pool.MinConns < 0 || cfg.Pool.MinConns > cfg.Pool.MaxConns {
		return AppConfig{}, errors.New("DB_MIN_CONNS must be between 0 and DB_MAX_CONNS")
	}
	cfg.Pool.MaxConnLifetime, err = time.ParseDuration(envOrDefault("DB_MAX_CONN_LIFETIME", "1h"))
	if err != nil || cfg.Pool.MaxConnLifetime <= 0 {
		return AppConfig{}, errors.New("DB_MAX_CONN_LIFETIME must be a positive duration, such as 30m or 1h")
	}
	cfg.Database, err = loadDatabase(ctx, readSecrets)
	if err != nil {
		return AppConfig{}, err
	}
	return cfg, nil
}

func loadDatabase(ctx context.Context, readSecrets secretReader) (DatabaseConfig, error) {
	u, err := parseDatabaseTemplate(os.Getenv("DB_URL"))
	if err != nil {
		return DatabaseConfig{}, err
	}
	secrets, err := readSecrets(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return DatabaseConfig{}, fmt.Errorf("read Vault database secrets: %w", ctx.Err())
		}
		// Provider errors may contain response bodies or credentials.
		return DatabaseConfig{}, errors.New("cannot read database secrets from Vault app/job-manager")
	}
	user, err := secretString(secrets, "db_user")
	if err != nil {
		return DatabaseConfig{}, err
	}
	password, err := secretString(secrets, "db_password")
	if err != nil {
		return DatabaseConfig{}, err
	}
	u.User = url.UserPassword(user, password)
	return DatabaseConfig{URL: u.String()}, nil
}

func parseDatabaseTemplate(template string) (*url.URL, error) {
	invalid := errors.New("DB_URL must be a PostgreSQL URL with {db_user}:{db_password} credentials, a host and a database name")
	_, authority, hasScheme := strings.Cut(template, "://")
	if !hasScheme || !strings.HasPrefix(authority, "{db_user}:{db_password}@") {
		return nil, invalid
	}
	if strings.Count(template, "{db_user}") != 1 || strings.Count(template, "{db_password}") != 1 {
		return nil, invalid
	}
	// Parse harmless userinfo, then assign the actual credentials with net/url.
	value := strings.NewReplacer("{db_user}", "template_user", "{db_password}", "template_password").Replace(template)
	if strings.ContainsAny(value, "{}") {
		return nil, invalid
	}
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.Fragment != "" || strings.Trim(u.Path, "/") == "" || u.User == nil {
		return nil, invalid
	}
	password, ok := u.User.Password()
	if u.User.Username() != "template_user" || !ok || password != "template_password" || (u.Port() != "" && !validPort(u.Port(), false)) {
		return nil, invalid
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || query.Has("user") || query.Has("password") {
		return nil, invalid
	}
	// Validate driver options without ever putting secrets in a parse error.
	if _, err := pgx.ParseConfig(u.String()); err != nil {
		return nil, errors.New("DB_URL contains invalid PostgreSQL connection options")
	}
	return u, nil
}

func secretString(secrets map[string]any, name string) (string, error) {
	value, ok := secrets[name].(string)
	if !ok || value == "" {
		return "", fmt.Errorf("Vault secret %s must be a non-empty string", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func envInt32(name, fallback string) (int32, error) {
	value, err := strconv.ParseInt(envOrDefault(name, fallback), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be a 32-bit integer", name)
	}
	return int32(value), nil
}

func validPort(value string, allowZero bool) bool {
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && (allowZero || port > 0)
}
