package postgres

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/sponez/job-manager/migrations"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Owned by TestMain and shared by all repository tests in this package.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	flag.Parse() // TestMain runs before testing parses flags such as -short.
	// runRepositoryTests returns after its deferred cleanup, before os.Exit.
	os.Exit(runRepositoryTests(m))
}

func runRepositoryTests(m *testing.M) (exitCode int) {
	if testing.Short() {
		return m.Run()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := pgcontainer.Run(ctx, "postgres:18.4",
		pgcontainer.WithDatabase("job_manager_test"),
		pgcontainer.WithUsername("test_user"),
		pgcontainer.WithPassword("test_password"),
		pgcontainer.BasicWaitStrategies(),
	)
	// Also clean up when container startup only partially succeeded.
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "remove test PostgreSQL container: %v\n", err)
			exitCode = 1
		}
	}()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start test PostgreSQL (Docker must be running): %v\n", err)
		return 1
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get test PostgreSQL connection string: %v\n", err)
		return 1
	}
	if err := applyTestMigrations(ctx, dsn); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create test PostgreSQL pool: %v\n", err)
		return 1
	}
	// Deferred calls run in reverse order: close the pool, then the container.
	defer testPool.Close()
	if err := testPool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping test PostgreSQL: %v\n", err)
		return 1
	}
	cancel() // The startup deadline does not limit the lifetime of the test suite.
	return m.Run()
}

// repositoryTestPool borrows the shared pool; individual tests must not close it.
func repositoryTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("PostgreSQL integration test requires Docker")
	}
	return testPool
}

func applyTestMigrations(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.Files)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("apply test migrations: %w", err)
	}
	if testing.Verbose() {
		fmt.Printf("applied %d migrations to shared test PostgreSQL\n", len(results))
	}
	return nil
}
