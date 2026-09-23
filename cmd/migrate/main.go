package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/sponez/job-manager/config"
	"github.com/sponez/job-manager/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("run migrations: migrate <status|version|up|up-to|down|down-to>")
	}
	if err := config.LoadEnv(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDatabase(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return fmt.Errorf("open database: invalid connection configuration")
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping database: %w", err)
	}

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("lock database: %w", err)
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		migrations.Files,
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("create provider: %w", err)
	}
	defer provider.Close()

	switch os.Args[1] {
	case "status":
		return statusExec(ctx, provider)
	case "version":
		return versionExec(ctx, provider)
	case "up":
		return upExec(ctx, provider)
	case "down":
		return downExec(ctx, provider)
	case "up-to":
		return upToExec(ctx, provider)
	case "down-to":
		return downToExec(ctx, provider)
	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}

func statusExec(ctx context.Context, provider *goose.Provider) error {
	statuses, err := provider.Status(ctx)
	if err != nil {
		return fmt.Errorf("get statuses: %w", err)
	}

	for _, status := range statuses {
		if status.Source == nil {
			fmt.Printf("%-8s <unknown>\n", status.State)
			continue
		}

		fmt.Printf(
			"%-8s %d %s\n",
			status.State,
			status.Source.Version,
			status.Source.Path,
		)
	}

	return nil
}

func versionExec(ctx context.Context, provider *goose.Provider) error {
	version, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}

	fmt.Println(version)

	return nil
}

func upExec(ctx context.Context, provider *goose.Provider) error {
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("up: %w", err)
	}

	for _, result := range results {
		fmt.Println(result)
	}

	return nil
}

func downExec(ctx context.Context, provider *goose.Provider) error {
	result, err := provider.Down(ctx)
	if err != nil {
		return fmt.Errorf("down: %w", err)
	}

	fmt.Println(result)

	return nil
}

func upToExec(ctx context.Context, provider *goose.Provider) error {
	version, err := parseVersion(os.Args)
	if err != nil {
		return err
	}

	results, err := provider.UpTo(ctx, version)
	if err != nil {
		return fmt.Errorf("up to %d: %w", version, err)
	}

	for _, result := range results {
		fmt.Println(result)
	}

	return nil
}

func downToExec(ctx context.Context, provider *goose.Provider) error {
	version, err := parseVersion(os.Args)
	if err != nil {
		return err
	}

	results, err := provider.DownTo(ctx, version)
	if err != nil {
		return fmt.Errorf("down to %d: %w", version, err)
	}

	for _, result := range results {
		fmt.Println(result)
	}

	return nil
}

func parseVersion(args []string) (int64, error) {
	if len(args) < 3 {
		return 0, fmt.Errorf("migration version is required")
	}

	version, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse version: %w", err)
	}

	return version, nil
}
