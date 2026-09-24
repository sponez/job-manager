package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sponez/job-manager/config"
)

func createDB(ctx context.Context, database config.DatabaseConfig, pool config.PoolConfig) (*pgxpool.Pool, error) {
	dbCfg, err := pgxpool.ParseConfig(database.URL)
	if err != nil {
		// Parse errors can contain the connection string, including credentials.
		return nil, errors.New("invalid database connection string")
	}
	dbCfg.MaxConns = pool.MaxConns
	dbCfg.MinConns = pool.MinConns
	dbCfg.MaxConnLifetime = pool.MaxConnLifetime

	dbPool, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := dbPool.Ping(ctx); err != nil {
		dbPool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return dbPool, nil
}
