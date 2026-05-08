// Package store is the data-access layer. Wraps pgxpool and exposes
// the sqlc-generated typed query layer.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/759257989/processing-platform/internal/store/db"
)

// Store bundles the connection pool and a Queries handle.
// Pass *Store around; don't reach into pool or queries directly elsewhere.
type Store struct {
	Pool    *pgxpool.Pool
	Queries *db.Queries
}

// New connects to Postgres using `dsn` (a libpq-style connection string)
// and returns a ready-to-use Store.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}
	// Sane pool defaults. Tune with metrics in Stage 4.
	cfg.MaxConns = 25
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pg: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}

	return &Store{
		Pool:    pool,
		Queries: db.New(pool),
	}, nil
}

// Close releases the underlying pool. Safe to call multiple times.
func (s *Store) Close() {
	s.Pool.Close()
}
