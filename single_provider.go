package smpostgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SingleProvider is an isolated entity for single-instance postgres mode.
type SingleProvider struct {
	name      string
	pool      *pgxpool.Pool
	closeOnce sync.Once
}

func NewSingle(ctx context.Context, node NodeConfig) (*SingleProvider, error) {
	return NewNamedSingle(ctx, "", node)
}

func NewNamedSingle(ctx context.Context, name string, node NodeConfig) (*SingleProvider, error) {
	if err := validateNode("single", node); err != nil {
		return nil, err
	}

	pool, err := newPool(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("create single pool: %w", err)
	}

	return &SingleProvider{name: name, pool: pool}, nil
}

func (p *SingleProvider) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.pool.Exec(ctx, sql, arguments...)
}

func (p *SingleProvider) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

func (p *SingleProvider) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

func (p *SingleProvider) QueryMaster(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

func (p *SingleProvider) QueryRowMaster(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

func (p *SingleProvider) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.pool.Begin(ctx)
}

func (p *SingleProvider) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *SingleProvider) Name() string {
	return p.name
}

func (p *SingleProvider) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReadinessTimeout)
	defer cancel()
	return p.Ping(ctx) == nil
}

func (p *SingleProvider) Close() {
	p.closeOnce.Do(func() {
		if p.pool != nil {
			p.pool.Close()
		}
	})
}
