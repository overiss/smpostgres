package smpostgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SingleProvider is an isolated entity for single-instance postgres mode.
type SingleProvider struct {
	name      string
	pool      *pgxpool.Pool
	closeOnce sync.Once
	ready     atomic.Bool
	stopCh    chan struct{}
	monitorWG sync.WaitGroup
}

// NewSingle creates single-instance provider without logical client name.
func NewSingle(ctx context.Context, node NodeConfig) (*SingleProvider, error) {
	return NewNamedSingle(ctx, "", node)
}

// NewNamedSingle creates single-instance provider with logical client name.
func NewNamedSingle(ctx context.Context, name string, node NodeConfig) (*SingleProvider, error) {
	if err := validateNode("single", node); err != nil {
		return nil, err
	}

	pool, err := newPool(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("create single pool: %w", err)
	}

	provider := &SingleProvider{
		name:   name,
		pool:   pool,
		stopCh: make(chan struct{}),
	}
	provider.startReadinessMonitor()
	return provider, nil
}

// Exec executes statement on single database pool.
func (p *SingleProvider) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.pool.Exec(ctx, sql, arguments...)
}

// Query executes read query on single database pool.
func (p *SingleProvider) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

// QueryRow executes single-row read query on single database pool.
func (p *SingleProvider) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

// QueryMaster is an alias to Query for single mode.
func (p *SingleProvider) QueryMaster(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

// QueryRowMaster is an alias to QueryRow for single mode.
func (p *SingleProvider) QueryRowMaster(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

// Begin starts transaction on single database pool.
func (p *SingleProvider) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.pool.Begin(ctx)
}

// Ping checks connectivity for single database pool.
func (p *SingleProvider) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Name returns logical postgres client name.
func (p *SingleProvider) Name() string {
	return p.name
}

// IsReady returns cached readiness updated by background monitor.
func (p *SingleProvider) IsReady() bool {
	return p.ready.Load()
}

// Close closes single pool and is safe for repeated calls.
func (p *SingleProvider) Close() {
	p.closeOnce.Do(func() {
		if p.stopCh != nil {
			close(p.stopCh)
		}
		p.monitorWG.Wait()
		if p.pool != nil {
			p.pool.Close()
		}
	})
}

// startReadinessMonitor launches lightweight background readiness checks.
func (p *SingleProvider) startReadinessMonitor() {
	p.monitorWG.Add(1)
	go func() {
		defer p.monitorWG.Done()
		p.updateReady()

		ticker := time.NewTicker(DefaultReadinessCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				p.updateReady()
			}
		}
	}()
}

// updateReady performs one ping attempt and stores readiness state.
func (p *SingleProvider) updateReady() {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReadinessTimeout)
	defer cancel()
	p.ready.Store(p.Ping(ctx) == nil)
}
