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

// MasterReplicaConfig configures cluster provider with master/sync/async nodes.
type MasterReplicaConfig struct {
	// Name is logical postgres client name used in diagnostics/readiness.
	Name string
	// ReadPreference controls routing for Query/QueryRow operations.
	ReadPreference ReadPreference
	// Master is writer node configuration.
	Master NodeConfig
	// SyncReplica is synchronous replica available for read traffic.
	SyncReplica *NodeConfig
	// AsyncReplica is asynchronous replica available for read traffic.
	AsyncReplica *NodeConfig
}

// MasterReplicaProvider is an isolated entity for cluster mode:
// master + sync replicas + async replicas in one client.
type MasterReplicaProvider struct {
	name           string
	readPreference ReadPreference

	master       *pgxpool.Pool
	syncReplica  *pgxpool.Pool
	asyncReplica *pgxpool.Pool

	closeOnce sync.Once
	rrAny     atomic.Uint64
	ready     atomic.Bool
	stopCh    chan struct{}
	monitorWG sync.WaitGroup
}

// NewMasterReplica creates cluster provider with master and sync/async replicas.
func NewMasterReplica(ctx context.Context, cfg MasterReplicaConfig) (*MasterReplicaProvider, error) {
	if err := validateNode("master", cfg.Master); err != nil {
		return nil, err
	}
	if cfg.SyncReplica != nil {
		if err := validateNode("sync replica", *cfg.SyncReplica); err != nil {
			return nil, err
		}
	}
	if cfg.AsyncReplica != nil {
		if err := validateNode("async replica", *cfg.AsyncReplica); err != nil {
			return nil, err
		}
	}

	readPreference := cfg.ReadPreference
	if readPreference == "" {
		readPreference = ReadPreferReplica
	}
	if readPreference == ReadReplicaOnly && cfg.SyncReplica == nil && cfg.AsyncReplica == nil {
		return nil, ErrNoReadPool
	}

	master, err := newPool(ctx, cfg.Master)
	if err != nil {
		return nil, fmt.Errorf("create master pool: %w", err)
	}

	p := &MasterReplicaProvider{
		name:           cfg.Name,
		readPreference: readPreference,
		master:         master,
		stopCh:         make(chan struct{}),
	}

	if cfg.SyncReplica != nil {
		replicaPool, replicaErr := newPool(ctx, *cfg.SyncReplica)
		if replicaErr != nil {
			p.Close()
			return nil, fmt.Errorf("create sync replica pool: %w", replicaErr)
		}
		p.syncReplica = replicaPool
	}
	if cfg.AsyncReplica != nil {
		replicaPool, replicaErr := newPool(ctx, *cfg.AsyncReplica)
		if replicaErr != nil {
			p.Close()
			return nil, fmt.Errorf("create async replica pool: %w", replicaErr)
		}
		p.asyncReplica = replicaPool
	}

	p.startReadinessMonitor()
	return p, nil
}

// Exec always executes write statement on master.
func (p *MasterReplicaProvider) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.master.Exec(ctx, sql, arguments...)
}

// Query executes read query with selected read preference.
func (p *MasterReplicaProvider) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	readPool := p.pickReadPool()
	if readPool == nil {
		return nil, ErrNoReadPool
	}
	return readPool.Query(ctx, sql, args...)
}

// QueryRow executes single-row read query with selected read preference.
func (p *MasterReplicaProvider) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	readPool := p.pickReadPool()
	if readPool == nil {
		return errorRow{err: ErrNoReadPool}
	}
	return readPool.QueryRow(ctx, sql, args...)
}

// QueryMaster forces read query on master pool.
func (p *MasterReplicaProvider) QueryMaster(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.master.Query(ctx, sql, args...)
}

// QueryRowMaster forces single-row read query on master pool.
func (p *MasterReplicaProvider) QueryRowMaster(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.master.QueryRow(ctx, sql, args...)
}

// QuerySyncReplica executes read query only on sync replicas.
func (p *MasterReplicaProvider) QuerySyncReplica(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	readPool := p.pickSyncReplicaPool()
	if readPool == nil {
		return nil, ErrNoSyncReplica
	}
	return readPool.Query(ctx, sql, args...)
}

// QueryRowSyncReplica executes single-row read query only on sync replicas.
func (p *MasterReplicaProvider) QueryRowSyncReplica(ctx context.Context, sql string, args ...any) pgx.Row {
	readPool := p.pickSyncReplicaPool()
	if readPool == nil {
		return errorRow{err: ErrNoSyncReplica}
	}
	return readPool.QueryRow(ctx, sql, args...)
}

// QueryAsyncReplica executes read query only on async replicas.
func (p *MasterReplicaProvider) QueryAsyncReplica(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	readPool := p.pickAsyncReplicaPool()
	if readPool == nil {
		return nil, ErrNoAsyncReplica
	}
	return readPool.Query(ctx, sql, args...)
}

// QueryRowAsyncReplica executes single-row read query only on async replicas.
func (p *MasterReplicaProvider) QueryRowAsyncReplica(ctx context.Context, sql string, args ...any) pgx.Row {
	readPool := p.pickAsyncReplicaPool()
	if readPool == nil {
		return errorRow{err: ErrNoAsyncReplica}
	}
	return readPool.QueryRow(ctx, sql, args...)
}

// Begin starts transaction on master pool.
func (p *MasterReplicaProvider) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.master.Begin(ctx)
}

// Ping checks connectivity for master and all configured replicas.
func (p *MasterReplicaProvider) Ping(ctx context.Context) error {
	if err := p.master.Ping(ctx); err != nil {
		return fmt.Errorf("master ping failed: %w", err)
	}

	if p.syncReplica != nil {
		if err := p.syncReplica.Ping(ctx); err != nil {
			return fmt.Errorf("sync replica ping failed: %w", err)
		}
	}
	if p.asyncReplica != nil {
		if err := p.asyncReplica.Ping(ctx); err != nil {
			return fmt.Errorf("async replica ping failed: %w", err)
		}
	}

	return nil
}

// Name returns logical postgres client name.
func (p *MasterReplicaProvider) Name() string {
	return p.name
}

// IsReady returns cached readiness updated by background monitor.
func (p *MasterReplicaProvider) IsReady() bool {
	return p.ready.Load()
}

// Close closes master and replica pools and is safe for repeated calls.
func (p *MasterReplicaProvider) Close() {
	p.closeOnce.Do(func() {
		if p.stopCh != nil {
			close(p.stopCh)
		}
		p.monitorWG.Wait()

		if p.syncReplica != nil {
			p.syncReplica.Close()
		}
		if p.asyncReplica != nil {
			p.asyncReplica.Close()
		}
		if p.master != nil {
			p.master.Close()
		}
	})
}

// startReadinessMonitor launches lightweight background readiness checks.
func (p *MasterReplicaProvider) startReadinessMonitor() {
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
func (p *MasterReplicaProvider) updateReady() {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReadinessTimeout)
	defer cancel()
	p.ready.Store(p.Ping(ctx) == nil)
}

// pickReadPool selects pool for generic reads according to read preference.
func (p *MasterReplicaProvider) pickReadPool() *pgxpool.Pool {
	switch p.readPreference {
	case ReadPreferMaster:
		return p.master
	case ReadReplicaOnly:
		return p.nextFromAnyReplicas()
	case ReadPreferReplica:
		fallthrough
	default:
		if p.syncReplica == nil && p.asyncReplica == nil {
			return p.master
		}
		return p.nextFromAnyReplicas()
	}
}

// pickSyncReplicaPool selects sync replica pool with round-robin.
func (p *MasterReplicaProvider) pickSyncReplicaPool() *pgxpool.Pool {
	return p.syncReplica
}

// pickAsyncReplicaPool selects async replica pool with round-robin.
func (p *MasterReplicaProvider) pickAsyncReplicaPool() *pgxpool.Pool {
	return p.asyncReplica
}

// nextFromAnyReplicas selects sync/async replica according to availability.
func (p *MasterReplicaProvider) nextFromAnyReplicas() *pgxpool.Pool {
	if p.syncReplica == nil && p.asyncReplica == nil {
		return nil
	}
	if p.syncReplica != nil && p.asyncReplica == nil {
		return p.syncReplica
	}
	if p.syncReplica == nil && p.asyncReplica != nil {
		return p.asyncReplica
	}
	if p.rrAny.Add(1)%2 == 0 {
		return p.syncReplica
	}
	return p.asyncReplica
}
