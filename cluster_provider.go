package smpostgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterReplicaConfig struct {
	Name           string
	ReadPreference ReadPreference
	Master         NodeConfig
	SyncReplicas   []NodeConfig
	AsyncReplicas  []NodeConfig
}

// MasterReplicaProvider is an isolated entity for cluster mode:
// master + sync replicas + async replicas in one client.
type MasterReplicaProvider struct {
	name           string
	readPreference ReadPreference

	master        *pgxpool.Pool
	syncReplicas  []*pgxpool.Pool
	asyncReplicas []*pgxpool.Pool
	anyReplicas   []*pgxpool.Pool

	closeOnce sync.Once
	rrAny     atomic.Uint64
	rrSync    atomic.Uint64
	rrAsync   atomic.Uint64
}

func NewMasterReplica(ctx context.Context, cfg MasterReplicaConfig) (*MasterReplicaProvider, error) {
	if err := validateNode("master", cfg.Master); err != nil {
		return nil, err
	}
	for idx, replica := range cfg.SyncReplicas {
		if err := validateNode(fmt.Sprintf("sync replica[%d]", idx), replica); err != nil {
			return nil, err
		}
	}
	for idx, replica := range cfg.AsyncReplicas {
		if err := validateNode(fmt.Sprintf("async replica[%d]", idx), replica); err != nil {
			return nil, err
		}
	}

	readPreference := cfg.ReadPreference
	if readPreference == "" {
		readPreference = ReadPreferReplica
	}
	if readPreference == ReadReplicaOnly && len(cfg.SyncReplicas)+len(cfg.AsyncReplicas) == 0 {
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
		syncReplicas:   make([]*pgxpool.Pool, 0, len(cfg.SyncReplicas)),
		asyncReplicas:  make([]*pgxpool.Pool, 0, len(cfg.AsyncReplicas)),
	}

	for _, replicaCfg := range cfg.SyncReplicas {
		replicaPool, replicaErr := newPool(ctx, replicaCfg)
		if replicaErr != nil {
			p.Close()
			return nil, fmt.Errorf("create sync replica pool: %w", replicaErr)
		}
		p.syncReplicas = append(p.syncReplicas, replicaPool)
		p.anyReplicas = append(p.anyReplicas, replicaPool)
	}
	for _, replicaCfg := range cfg.AsyncReplicas {
		replicaPool, replicaErr := newPool(ctx, replicaCfg)
		if replicaErr != nil {
			p.Close()
			return nil, fmt.Errorf("create async replica pool: %w", replicaErr)
		}
		p.asyncReplicas = append(p.asyncReplicas, replicaPool)
		p.anyReplicas = append(p.anyReplicas, replicaPool)
	}

	return p, nil
}

func (p *MasterReplicaProvider) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.master.Exec(ctx, sql, arguments...)
}

func (p *MasterReplicaProvider) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	readPool := p.pickReadPool()
	if readPool == nil {
		return nil, ErrNoReadPool
	}
	return readPool.Query(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	readPool := p.pickReadPool()
	if readPool == nil {
		return errorRow{err: ErrNoReadPool}
	}
	return readPool.QueryRow(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QueryMaster(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.master.Query(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QueryRowMaster(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.master.QueryRow(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QuerySyncReplica(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	readPool := p.pickSyncReplicaPool()
	if readPool == nil {
		return nil, ErrNoSyncReplica
	}
	return readPool.Query(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QueryRowSyncReplica(ctx context.Context, sql string, args ...any) pgx.Row {
	readPool := p.pickSyncReplicaPool()
	if readPool == nil {
		return errorRow{err: ErrNoSyncReplica}
	}
	return readPool.QueryRow(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QueryAsyncReplica(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	readPool := p.pickAsyncReplicaPool()
	if readPool == nil {
		return nil, ErrNoAsyncReplica
	}
	return readPool.Query(ctx, sql, args...)
}

func (p *MasterReplicaProvider) QueryRowAsyncReplica(ctx context.Context, sql string, args ...any) pgx.Row {
	readPool := p.pickAsyncReplicaPool()
	if readPool == nil {
		return errorRow{err: ErrNoAsyncReplica}
	}
	return readPool.QueryRow(ctx, sql, args...)
}

func (p *MasterReplicaProvider) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.master.Begin(ctx)
}

func (p *MasterReplicaProvider) Ping(ctx context.Context) error {
	if err := p.master.Ping(ctx); err != nil {
		return fmt.Errorf("master ping failed: %w", err)
	}

	for idx, readPool := range p.syncReplicas {
		if err := readPool.Ping(ctx); err != nil {
			return fmt.Errorf("sync replica[%d] ping failed: %w", idx, err)
		}
	}
	for idx, readPool := range p.asyncReplicas {
		if err := readPool.Ping(ctx); err != nil {
			return fmt.Errorf("async replica[%d] ping failed: %w", idx, err)
		}
	}

	return nil
}

func (p *MasterReplicaProvider) Name() string {
	return p.name
}

func (p *MasterReplicaProvider) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReadinessTimeout)
	defer cancel()
	return p.Ping(ctx) == nil
}

func (p *MasterReplicaProvider) Close() {
	p.closeOnce.Do(func() {
		for _, readPool := range p.syncReplicas {
			readPool.Close()
		}
		for _, readPool := range p.asyncReplicas {
			readPool.Close()
		}
		if p.master != nil {
			p.master.Close()
		}
	})
}

func (p *MasterReplicaProvider) pickReadPool() *pgxpool.Pool {
	switch p.readPreference {
	case ReadPreferMaster:
		return p.master
	case ReadReplicaOnly:
		if len(p.anyReplicas) == 0 {
			return nil
		}
		return p.nextFromAnyReplicas()
	case ReadPreferReplica:
		fallthrough
	default:
		if len(p.anyReplicas) == 0 {
			return p.master
		}
		return p.nextFromAnyReplicas()
	}
}

func (p *MasterReplicaProvider) pickSyncReplicaPool() *pgxpool.Pool {
	return nextPool(p.syncReplicas, &p.rrSync)
}

func (p *MasterReplicaProvider) pickAsyncReplicaPool() *pgxpool.Pool {
	return nextPool(p.asyncReplicas, &p.rrAsync)
}

func (p *MasterReplicaProvider) nextFromAnyReplicas() *pgxpool.Pool {
	return nextPool(p.anyReplicas, &p.rrAny)
}

func nextPool(pools []*pgxpool.Pool, rr *atomic.Uint64) *pgxpool.Pool {
	if len(pools) == 0 {
		return nil
	}
	if len(pools) == 1 {
		return pools[0]
	}

	next := rr.Add(1)
	idx := int(next % uint64(len(pools)))
	return pools[idx]
}
