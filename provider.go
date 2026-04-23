package smpostgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotInitialized = errors.New("smpostgres is not initialized")
	ErrNoReadPool     = errors.New("no read pool available for configured read preference")
	ErrNoSyncReplica  = errors.New("no sync replica pool configured")
	ErrNoAsyncReplica = errors.New("no async replica pool configured")
)

type Provider interface {
	Name() string
	IsReady() bool

	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	QueryMaster(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRowMaster(ctx context.Context, sql string, args ...any) pgx.Row

	Begin(ctx context.Context) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

func New(ctx context.Context, cfg Config) (Provider, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	switch cfg.Mode {
	case ModeSingle:
		return NewNamedSingle(ctx, cfg.Name, *cfg.Single)
	case ModeMasterSyncAsyncReplica, ModeMasterReplica:
		return NewMasterReplica(ctx, MasterReplicaConfig{
			Name:           cfg.Name,
			ReadPreference: cfg.readPreferenceOrDefault(),
			Master:         *cfg.Master,
			SyncReplicas:   append([]NodeConfig{}, cfg.SyncReplicas...),
			AsyncReplicas:  append([]NodeConfig{}, cfg.AsyncReplicas...),
		})
	default:
		return nil, fmt.Errorf("unsupported mode %q", cfg.Mode)
	}
}

func newPool(ctx context.Context, node NodeConfig) (*pgxpool.Pool, error) {
	pgxCfg, err := pgxpool.ParseConfig(node.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	if node.MaxConns > 0 {
		pgxCfg.MaxConns = node.MaxConns
	}
	if node.MinConns > 0 {
		pgxCfg.MinConns = node.MinConns
	}
	if node.MaxConnLifetime > 0 {
		pgxCfg.MaxConnLifetime = node.MaxConnLifetime
	}
	if node.MaxConnIdleTime > 0 {
		pgxCfg.MaxConnIdleTime = node.MaxConnIdleTime
	}
	if node.HealthCheckPeriod > 0 {
		pgxCfg.HealthCheckPeriod = node.HealthCheckPeriod
	}
	if node.ConnectTimeout > 0 {
		pgxCfg.ConnConfig.ConnectTimeout = node.ConnectTimeout
	}

	return pgxpool.NewWithConfig(ctx, pgxCfg)
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(dest ...any) error {
	return r.err
}

var (
	defaultMu       sync.RWMutex
	defaultProvider Provider
)

func Init(ctx context.Context, cfg Config) error {
	p, err := New(ctx, cfg)
	if err != nil {
		return err
	}

	defaultMu.Lock()
	defer defaultMu.Unlock()

	if defaultProvider != nil {
		defaultProvider.Close()
	}
	defaultProvider = p
	return nil
}

func Default() (Provider, error) {
	defaultMu.RLock()
	defer defaultMu.RUnlock()

	if defaultProvider == nil {
		return nil, ErrNotInitialized
	}

	return defaultProvider, nil
}

func MustDefault() Provider {
	p, err := Default()
	if err != nil {
		panic(err)
	}
	return p
}

func CloseDefault() {
	defaultMu.Lock()
	defer defaultMu.Unlock()

	if defaultProvider != nil {
		defaultProvider.Close()
		defaultProvider = nil
	}
}

func PingDefault(ctx context.Context) error {
	p, err := Default()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.Ping(ctx)
}

const DefaultReadinessTimeout = 2 * time.Second

func IsDefaultReady() bool {
	p, err := Default()
	if err != nil {
		return false
	}

	return p.IsReady()
}
