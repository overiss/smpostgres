package smpostgres

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrNotInitialized is returned when default provider was not initialized.
	ErrNotInitialized = errors.New("smpostgres is not initialized")
	// ErrNoReadPool is returned when read route has no available pool.
	ErrNoReadPool = errors.New("no read pool available for configured read preference")
	// ErrNoSyncReplica is returned when sync-only query has no sync replicas.
	ErrNoSyncReplica = errors.New("no sync replica pool configured")
	// ErrNoAsyncReplica is returned when async-only query has no async replicas.
	ErrNoAsyncReplica = errors.New("no async replica pool configured")
)

// Provider is a common abstraction for single and cluster postgres clients.
type Provider interface {
	// Name returns logical postgres client name from config.
	Name() string
	// IsReady returns true when provider can serve requests.
	IsReady() bool

	// Exec executes a write statement on writer pool.
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	// Query executes a read query using configured read routing.
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	// QueryRow executes single-row read query using read routing.
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	// QueryMaster forces read query against master pool.
	QueryMaster(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	// QueryRowMaster forces single-row read query against master pool.
	QueryRowMaster(ctx context.Context, sql string, args ...any) pgx.Row

	// Begin starts transaction on writer pool.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Ping checks availability of all required pools.
	Ping(ctx context.Context) error
	// Close releases all allocated pools.
	Close()
}

// New builds Provider implementation based on config mode.
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
			SyncReplica:    cfg.SyncReplica,
			AsyncReplica:   cfg.AsyncReplica,
		})
	default:
		return nil, fmt.Errorf("unsupported mode %q", cfg.Mode)
	}
}

// DefaultMaxConnsCap limits automatically tuned pool size.
const DefaultMaxConnsCap int32 = 32

// DefaultReadinessCheckInterval is interval for background readiness checks.
const DefaultReadinessCheckInterval = 10 * time.Second

// newPool creates and configures pgx pool for one node.
func newPool(ctx context.Context, node NodeConfig) (*pgxpool.Pool, error) {
	pgxCfg, err := pgxpool.ParseConfig(node.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	if node.MaxConns > 0 {
		pgxCfg.MaxConns = node.MaxConns
	} else {
		pgxCfg.MaxConns = defaultMaxConns()
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

// defaultMaxConns derives conservative per-pool limit from available CPUs.
func defaultMaxConns() int32 {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}

	tuned := int32(workers * 4)
	if tuned < 4 {
		tuned = 4
	}
	if tuned > DefaultMaxConnsCap {
		return DefaultMaxConnsCap
	}
	return tuned
}

// errorRow is a pgx.Row adapter used to return deferred routing errors.
type errorRow struct {
	err error
}

// Scan returns stored error without touching destination values.
func (r errorRow) Scan(dest ...any) error {
	return r.err
}

var (
	defaultMu       sync.RWMutex
	defaultProvider Provider
)

// Init creates provider from config and stores it as package default instance.
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

// Default returns currently configured package default provider.
func Default() (Provider, error) {
	defaultMu.RLock()
	defer defaultMu.RUnlock()

	if defaultProvider == nil {
		return nil, ErrNotInitialized
	}

	return defaultProvider, nil
}

// MustDefault returns default provider or panics when it is missing.
func MustDefault() Provider {
	p, err := Default()
	if err != nil {
		panic(err)
	}
	return p
}

// CloseDefault closes and clears package default provider.
func CloseDefault() {
	defaultMu.Lock()
	defer defaultMu.Unlock()

	if defaultProvider != nil {
		defaultProvider.Close()
		defaultProvider = nil
	}
}

// PingDefault checks connectivity of package default provider.
func PingDefault(ctx context.Context) error {
	p, err := Default()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.Ping(ctx)
}

// DefaultReadinessTimeout is timeout used for one readiness ping attempt.
const DefaultReadinessTimeout = 2 * time.Second

// IsDefaultReady reports readiness for package default provider.
func IsDefaultReady() bool {
	p, err := Default()
	if err != nil {
		return false
	}

	return p.IsReady()
}
