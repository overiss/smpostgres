package smpostgres

import (
	"errors"
	"fmt"
	"time"
)

// DeploymentMode defines postgres topology used by the provider.
type DeploymentMode string

const (
	// ModeSingle configures provider for one postgres instance.
	ModeSingle DeploymentMode = "single"
	// ModeMasterSyncAsyncReplica runs with one master and optional sync/async replicas.
	ModeMasterSyncAsyncReplica DeploymentMode = "master-sync-async-replica"
	// ModeMasterReplica is kept for backward compatibility.
	ModeMasterReplica DeploymentMode = "master-replica"
)

// ReadPreference controls where generic read operations are routed.
type ReadPreference string

const (
	// ReadPreferReplica routes reads to replicas first and
	// falls back to master when no replicas are configured.
	ReadPreferReplica ReadPreference = "prefer-replica"
	// ReadPreferMaster keeps reads on master.
	ReadPreferMaster ReadPreference = "prefer-master"
	// ReadReplicaOnly requires at least one replica for reads.
	ReadReplicaOnly ReadPreference = "replica-only"
)

type NodeConfig struct {
	// DSN is a postgres connection string.
	DSN string

	// MaxConns limits maximum connections in pool (0 means auto-tuned default).
	MaxConns int32
	// MinConns keeps minimum number of established connections.
	MinConns int32
	// MaxConnLifetime closes a connection after this lifetime.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime closes idle connection after this duration.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod controls background pool health checks.
	HealthCheckPeriod time.Duration
	// ConnectTimeout limits time for establishing a new connection.
	ConnectTimeout time.Duration
}

// Config is top-level package configuration used by New and Init.
type Config struct {
	// Name is a logical postgres client name
	// used in readiness and diagnostics.
	Name string

	// Mode selects single or cluster topology.
	Mode DeploymentMode
	// ReadPreference configures generic read routing in cluster mode.
	ReadPreference ReadPreference

	// Single contains node config for single mode.
	Single *NodeConfig

	// Master contains writer node config for cluster mode.
	Master *NodeConfig
	// SyncReplica contains synchronous replica node for cluster mode.
	SyncReplica *NodeConfig
	// AsyncReplica contains asynchronous replica node for cluster mode.
	AsyncReplica *NodeConfig
}

// validate checks that config structure is consistent for selected mode.
func (c Config) validate() error {
	if c.Mode == "" {
		return errors.New("mode is required")
	}

	switch c.Mode {
	case ModeSingle:
		if c.Single == nil {
			return errors.New("single config is required for single mode")
		}
		return validateNode("single", *c.Single)
	case ModeMasterSyncAsyncReplica, ModeMasterReplica:
		if c.Master == nil {
			return errors.New("master config is required for master-sync-async-replica mode")
		}
		if err := validateNode("master", *c.Master); err != nil {
			return err
		}

		if c.SyncReplica != nil {
			if err := validateNode("sync replica", *c.SyncReplica); err != nil {
				return err
			}
		}
		if c.AsyncReplica != nil {
			if err := validateNode("async replica", *c.AsyncReplica); err != nil {
				return err
			}
		}

		if c.readPreferenceOrDefault() == ReadReplicaOnly &&
			c.SyncReplica == nil && c.AsyncReplica == nil {
			return errors.New("read preference replica-only requires at least one replica")
		}
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}
}

// readPreferenceOrDefault returns explicit preference or package default.
func (c Config) readPreferenceOrDefault() ReadPreference {
	if c.ReadPreference == "" {
		return ReadPreferReplica
	}
	return c.ReadPreference
}

// validateNode validates DSN and optional pool tuning values for one node.
func validateNode(name string, node NodeConfig) error {
	if node.DSN == "" {
		return fmt.Errorf("%s dsn is required", name)
	}

	if node.MaxConns < 0 {
		return fmt.Errorf("%s max conns must be >= 0", name)
	}
	if node.MinConns < 0 {
		return fmt.Errorf("%s min conns must be >= 0", name)
	}
	if node.MaxConns > 0 && node.MinConns > node.MaxConns {
		return fmt.Errorf("%s min conns must be <= max conns", name)
	}
	if node.MaxConns == 0 && node.MinConns > defaultMaxConns() {
		return fmt.Errorf("%s min conns must be <= auto max conns (%d)", name, defaultMaxConns())
	}
	if node.MaxConnLifetime < 0 {
		return fmt.Errorf("%s max conn lifetime must be >= 0", name)
	}
	if node.MaxConnIdleTime < 0 {
		return fmt.Errorf("%s max conn idle time must be >= 0", name)
	}
	if node.HealthCheckPeriod < 0 {
		return fmt.Errorf("%s health check period must be >= 0", name)
	}
	if node.ConnectTimeout < 0 {
		return fmt.Errorf("%s connect timeout must be >= 0", name)
	}
	return nil
}
