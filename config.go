package smpostgres

import (
	"errors"
	"fmt"
	"time"
)

type DeploymentMode string

const (
	ModeSingle DeploymentMode = "single"
	// ModeMasterSyncAsyncReplica runs with one master and optional sync/async replicas.
	ModeMasterSyncAsyncReplica DeploymentMode = "master-sync-async-replica"
	// ModeMasterReplica is kept for backward compatibility.
	ModeMasterReplica DeploymentMode = "master-replica"
)

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
	DSN string

	// Optional pgx pool tuning values.
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

type Config struct {
	// Name is a logical postgres client name
	// used in readiness and diagnostics.
	Name string

	Mode           DeploymentMode
	ReadPreference ReadPreference

	Single *NodeConfig

	Master        *NodeConfig
	SyncReplicas  []NodeConfig
	AsyncReplicas []NodeConfig
}

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

		for idx, replica := range c.SyncReplicas {
			if err := validateNode(fmt.Sprintf("sync replica[%d]", idx), replica); err != nil {
				return err
			}
		}
		for idx, replica := range c.AsyncReplicas {
			if err := validateNode(fmt.Sprintf("async replica[%d]", idx), replica); err != nil {
				return err
			}
		}

		if c.readPreferenceOrDefault() == ReadReplicaOnly &&
			len(c.SyncReplicas)+len(c.AsyncReplicas) == 0 {
			return errors.New("read preference replica-only requires at least one replica")
		}
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}
}

func (c Config) readPreferenceOrDefault() ReadPreference {
	if c.ReadPreference == "" {
		return ReadPreferReplica
	}
	return c.ReadPreference
}

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
