package smpostgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMasterReplicaProvider_NewMasterReplica(t *testing.T) {
	t.Run("replica-only without replicas", func(t *testing.T) {
		_, err := NewMasterReplica(context.Background(), MasterReplicaConfig{
			ReadPreference: ReadReplicaOnly,
			Master: NodeConfig{
				DSN: "postgres://user:pass@localhost:5432/db?sslmode=disable",
			},
		})
		if err == nil {
			t.Fatal("expected error for missing replicas")
		}
	})

	t.Run("valid sync async config", func(t *testing.T) {
		p, err := NewMasterReplica(context.Background(), MasterReplicaConfig{
			Name:           "users-cluster",
			ReadPreference: ReadPreferReplica,
			Master: NodeConfig{
				DSN: "postgres://user:pass@localhost:5432/db?sslmode=disable",
			},
			SyncReplica:  &NodeConfig{DSN: "postgres://user:pass@localhost:5433/db?sslmode=disable"},
			AsyncReplica: &NodeConfig{DSN: "postgres://user:pass@localhost:5434/db?sslmode=disable"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Name() != "users-cluster" {
			t.Fatalf("expected provider name users-cluster, got %q", p.Name())
		}
		p.Close()
	})
}

func TestMasterReplicaProvider_Routing(t *testing.T) {
	master := new(pgxpool.Pool)
	syncReplica := new(pgxpool.Pool)
	asyncReplica := new(pgxpool.Pool)

	p := &MasterReplicaProvider{
		readPreference: ReadPreferReplica,
		master:         master,
		syncReplica:    syncReplica,
		asyncReplica:   asyncReplica,
	}

	if got := p.pickSyncReplicaPool(); got != syncReplica {
		t.Fatal("expected sync replica pool")
	}
	if got := p.pickAsyncReplicaPool(); got != asyncReplica {
		t.Fatal("expected async replica pool")
	}
	if got := p.pickReadPool(); got == nil {
		t.Fatal("expected read pool with prefer-replica")
	}

	p.readPreference = ReadPreferMaster
	if got := p.pickReadPool(); got != master {
		t.Fatal("expected master pool with prefer-master")
	}
}

func TestMasterReplicaProvider_NewFromGenericConfig(t *testing.T) {
	p, err := New(context.Background(), Config{
		Name:           "events-cluster",
		Mode:           ModeMasterSyncAsyncReplica,
		ReadPreference: ReadPreferReplica,
		Master: &NodeConfig{
			DSN: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		},
		SyncReplica:  &NodeConfig{DSN: "postgres://user:pass@localhost:5433/db?sslmode=disable"},
		AsyncReplica: &NodeConfig{DSN: "postgres://user:pass@localhost:5434/db?sslmode=disable"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer p.Close()

	if _, ok := p.(*MasterReplicaProvider); !ok {
		t.Fatalf("expected *MasterReplicaProvider, got %T", p)
	}
	if p.Name() != "events-cluster" {
		t.Fatalf("expected provider name events-cluster, got %q", p.Name())
	}
}
