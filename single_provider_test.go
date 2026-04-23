package smpostgres

import (
	"context"
	"testing"
)

func TestSingleProvider_NewSingle(t *testing.T) {
	t.Run("invalid dsn", func(t *testing.T) {
		_, err := NewSingle(context.Background(), NodeConfig{DSN: "://bad"})
		if err == nil {
			t.Fatal("expected error for invalid dsn")
		}
	})

	t.Run("valid config", func(t *testing.T) {
		p, err := NewNamedSingle(context.Background(), "orders-postgres", NodeConfig{
			DSN: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Name() != "orders-postgres" {
			t.Fatalf("expected provider name orders-postgres, got %q", p.Name())
		}
		p.Close()
	})

	t.Run("min conns exceeds auto max", func(t *testing.T) {
		_, err := NewSingle(context.Background(), NodeConfig{
			DSN:      "postgres://user:pass@localhost:5432/db?sslmode=disable",
			MinConns: defaultMaxConns() + 1,
		})
		if err == nil {
			t.Fatal("expected error for min conns above auto max")
		}
	})
}

func TestSingleProvider_NewFromGenericConfig(t *testing.T) {
	p, err := New(context.Background(), Config{
		Name: "billing-postgres",
		Mode: ModeSingle,
		Single: &NodeConfig{
			DSN: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer p.Close()

	if _, ok := p.(*SingleProvider); !ok {
		t.Fatalf("expected *SingleProvider, got %T", p)
	}

	if p.Name() != "billing-postgres" {
		t.Fatalf("expected provider name billing-postgres, got %q", p.Name())
	}
}

func TestIsDefaultReady_NotInitialized(t *testing.T) {
	CloseDefault()

	if IsDefaultReady() {
		t.Fatal("expected IsDefaultReady to be false for uninitialized provider")
	}
}
