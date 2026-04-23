# smpostgres

Легкая библиотека-провайдер над `pgx/v5` с простым сценарием:

1. Передать конфиг.
2. Вызвать `smpostgres.Init(...)` или `smpostgres.New(...)`.
3. Работать через единый интерфейс `Provider`.

Поддерживаемые режимы:

- `single` - один инстанс PostgreSQL.
- `master-sync-async-replica` - один master и набор sync/async replicas.

## Установка

```bash
go get github.com/overiss/smpostgres
```

## Быстрый старт (single)

```go
package main

import (
	"context"
	"log"

	"github.com/overiss/smpostgres"
)

func main() {
	ctx := context.Background()

	err := smpostgres.Init(ctx, smpostgres.Config{
		Name: "users-postgres",
		Mode: smpostgres.ModeSingle,
		Single: &smpostgres.NodeConfig{
			DSN: "postgres://user:pass@localhost:5432/appdb?sslmode=disable",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer smpostgres.CloseDefault()

	db := smpostgres.MustDefault()

	if _, err := db.Exec(ctx, "insert into users(name) values($1)", "max"); err != nil {
		log.Fatal(err)
	}

	row := db.QueryRow(ctx, "select count(*) from users")
	var total int
	if err := row.Scan(&total); err != nil {
		log.Fatal(err)
	}
}
```

## Быстрый старт (master/sync-replica/async-replica)

```go
ctx := context.Background()

err := smpostgres.Init(ctx, smpostgres.Config{
	Name:           "billing-postgres",
	Mode:           smpostgres.ModeMasterSyncAsyncReplica,
	ReadPreference: smpostgres.ReadPreferReplica,
	Master: &smpostgres.NodeConfig{
		DSN: "postgres://writer:pass@pg-master:5432/appdb?sslmode=disable",
	},
	SyncReplicas: []smpostgres.NodeConfig{
		{DSN: "postgres://reader:pass@pg-replica-sync-1:5432/appdb?sslmode=disable"},
	},
	AsyncReplicas: []smpostgres.NodeConfig{
		{DSN: "postgres://reader:pass@pg-replica-async-1:5432/appdb?sslmode=disable"},
	},
})
if err != nil {
	// handle error
}
```

Для явной работы с sync/async репликами:

```go
cluster, err := smpostgres.NewMasterReplica(ctx, smpostgres.MasterReplicaConfig{
	Name:           "billing-postgres",
	ReadPreference: smpostgres.ReadPreferReplica,
	Master: smpostgres.NodeConfig{
		DSN: "postgres://writer:pass@pg-master:5432/appdb?sslmode=disable",
	},
	SyncReplicas: []smpostgres.NodeConfig{
		{DSN: "postgres://reader:pass@pg-replica-sync-1:5432/appdb?sslmode=disable"},
	},
	AsyncReplicas: []smpostgres.NodeConfig{
		{DSN: "postgres://reader:pass@pg-replica-async-1:5432/appdb?sslmode=disable"},
	},
})
if err != nil {
	// handle error
}
defer cluster.Close()

_, _ = cluster.QuerySyncReplica(ctx, "select * from users where id = $1", 42)
_, _ = cluster.QueryAsyncReplica(ctx, "select * from users where id = $1", 42)
```

Маршрутизация:

- `Exec` и `Begin` всегда идут в `master`.
- `Query` и `QueryRow` идут в зависимости от `ReadPreference`.
- `QueryMaster` и `QueryRowMaster` принудительно читают из `master`.
- `QuerySyncReplica`/`QueryRowSyncReplica` читают только из sync replica.
- `QueryAsyncReplica`/`QueryRowAsyncReplica` читают только из async replica.

## ReadPreference

- `ReadPreferReplica` (по умолчанию): читать из replica, fallback в master.
- `ReadPreferMaster`: читать из master.
- `ReadReplicaOnly`: читать только из replica, без fallback.

## Readiness Probe

```go
// Пример для /readyz в HTTP-сервисе:
db, err := smpostgres.Default()
if err != nil || !db.IsReady() {
	name := "default-postgres"
	if err == nil && db.Name() != "" {
		name = db.Name()
	}
	http.Error(w, "postgres is not ready: "+name, http.StatusServiceUnavailable)
	return
}
w.WriteHeader(http.StatusOK)
```

- Контракт readiness: `IsReady() bool` и `Name() string`.
- Для default-провайдера есть helper: `smpostgres.IsDefaultReady()`.
