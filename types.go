package pgstream

import "github.com/ArkamFahry/pgstream/internal/replication"

type OnMessage = func(message replication.Wal2JsonChanges)
