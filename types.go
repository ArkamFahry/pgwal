package pgstream

import "github.com/ArkamFahry/pgwal/internal/replication"

type OnMessage = func(message replication.Wal2JsonChanges)
