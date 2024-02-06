package pgstream

import (
	"context"
	"github.com/ArkamFahry/pgstream/internal/replication"
	"github.com/ArkamFahry/pgstream/internal/schemas"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
	"time"
)

const outputPlugin = "wal2json"

var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Stream struct {
	pgConn                     *pgconn.PgConn
	pgConfig                   *pgconn.Config
	ctx                        context.Context
	cancel                     context.CancelFunc
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan replication.Wal2JsonChanges
	snapshotMessages           chan replication.Wal2JsonChanges
	snapshotName               string
	changeFilter               replication.ChangeFilter
	listenerStart              pglogrepl.LSN
	slotName                   string
	schema                     string
	tableSchemas               []schemas.DataTableSchema
	tableNames                 []string
	separateChanges            bool
	snapshotBatchSize          int
	snapshotMemorySafetyFactor float64
	logger                     *zap.Logger
}
