package pgstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ArkamFahry/pgstream/internal/helpers"
	"github.com/ArkamFahry/pgstream/internal/replication"
	"github.com/ArkamFahry/pgstream/internal/schemas"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
)

const outputPlugin = "wal2json"

var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Stream struct {
	pgConn                     *pgconn.PgConn
	pgConfig                   pgconn.Config
	ctx                        context.Context
	cancel                     context.CancelFunc
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan replication.Wal2JsonChanges
	snapshotMessages           chan replication.Wal2JsonChanges
	snapshotName               string
	changeFilter               replication.ChangeFilter
	lsnrestart                 pglogrepl.LSN
	slotName                   string
	schema                     string
	tableSchemas               []schemas.DataTableSchema
	tableNames                 []string
	separateChanges            bool
	snapshotBatchSize          int
	snapshotMemorySafetyFactor float64
	logger                     *zap.Logger
}

func NewStream(config Config, logger *zap.Logger) (*Stream, error) {
	var (
		cfg *pgconn.Config
		err error
	)
	if cfg, err = pgconn.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		config.DatabaseUser,
		config.DatabasePassword,
		config.DatabaseHost,
		config.DatabasePort,
		config.DatabaseName,
	)); err != nil {
		return nil, err
	}
	if config.TlsVerify == TlsRequireVerify {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	var tableNames []string
	var dataSchemas []schemas.DataTableSchema
	for _, table := range config.DatabaseTables {
		tableNames = append(tableNames, strings.Split(table.Name, ".")[1])
		var dts schemas.DataTableSchema
		dts.TableName = table.Name
		var arrowSchemaFields []arrow.Field
		for _, col := range table.Columns {
			arrowSchemaFields = append(arrowSchemaFields, arrow.Field{
				Name:     col.Name,
				Type:     helpers.MapPlainTypeToArrow(col.InternalType),
				Nullable: col.Nullable,
				Metadata: arrow.Metadata{},
			})
		}
		dts.Schema = arrow.NewSchema(arrowSchemaFields, nil)
		dataSchemas = append(dataSchemas, dts)
	}

	stream := &Stream{
		pgConn:                     dbConn,
		pgConfig:                   *cfg,
		messages:                   make(chan replication.Wal2JsonChanges),
		snapshotMessages:           make(chan replication.Wal2JsonChanges, 100),
		slotName:                   config.ReplicationSlotName,
		schema:                     config.DatabaseSchema,
		tableSchemas:               dataSchemas,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		separateChanges:            config.SeparateChanges,
		snapshotBatchSize:          config.BatchSize,
		tableNames:                 tableNames,
		changeFilter:               replication.NewChangeFilter(dataSchemas, config.DatabaseSchema),
		logger:                     logger,
	}

	result := stream.pgConn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS pglog_stream_%s;", config.ReplicationSlotName))
	_, err = result.ReadAll()
	if err != nil {
		stream.logger.Error("drop publication if exists error", zap.Error(err))
	}

	//
	for i, table := range tableNames {
		tableNames[i] = fmt.Sprintf("%s.%s", config.DatabaseSchema, table)
	}

	tablesSchemaFilter := fmt.Sprintf("FOR TABLE %s", strings.Join(tableNames, ","))
	stream.logger.Info("create publication for table schemas with query", zap.String("query", fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter)))
	result = stream.pgConn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	_, err = result.ReadAll()
	if err != nil {
		stream.logger.Fatal("create publication error", zap.Error(err))
	}
	stream.logger.Info("created Postgresql publication", zap.String("publication_name", config.ReplicationSlotName))

	sysident, err := pglogrepl.IdentifySystem(context.Background(), stream.pgConn)
	if err != nil {
		stream.logger.Fatal("failed to identify the system", zap.Error(err))
	}

	stream.logger.Info("system identification result", zap.String("SystemID", sysident.SystemID), zap.Int32("Timeline", sysident.Timeline), zap.Any("XLogPos", sysident.XLogPos), zap.String("DBName:", sysident.DBName))

	var freshlyCreatedSlot = false
	var confirmedLSNFromDB string
	// check is replication slot exist to get last restart SLN
	connExecResult := stream.pgConn.Exec(context.TODO(), fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.ReplicationSlotName))
	if slotCheckResults, err := connExecResult.ReadAll(); err != nil {
		stream.logger.Fatal("reading replication slot check results failed", zap.Error(err))
	} else {
		if len(slotCheckResults) == 0 || len(slotCheckResults[0].Rows) == 0 {
			// here we create a new replication slot because there is no slot found
			var createSlotResult replication.CreateReplicationSlotResult
			createSlotResult, err = replication.CreateReplicationSlot(context.Background(), stream.pgConn, stream.slotName, outputPlugin,
				replication.CreateReplicationSlotOptions{Temporary: false,
					SnapshotAction: "export",
				})
			if err != nil {
				stream.logger.Fatal("failed to create replication slot for the database", zap.Error(err))
			}
			stream.snapshotName = createSlotResult.SnapshotName
			freshlyCreatedSlot = true
		} else {
			slotCheckRow := slotCheckResults[0].Rows[0]
			confirmedLSNFromDB = string(slotCheckRow[0])
			stream.logger.Info("replication slot restart LSN extracted from DB", zap.String("LSN", confirmedLSNFromDB))
		}
	}

	var lsnrestart pglogrepl.LSN
	if freshlyCreatedSlot {
		lsnrestart = sysident.XLogPos
	} else {
		lsnrestart, _ = pglogrepl.ParseLSN(confirmedLSNFromDB)
	}

	stream.lsnrestart = lsnrestart

	if freshlyCreatedSlot {
		stream.clientXLogPos = sysident.XLogPos
	} else {
		stream.clientXLogPos = lsnrestart
	}

	stream.standbyMessageTimeout = time.Second * 10
	stream.nextStandbyMessageDeadline = time.Now().Add(stream.standbyMessageTimeout)
	stream.ctx, stream.cancel = context.WithCancel(context.Background())

	if !freshlyCreatedSlot || config.StreamOldData == false {
		stream.startLr()
		go stream.streamMessagesAsync()
	} else {
		// New messages will be streamed after the snapshot has been processed.
		go stream.processSnapshot()
	}

	return stream, err
}

func (s *Stream) startLr() {
	var err error
	err = pglogrepl.StartReplication(context.Background(), s.pgConn, s.slotName, s.lsnrestart, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		s.logger.Fatal("starting replication slot failed", zap.Error(err))
	}
	s.logger.Info("started logical replication on slot", zap.String("slot_name", s.slotName))
}

func (s *Stream) AckLSN(lsn string) {
	var err error
	s.clientXLogPos, err = pglogrepl.ParseLSN(lsn)
	if err != nil {
		s.logger.Fatal("failed to parse LSN for Acknowledge", zap.Error(err))
	}

	err = pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.clientXLogPos,
		WALFlushPosition: s.clientXLogPos,
	})

	if err != nil {
		s.logger.Fatal("SendStandbyStatusUpdate failed", zap.Error(err))
	}
	s.logger.Debug("Sent Standby status message at LSN", zap.String("clientXLogPos", s.clientXLogPos.String()))
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
}

func (s *Stream) streamMessagesAsync() {
	for {
		select {
		case <-s.ctx.Done():
			s.cancel()
			return
		default:
			if time.Now().After(s.nextStandbyMessageDeadline) {
				var err error
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: s.clientXLogPos,
				})

				if err != nil {
					s.logger.Fatal("SendStandbyStatusUpdate failed", zap.Error(err))
				}
				s.logger.Debug("sent Standby status message at LSN", zap.String("clientXLogPos", s.clientXLogPos.String()))
				s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
			rawMsg, err := s.pgConn.ReceiveMessage(ctx)
			s.cancel = cancel
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				s.logger.Fatal("failed to receive messages from PostgreSQL", zap.Error(err))
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				s.logger.Fatal("received broken Postgres WAL", zap.Any("errMsg", errMsg))
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				s.logger.Warn("received unexpected message", zap.Any("rawMsg", rawMsg))
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					s.logger.Fatal("parsing PrimaryKeepaliveMessage failed", zap.Error(err))
				}

				if pkm.ReplyRequested {
					s.nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					s.logger.Fatal("parsing XLogData failed", zap.Error(err))
				}
				clientXLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				s.changeFilter.FilterChange(clientXLogPos.String(), xld.WALData, func(change replication.Wal2JsonChanges) {
					s.messages <- change
				})
			}
		}
	}
}
func (s *Stream) processSnapshot() {
	snapshotter, err := replication.NewSnapshotter(s.pgConfig, s.snapshotName)
	if err != nil {
		s.logger.Error("failed to create database snapshot", zap.Error(err))
		s.cleanUpOnFailure()
		os.Exit(1)
	}
	if err = snapshotter.Prepare(); err != nil {
		s.logger.Error("failed to prepare database snapshot", zap.Error(err))
		s.cleanUpOnFailure()
		os.Exit(1)
	}
	defer func() {
		if err = snapshotter.ReleaseSnapshot(); err != nil {
			s.logger.Error("failed to release database snapshot", zap.Error(err))
		}
		if err = snapshotter.CloseConn(); err != nil {
			s.logger.Error("failed to close database snapshot connection", zap.Error(err))
		}
	}()

	for _, table := range s.tableSchemas {
		s.logger.Info("processing database snapshot", zap.String("schema", s.schema), zap.Any("table", table))

		var offset = 0

		pk, err := s.getPrimaryKeyColumn(table.TableName)
		if err != nil {
			s.logger.Fatal("failed to resolve pk %s", zap.Error(err))
		}

		s.logger.Info("query snapshot", zap.Int("batch_size", s.snapshotBatchSize))
		builder := array.NewRecordBuilder(memory.DefaultAllocator, table.Schema)

		colNames := make([]string, 0, len(table.Schema.Fields()))

		for _, col := range table.Schema.Fields() {
			colNames = append(colNames, pgx.Identifier{col.Name}.Sanitize())
		}

		for {
			var snapshotRows pgx.Rows
			s.logger.Info("query snapshot", zap.String("table", table.TableName), zap.Strings("columns", colNames), zap.Int("batch_size", s.snapshotBatchSize), zap.Int("offset", offset))
			if snapshotRows, err = snapshotter.QuerySnapshotData(table.TableName, colNames, pk, s.snapshotBatchSize, offset); err != nil {
				s.logger.Error("failed to query snapshot data", zap.Error(err))
				s.cleanUpOnFailure()
				os.Exit(1)
			}

			var rowsCount = 0
			for snapshotRows.Next() {
				rowsCount += 1

				values, err := snapshotRows.Values()
				if err != nil {
					panic(err)
				}

				for i, v := range values {
					s := scalar.NewScalar(table.Schema.Field(i).Type)
					if err := s.Set(v); err != nil {
						panic(err)
					}

					scalar.AppendToBuilder(builder.Field(i), s)
				}
				var snapshotChanges = replication.Wal2JsonChanges{
					Lsn: "",
					Changes: []replication.Wal2JsonChange{
						{
							Kind:   "insert",
							Schema: s.schema,
							Table:  strings.Split(table.TableName, ".")[1],
							Row:    builder.NewRecord(),
						},
					},
				}

				s.snapshotMessages <- snapshotChanges
			}

			snapshotRows.Close()

			offset += s.snapshotBatchSize

			if s.snapshotBatchSize != rowsCount {
				break
			}
		}

	}

	s.startLr()
	go s.streamMessagesAsync()
}

func (s *Stream) OnMessage(callback OnMessage) {
	for {
		select {
		case snapshotMessage := <-s.snapshotMessages:
			callback(snapshotMessage)
		case message := <-s.messages:
			callback(message)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Stream) SnapshotMessageC() chan replication.Wal2JsonChanges {
	return s.snapshotMessages
}

func (s *Stream) LrMessageC() chan replication.Wal2JsonChanges {
	return s.messages
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Stream) cleanUpOnFailure() {
	s.logger.Warn("cleaning up replication slot and resources on accident.", zap.String("replication_slot", s.slotName))
	err := replication.DropReplicationSlot(context.Background(), s.pgConn, s.slotName, replication.DropReplicationSlotOptions{Wait: true})
	if err != nil {
		s.logger.Error("failed to drop replication slot", zap.Error(err))
	}
	if err = s.pgConn.Close(context.TODO()); err != nil {
		s.logger.Error("failed to close database connection", zap.Error(err))
	}
}

func (s *Stream) getPrimaryKeyColumn(tableName string) (string, error) {
	q := fmt.Sprintf(`
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
							 AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = '%s'::regclass
		AND    i.indisprimary;	
	`, strings.Split(tableName, ".")[1])

	reader := s.pgConn.Exec(context.Background(), q)
	data, err := reader.ReadAll()
	if err != nil {
		return "", err
	}

	pkResultRow := data[0].Rows[0]
	pkColName := string(pkResultRow[0])
	return pkColName, nil
}

func (s *Stream) Stop() error {
	if s.pgConn != nil {
		if s.ctx != nil {
			s.cancel()
		}

		return s.pgConn.Close(context.TODO())
	}

	return nil
}
