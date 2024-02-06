package pgstream

type TlsVerify string

const TlsNoVerify TlsVerify = "none"
const TlsRequireVerify TlsVerify = "require"

type Column struct {
	Name         string `json:"name" mapstructure:"name"`
	InternalType string `json:"internal_type" mapstructure:"internal_type"`
	NativeType   string `json:"native_type" mapstructure:"native_type"`
	PrimaryKey   bool   `json:"primary_key" mapstructure:"primary_key"`
	Nullable     bool   `json:"nullable" mapstructure:"nullable"`
}

type Table struct {
	Name    string   `json:"name" mapstructure:"name"`
	Columns []Column `json:"columns" mapstructure:"columns"`
}

type Config struct {
	DatabaseHost               string    `json:"database_host" mapstructure:"database_host"`
	DatabasePort               string    `json:"database_port" mapstructure:"database_port"`
	DatabaseUser               string    `json:"database_user" mapstructure:"database_user"`
	DatabasePassword           string    `json:"database_password" mapstructure:"database_password"`
	DatabaseName               string    `json:"database_name" mapstructure:"database_name"`
	DatabaseSchema             string    `json:"database_schema" mapstructure:"database_schema"`
	DatabaseTables             []Table   `json:"database_tables" mapstructure:"database_tables"`
	ReplicationSlotName        string    `json:"replication_slot_name" mapstructure:"replication_slot_name"`
	TlsVerify                  TlsVerify `json:"tls_verify" mapstructure:"tls_verify"`
	StreamOldData              bool      `json:"stream_old_data" mapstructure:"stream_old_data"`
	SeparateChanges            bool      `json:"separate_changes" mapstructure:"separate_changes"`
	SnapshotMemorySafetyFactor float64   `json:"snapshot_memory_safety_factor" mapstructure:"snapshot_memory_safety_factor"`
	BatchSize                  int       `json:"batch_size" mapstructure:"batch_size"`
}
