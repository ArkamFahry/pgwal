package pgstream

type CheckPointer interface {
	SetCheckpoint(lsn, slot string) error
	GetCheckpoint(slot string) string
}
