package storage

import "encoding/json"

// CommandOp identifies the kind of mutation a Command represents.
type CommandOp uint8

const (
	CommandSet    CommandOp = 1
	CommandDelete CommandOp = 2
)

// Command is a KV mutation encoded into a Raft log entry's Command bytes.
// JSON is used for simplicity and debuggability — commands are written
// infrequently relative to, say, WAL records, so encoding performance isn't
// a concern here.
type Command struct {
	Op          CommandOp   `json:"op"`
	Key         string      `json:"key"`
	Value       []byte      `json:"value,omitempty"`
	Consistency Consistency `json:"consistency,omitempty"`
	TTLSeconds  int64       `json:"ttl_seconds,omitempty"`
}

// EncodeCommand serializes c for storage in a Raft log entry.
func EncodeCommand(c Command) ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand reverses EncodeCommand.
func DecodeCommand(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}
