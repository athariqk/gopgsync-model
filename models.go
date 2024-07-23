package pgcdcmodels

import (
	"container/list"
	"errors"
	"strings"
)

type Field struct {
	Content     interface{}
	IsKey       bool
	DataTypeOID uint32
}

type Row struct {
	Namespace string
	RelName   string
	// NOTE: all field names MUST BE a fully qualified name!
	Fields map[string]Field
}

type SQLCommandType uint8

const (
	INSERT SQLCommandType = iota
	DELETE
	UPDATE
)

type DmlCommand struct {
	Data    Row
	CmdType SQLCommandType
}

type ReplicationMessageFlag uint8

const (
	FULL_REPLICATION_PROGRESS ReplicationMessageFlag = iota
	FULL_REPLICATION_NEW_ROWS
	FULL_REPLICATION_FINISHED
	STREAM_REPLICATION
)

type ReplicationMessage struct {
	ReplicationFlag ReplicationMessageFlag
	Command         *DmlCommand
	Total           int
}

// Flattens field map into basic column names (<namespace>.<table>.<field>) with plain field values
func Flatten(columns map[string]Field, onlyKey bool) map[string]interface{} {
	row := map[string]interface{}{}
	for name, field := range columns {
		if onlyKey && !field.IsKey {
			continue
		}
		splits := strings.Split(name, ".")
		if len(splits) == 3 {
			name = splits[2]
		}
		row[name] = field.Content
	}
	return row
}

func CastToDmlCmd(e *list.Element) (*DmlCommand, error) {
	if e == nil {
		return nil, nil
	}
	dmlCommand, ok := e.Value.(*DmlCommand)
	if !ok {
		return nil, errors.New("incorrect casting of value to DmlCommand from queue")
	}
	return dmlCommand, nil
}
