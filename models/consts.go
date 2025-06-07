package models

// Chunks

const (
	BLOCK_MAX_SIZE    int = 100
	MAX_LOGS_IN_CHUNK int = 2000
)

// Dirs

const (
	DIR_STORAGES     string = "storages"
	DIR_TRANSACTIONS string = "transactions"
	DIR_DELETE_TASKS string = "delete_tasks"
)

// Columns

const (
	C_TIMESTAMP string = "timestamp"
	C_LEVEL     string = "level"
	C_TRACES    string = "traces"
	C_ENTITY    string = "entity"
	C_ENTITY_ID string = "entity_id"
	C_MESSAGE   string = "message"
	C_MODULES   string = "modules"
	C_LABELS    string = "labels"
	C_FIELDS    string = "fields"
)

func GetLogColumns() []string {
	return []string{
		C_TIMESTAMP,
		C_LEVEL,
		C_TRACES,
		C_ENTITY,
		C_ENTITY_ID,
		C_MESSAGE,
		C_MODULES,
		C_LABELS,
		C_FIELDS,
	}
}

// Types

type ValueType byte

const (
	STR ValueType = iota
	INT
	STR_ARRAY
	INT_ARRAY
	STR_MAP
)

var _columnTypes = map[string]ValueType{
	C_TIMESTAMP: INT,
	C_LEVEL:     INT,
	C_TRACES:    STR_ARRAY,
	C_ENTITY:    STR,
	C_ENTITY_ID: STR,
	C_MESSAGE:   STR,
	C_MODULES:   STR_ARRAY,
	C_LABELS:    STR_ARRAY,
	C_FIELDS:    STR_MAP,
}

func GetColumnType(column string) (ValueType, bool) {
	t, ok := _columnTypes[column]
	return t, ok
}

// Aggregators

const (
	AG_COUNT string = "count"
	AG_AVG   string = "avg"
	AG_MAX   string = "max"
	AG_MIN   string = "min"
	AG_SUM   string = "sum"
)

var _aggrTypes = map[string]ValueType{
	AG_COUNT: INT,
	AG_AVG:   INT,
	AG_MAX:   INT,
	AG_MIN:   INT,
	AG_SUM:   INT,
}

func GetAggrType(aggrName string) (ValueType, bool) {
	t, ok := _aggrTypes[aggrName]
	return t, ok
}
