package engines

import (
	"fmt"
	"sort"
	"strings"
)

// jsonFieldFilterTypedBranches are GraphQL input field names on JSONFieldFilter (camelCase).
var jsonFieldFilterTypedBranches = []string{
	"int", "bigInt", "float", "string", "bool", "date", "time", "dateTime",
	"timestamp", "interval", "intRange", "bigIntRange", "timestampRange", "geometry",
}

// JSONFieldFilterSQL generates SQL for one JSONFieldFilter: path, optional coalesce,
// and exactly one typed sub-filter (int, string, …) whose operators are delegated to
// FilterOperationSQLValue on the appropriate engine.
func JSONFieldFilterSQL(e Engine, sqlName string, filter map[string]any, params []any) (string, []any, error) {
	pathVal, ok := filter["path"]
	if !ok {
		return "", nil, fmt.Errorf("field filter requires 'path'")
	}
	path, ok := pathVal.(string)
	if !ok || path == "" {
		return "", nil, fmt.Errorf("field filter 'path' must be a non-empty string")
	}

	coalesceVal, hasCoalesce := filter["coalesce"]

	var sqlForOps string
	pathForOps := path
	if hasCoalesce && coalesceVal != nil {
		extracted := e.FieldValueByPath(sqlName, path)
		coalesceSQLVal, err := e.SQLValue(coalesceVal)
		if err != nil {
			return "", nil, fmt.Errorf("invalid coalesce value: %w", err)
		}
		sqlForOps = fmt.Sprintf("COALESCE(%s, %s)", extracted, coalesceSQLVal)
		pathForOps = ""
	} else {
		sqlForOps = sqlName
	}

	var typedKey string
	var typedMap map[string]any
	for _, k := range jsonFieldFilterTypedBranches {
		v, ok := filter[k]
		if !ok || v == nil {
			continue
		}
		if typedKey != "" {
			return "", nil, fmt.Errorf("JSONFieldFilter: at most one typed sub-filter allowed, got both %q and %q", typedKey, k)
		}
		m, ok := v.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("JSONFieldFilter.%s must be an object", k)
		}
		typedKey = k
		typedMap = m
	}
	if typedKey == "" {
		return "", nil, fmt.Errorf("JSONFieldFilter requires exactly one typed sub-filter (int, bigInt, float, string, bool, date, time, dateTime, timestamp, interval, intRange, bigIntRange, timestampRange, geometry)")
	}

	ops := make([]string, 0, len(typedMap))
	for op := range typedMap {
		if typedMap[op] == nil {
			continue
		}
		ops = append(ops, op)
	}
	sort.Strings(ops)

	var parts []string
	for _, op := range ops {
		v := typedMap[op]
		q, p, err := e.FilterOperationSQLValue(sqlForOps, pathForOps, op, v, params)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "("+q+")")
		params = p
	}
	if len(parts) == 0 {
		return "TRUE", params, nil
	}
	if len(parts) == 1 {
		return strings.TrimPrefix(strings.TrimSuffix(parts[0], ")"), "("), params, nil
	}
	return strings.Join(parts, " AND "), params, nil
}
