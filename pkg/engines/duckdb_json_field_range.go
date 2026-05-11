package engines

import (
	"fmt"

	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
)

func duckdbJSONFieldRangeFilter(lhsSQL, op string, value any, params []any) (string, []any, error) {
	switch op {
	case "eq":
		s, err := ctypes.RangeToBracketLiteral(value)
		if err != nil {
			return "", nil, err
		}
		params = append(params, s)
		n := len(params)
		return fmt.Sprintf("%s = $%d", lhsSQL, n), params, nil
	case "intersects":
		return duckdbJSONIntRangeOverlap(lhsSQL, value, false, params)
	case "includes":
		return duckdbJSONIntRangeOverlap(lhsSQL, value, true, params)
	default:
		return "", nil, fmt.Errorf("unsupported JSON field range operator for DuckDB: %s", op)
	}
}

func duckdbJSONIntRangeOverlap(lhsSQL string, value any, includes bool, params []any) (string, []any, error) {
	var rl, ru int64
	switch v := value.(type) {
	case ctypes.Int32Range:
		if v.Detail.IsEmpty() || v.Detail.IsLowerInfinity() || v.Detail.IsUpperInfinity() ||
			!v.Detail.IsLowerInclusive() || v.Detail.IsUpperInclusive() {
			return "", nil, fmt.Errorf("duckdb JSON field intRange: only bounded [a,b) half-open ranges are supported")
		}
		rl, ru = int64(v.Lower), int64(v.Upper)
	case ctypes.Int64Range:
		if v.Detail.IsEmpty() || v.Detail.IsLowerInfinity() || v.Detail.IsUpperInfinity() ||
			!v.Detail.IsLowerInclusive() || v.Detail.IsUpperInclusive() {
			return "", nil, fmt.Errorf("duckdb JSON field bigIntRange: only bounded [a,b) half-open ranges are supported")
		}
		rl, ru = v.Lower, v.Upper
	default:
		return "", nil, fmt.Errorf("duckdb JSON field range intersects/includes expects Int32Range or Int64Range, got %T", value)
	}

	loL := fmt.Sprintf("CAST(regexp_extract(%s, '^[\\[(](-?\\d+)', 1) AS BIGINT)", lhsSQL)
	hiL := fmt.Sprintf("CAST(regexp_extract(%s, ',(-?\\d+)[\\])]', 1) AS BIGINT)", lhsSQL)

	if includes {
		params = append(params, rl, ru)
		n := len(params)
		return fmt.Sprintf("((%s) <= $%d AND (%s) >= $%d)", loL, n-1, hiL, n), params, nil
	}
	params = append(params, ru, rl)
	n := len(params)
	return fmt.Sprintf("((%s) < $%d AND (%s) > $%d)", loL, n-1, hiL, n), params, nil
}
