package engines

import (
	"testing"
)

func TestJsonFieldFilterSQL_Postgres(t *testing.T) {
	e := &Postgres{}
	tests := []struct {
		name     string
		sqlName  string
		filter   map[string]any
		expected string
		nParams  int
		wantErr  bool
	}{
		{
			name:     "gt with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "gkh_kapremont_2026.ko", "int": map[string]any{"gt": 0}},
			expected: "COALESCE(attr @@ '$.gkh_kapremont_2026.ko > 0', false)",
			nParams:  0,
		},
		{
			name:     "eq with path and string value",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.name", "string": map[string]any{"eq": "test"}},
			expected: `COALESCE(attr @@ '$.catalog.name == "test"', false)`,
			nParams:  0,
		},
		{
			name:     "gte with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.field", "int": map[string]any{"gte": 10}},
			expected: "COALESCE(attr @@ '$.catalog.field >= 10', false)",
			nParams:  0,
		},
		{
			name:     "lt with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "x.y", "int": map[string]any{"lt": 5}},
			expected: "COALESCE(attr @@ '$.x.y < 5', false)",
			nParams:  0,
		},
		{
			name:     "lte with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "x.y", "int": map[string]any{"lte": 100}},
			expected: "COALESCE(attr @@ '$.x.y <= 100', false)",
			nParams:  0,
		},
		{
			name:     "is_null with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "int": map[string]any{"is_null": true}},
			expected: "attr->'catalog'->'ko' IS NULL",
			nParams:  0,
		},
		{
			name:     "is_not_null with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "int": map[string]any{"is_null": false}},
			expected: "attr->'catalog'->'ko' IS NOT NULL",
			nParams:  0,
		},
		{
			name:     "like with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.address", "string": map[string]any{"like": "%school%"}},
			expected: "attr->'catalog'->>'address' LIKE $1",
			nParams:  1,
		},
		{
			name:     "ilike with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.name", "string": map[string]any{"ilike": "%test%"}},
			expected: "attr->'catalog'->>'name' ILIKE $1",
			nParams:  1,
		},
		{
			name:     "regex with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.code", "string": map[string]any{"regex": `^MKD-\d+`}},
			expected: `COALESCE(attr @@ '$.catalog.code like_regex "^MKD-\d+"', false)`,
			nParams:  0,
		},
		{
			name:     "in with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.fkr_map", "int": map[string]any{"in": []any{2, 3}}},
			expected: "attr->'catalog'->'fkr_map' = ANY($1)",
			nParams:  1,
		},
		{
			name:     "coalesce + gt",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "coalesce": 0, "int": map[string]any{"gt": 0}},
			expected: "COALESCE(attr->'catalog'->'ko', 0) > $1",
			nParams:  1,
		},
		{
			name:     "coalesce + eq",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "coalesce": 0, "int": map[string]any{"eq": 0}},
			expected: "COALESCE(attr->'catalog'->'ko', 0) = $1",
			nParams:  1,
		},
		{
			name:     "coalesce + in",
			sqlName:  "attr",
			filter: map[string]any{
				"path": "catalog.fkr_map",
				"coalesce": 0,
				"int":      map[string]any{"in": []any{2, 3}},
			},
			expected: `COALESCE(attr->'catalog'->'fkr_map', 0) = ANY($1)`,
			nParams:  1,
		},
		{
			name:    "missing path",
			sqlName: "attr",
			filter:  map[string]any{"int": map[string]any{"gt": 0}},
			wantErr: true,
		},
		{
			name:    "empty path",
			sqlName: "attr",
			filter:  map[string]any{"path": "", "int": map[string]any{"gt": 0}},
			wantErr: true,
		},
		{
			name: "two typed sub-filters",
			sqlName: "attr",
			filter: map[string]any{
				"path":   "x",
				"int":    map[string]any{"gt": 0},
				"string": map[string]any{"eq": "a"},
			},
			wantErr: true,
		},
		{
			name:    "no typed sub-filter",
			sqlName: "attr",
			filter:  map[string]any{"path": "x.y"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, params, err := JSONFieldFilterSQL(e, tt.sqlName, tt.filter, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("expected error: %v, got: %v (err: %v)", tt.wantErr, err != nil, err)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.expected != "" && result != tt.expected {
				t.Errorf("expected:\n  %s\ngot:\n  %s", tt.expected, result)
			}
			if len(params) != tt.nParams {
				t.Errorf("expected %d params, got %d: %v", tt.nParams, len(params), params)
			}
		})
	}
}

func TestJsonFieldFilterSQL_DuckDB(t *testing.T) {
	e := &DuckDB{}
	tests := []struct {
		name     string
		sqlName  string
		filter   map[string]any
		expected string
		nParams  int
		wantErr  bool
	}{
		{
			name:     "gt with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "int": map[string]any{"gt": 0}},
			expected: "attr['catalog']['ko'] > $1",
			nParams:  1,
		},
		{
			name:     "eq with path and int",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.fkr_map", "int": map[string]any{"eq": 1}},
			expected: "attr['catalog']['fkr_map'] = $1",
			nParams:  1,
		},
		{
			name:     "is_null with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "int": map[string]any{"is_null": true}},
			expected: "attr['catalog']['ko'] IS NULL",
			nParams:  0,
		},
		{
			name:     "like with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.name", "string": map[string]any{"like": "%test%"}},
			expected: "attr['catalog']['name'] LIKE $1",
			nParams:  1,
		},
		{
			name:     "ilike with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.name", "string": map[string]any{"ilike": "%test%"}},
			expected: "attr['catalog']['name'] ILIKE $1",
			nParams:  1,
		},
		{
			name:     "regex with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.code", "string": map[string]any{"regex": "^MKD"}},
			expected: "regexp_matches(attr['catalog']['code'],$1)",
			nParams:  1,
		},
		{
			name:     "in with path",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.fkr_map", "int": map[string]any{"in": []any{2, 3}}},
			expected: "attr['catalog']['fkr_map'] IN (SELECT unnest($1))",
			nParams:  1,
		},
		{
			name:     "coalesce + gt",
			sqlName:  "attr",
			filter:   map[string]any{"path": "catalog.ko", "coalesce": 0, "int": map[string]any{"gt": 0}},
			expected: "COALESCE(attr['catalog']['ko'], 0) > $1",
			nParams:  1,
		},
		{
			name: "coalesce + in",
			sqlName: "attr",
			filter: map[string]any{
				"path": "catalog.fkr_map", "coalesce": 0,
				"int": map[string]any{"in": []any{2, 3}},
			},
			expected: "COALESCE(attr['catalog']['fkr_map'], 0) IN (SELECT unnest($1))",
			nParams:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, params, err := JSONFieldFilterSQL(e, tt.sqlName, tt.filter, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("expected error: %v, got: %v (err: %v)", tt.wantErr, err != nil, err)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.expected != "" && result != tt.expected {
				t.Errorf("expected:\n  %s\ngot:\n  %s", tt.expected, result)
			}
			if len(params) != tt.nParams {
				t.Errorf("expected %d params, got %d: %v", tt.nParams, len(params), params)
			}
		})
	}
}
