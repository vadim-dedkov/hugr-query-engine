package engines

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// ArrowIngestStagingBuilder owns every SQL expression evaluated by DuckDB while
// an Arrow reader is registered as a view. Target-specific conversion, when a
// target needs one, is applied separately through EngineIngestValueAdapter.
type ArrowIngestStagingBuilder struct {
	duckdb DuckDB
}

func NewArrowIngestStagingBuilder() *ArrowIngestStagingBuilder {
	return &ArrowIngestStagingBuilder{}
}

func (b *ArrowIngestStagingBuilder) SQLValue(v any) (string, error) {
	return b.duckdb.SQLValue(v)
}

func (b *ArrowIngestStagingBuilder) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
	return b.duckdb.FunctionCall(name, positional, named)
}

// SelectExpr converts an Arrow-view column to its canonical DuckDB staging
// representation for the target GraphQL field.
//
// sourceExpr is the SQL expression that reads the Arrow column from the
// temporary Arrow view. In the common case it is a quoted column identifier:
// for Arrow field geom_geojson, sourceExpr is "geom_geojson", and a Geometry
// target may return ST_GeomFromGeoJSON("geom_geojson").
func (b *ArrowIngestStagingBuilder) SelectExpr(field *ast.Field, arrowField arrow.Field, sourceExpr string) (string, error) {
	if field == nil || field.Definition == nil {
		return sourceExpr, nil
	}
	switch field.Definition.Type.Name() {
	case base.JSONTypeName:
		return arrowIngestJSONStagingExpr(arrowField, sourceExpr)
	case base.GeometryTypeName:
		return arrowIngestGeometryStagingExpr(arrowField, sourceExpr)
	default:
		return sourceExpr, nil
	}
}

// LiteralExpr converts a non-Arrow value, such as permission data, to a
// canonical DuckDB staging expression.
func (b *ArrowIngestStagingBuilder) LiteralExpr(field *ast.Field, value any) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	if field != nil && field.Definition != nil && field.Definition.Type.Name() == base.GeometryTypeName {
		geom, err := ctypes.ParseGeometryValue(value)
		if err != nil {
			return "", err
		}
		if geom == nil {
			return "NULL", nil
		}
		wkbValue, err := ctypes.GeometryToSQLValue(geom)
		if err != nil {
			return "", err
		}
		return "ST_GeomFromWKB(from_hex('" + strings.ToUpper(hex.EncodeToString(wkbValue)) + "'))", nil
	}
	return b.duckdb.SQLValue(value)
}

func arrowIngestJSONStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	ext := arrowExtensionNameFromTypeOrMetadata(arrowField)
	return arrowIngestJSONStagingExprFromExtension(ext, arrowField, sourceExpr)
}

func arrowIngestJSONStagingExprFromExtension(ext string, arrowField arrow.Field, sourceExpr string) (string, error) {
	switch ext {
	case "":
		return jsonExprFromPlainArrow(arrowField, sourceExpr), nil
	case "arrow.json":
		return jsonExprFromArrowJSONExtension(arrowField, sourceExpr)
	case "hugr.geojson", "geoarrow.geojson", "geojson":
		return jsonExprFromGeoJSONExtension(arrowField, sourceExpr)
	default:
		return "", fmt.Errorf("unsupported Arrow extension %q for JSON ingest", ext)
	}
}

func jsonExprFromPlainArrow(arrowField arrow.Field, sourceExpr string) string {
	if expr, ok := jsonExprFromSerializedStorage(arrowField, sourceExpr); ok {
		return expr
	}
	return duckDBToJSON(sourceExpr)
}

func jsonExprFromArrowJSONExtension(arrowField arrow.Field, sourceExpr string) (string, error) {
	// arrow.json is a canonical Arrow extension: its storage must be UTF-8 text.
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "CAST(" + sourceExpr + " AS JSON)", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot use arrow.json storage", arrowField.Name, arrowField.Type)
	}
}

func jsonExprFromGeoJSONExtension(arrowField arrow.Field, sourceExpr string) (string, error) {
	// GeoJSON aliases are Hugr-supported encodings; they may arrive as text,
	// bytes, or a JSON object-like Arrow value.
	if expr, ok := jsonExprFromSerializedStorage(arrowField, sourceExpr); ok {
		return expr, nil
	}
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.STRUCT, arrow.MAP:
		return duckDBToJSON(sourceExpr), nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot use GeoJSON storage", arrowField.Name, arrowField.Type)
	}
}

func jsonExprFromSerializedStorage(arrowField arrow.Field, sourceExpr string) (string, bool) {
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "CAST(" + sourceExpr + " AS JSON)", true
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return "CAST(decode(" + sourceExpr + ") AS JSON)", true
	default:
		return "", false
	}
}

func duckDBToJSON(sql string) string {
	return "to_json(" + sql + ")"
}

func arrowIngestGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	ext := arrowExtensionNameFromTypeOrMetadata(arrowField)
	return arrowIngestGeometryStagingExprFromExtension(ext, arrowField, sourceExpr)
}

// arrowIngestGeometryStagingExprFromExtension uses GeoArrow/Hugr extension
// metadata as the source of truth for geometry semantics. Type-specific helpers
// below only decode the Arrow storage for that known semantic type; unsupported
// metadata never falls back to guessing from Type.ID().
func arrowIngestGeometryStagingExprFromExtension(ext string, arrowField arrow.Field, sourceExpr string) (string, error) {
	switch ext {
	case "":
		return arrowIngestGeometryStagingExprFromPhysicalType(arrowField, sourceExpr)
	case "geoarrow.wkb":
		return arrowIngestWKBGeometryStagingExpr(arrowField, sourceExpr)
	case "hugr.hexwkb", "geoarrow.hexwkb", "hexwkb":
		return arrowIngestHexWKBGeometryStagingExpr(arrowField, sourceExpr)
	case "geoarrow.wkt":
		return arrowIngestWKTGeometryStagingExpr(arrowField, sourceExpr)
	case "hugr.geojson", "geoarrow.geojson", "geojson":
		return arrowIngestGeoJSONGeometryStagingExpr(arrowField, sourceExpr)
	case "geoarrow.linestring", "geoarrow.polygon",
		"geoarrow.multipoint", "geoarrow.multilinestring", "geoarrow.multipolygon",
		"geoarrow.point", "geoarrow.geometry", "geoarrow.geometrycollection":
		return arrowIngestGeoArrowNativeGeometryStagingExpr(ext, arrowField, sourceExpr)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}

func arrowIngestWKBGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return sourceExpr, nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot use geoarrow.wkb storage", arrowField.Name, arrowField.Type)
	}
}

func arrowIngestHexWKBGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "ST_GeomFromWKB(from_hex(" + sourceExpr + "))", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot use hexwkb storage", arrowField.Name, arrowField.Type)
	}
}

func arrowIngestWKTGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "ST_GeomFromText(" + sourceExpr + ", true)", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot use geoarrow.wkt storage", arrowField.Name, arrowField.Type)
	}
}

func arrowIngestGeoJSONGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowStorageTypeID(arrowField.Type) {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "ST_GeomFromGeoJSON(" + sourceExpr + ")", nil
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return "ST_GeomFromGeoJSON(CAST(decode(" + sourceExpr + ") AS VARCHAR))", nil
	case arrow.STRUCT, arrow.MAP:
		return "ST_GeomFromGeoJSON(to_json(" + sourceExpr + ")::VARCHAR)", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot use GeoJSON storage", arrowField.Name, arrowField.Type)
	}
}

// arrowIngestGeometryStagingExprFromPhysicalType is the best-effort path for
// unannotated Arrow columns. Without extension metadata we infer common
// geometry encodings from physical Arrow storage.
func arrowIngestGeometryStagingExprFromPhysicalType(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowField.Type.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return "ST_GeomFromWKB(" + sourceExpr + ")", nil
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "ST_GeomFromText(" + sourceExpr + ", true)", nil
	case arrow.STRUCT, arrow.MAP:
		return "ST_GeomFromGeoJSON(to_json(" + sourceExpr + ")::VARCHAR)", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot be ingested as Geometry without geoarrow/hugr metadata", arrowField.Name, arrowField.Type)
	}
}

func arrowExtensionNameFromTypeOrMetadata(field arrow.Field) string {
	if extType, ok := field.Type.(arrow.ExtensionType); ok {
		return strings.ToLower(extType.ExtensionName())
	}
	if ext, ok := field.Metadata.GetValue("ARROW:extension:name"); ok {
		return strings.ToLower(ext)
	}
	if ext, ok := field.Metadata.GetValue("extension:name"); ok {
		return strings.ToLower(ext)
	}
	return ""
}

func arrowStorageTypeID(dt arrow.DataType) arrow.Type {
	return arrowStorageType(dt).ID()
}

func arrowStorageType(dt arrow.DataType) arrow.DataType {
	if extType, ok := dt.(arrow.ExtensionType); ok {
		return extType.StorageType()
	}
	return dt
}

func arrowIngestGeoArrowPointGeometryStagingExpr(coordType arrow.DataType, sql string) (string, error) {
	switch typ := arrowStorageType(coordType).(type) {
	case *arrow.StructType:
		if err := validateGeoArrowStructCoordinate(typ); err != nil {
			return "", err
		}
		return "ST_Point(struct_extract(" + sql + ", 'x'), struct_extract(" + sql + ", 'y'))", nil
	case *arrow.FixedSizeListType:
		if err := validateGeoArrowFixedSizeListCoordinate(typ); err != nil {
			return "", err
		}
		return "ST_Point(" + sql + "[1], " + sql + "[2])", nil
	default:
		return "", fmt.Errorf("unsupported GeoArrow coordinate storage %s", coordType)
	}
}

func validateGeoArrowStructCoordinate(typ *arrow.StructType) error {
	x, ok := typ.FieldByName("x")
	if !ok {
		return fmt.Errorf("GeoArrow struct coordinate is missing x field")
	}
	y, ok := typ.FieldByName("y")
	if !ok {
		return fmt.Errorf("GeoArrow struct coordinate is missing y field")
	}
	if !isArrowFloatingPointType(x.Type) || !isArrowFloatingPointType(y.Type) {
		return fmt.Errorf("GeoArrow struct coordinate x/y fields must be floating-point values")
	}
	return nil
}

func validateGeoArrowFixedSizeListCoordinate(typ *arrow.FixedSizeListType) error {
	if typ.Len() < 2 {
		return fmt.Errorf("GeoArrow fixed-size-list coordinate must have at least two values")
	}
	if !isArrowFloatingPointType(typ.Elem()) {
		return fmt.Errorf("GeoArrow fixed-size-list coordinate values must be floating-point values")
	}
	return nil
}

func isArrowFloatingPointType(dt arrow.DataType) bool {
	switch arrowStorageTypeID(dt) {
	case arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func arrowListElemType(dt arrow.DataType) (arrow.DataType, bool) {
	switch typ := arrowStorageType(dt).(type) {
	case *arrow.ListType:
		return typ.Elem(), true
	case *arrow.LargeListType:
		return typ.Elem(), true
	case *arrow.ListViewType:
		return typ.Elem(), true
	case *arrow.LargeListViewType:
		return typ.Elem(), true
	default:
		return nil, false
	}
}

func arrowIngestGeoArrowLineStringGeometryStagingExpr(lineStringType arrow.DataType, sql string) (string, error) {
	coordType, ok := arrowListElemType(lineStringType)
	if !ok {
		return "", fmt.Errorf("GeoArrow linestring storage must be a list of coordinates, got %s", lineStringType)
	}
	pointExpr, err := arrowIngestGeoArrowPointGeometryStagingExpr(coordType, "_p")
	if err != nil {
		return "", err
	}
	return "ST_MakeLine(list_transform(" + sql + ", lambda _p: " + pointExpr + "))", nil
}

func arrowIngestGeoArrowPolygonGeometryStagingExpr(polygonType arrow.DataType, sql string) (string, error) {
	ringType, ok := arrowListElemType(polygonType)
	if !ok {
		return "", fmt.Errorf("GeoArrow polygon storage must be a list of rings, got %s", polygonType)
	}
	shell, err := arrowIngestGeoArrowLineStringGeometryStagingExpr(ringType, sql+"[1]")
	if err != nil {
		return "", err
	}
	holeExpr, err := arrowIngestGeoArrowLineStringGeometryStagingExpr(ringType, "_r")
	if err != nil {
		return "", err
	}
	holes := "list_transform(" + sql + "[2:], lambda _r: " + holeExpr + ")"
	return "ST_MakePolygon(" + shell + ", " + holes + ")", nil
}

func arrowIngestGeoArrowMultiPointGeometryStagingExpr(multiPointType arrow.DataType, sql string) (string, error) {
	coordType, ok := arrowListElemType(multiPointType)
	if !ok {
		return "", fmt.Errorf("GeoArrow multipoint storage must be a list of coordinates, got %s", multiPointType)
	}
	pointExpr, err := arrowIngestGeoArrowPointGeometryStagingExpr(coordType, "_p")
	if err != nil {
		return "", err
	}
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _p: " + pointExpr + ")))", nil
}

func arrowIngestGeoArrowMultiLineStringGeometryStagingExpr(multiLineStringType arrow.DataType, sql string) (string, error) {
	lineStringType, ok := arrowListElemType(multiLineStringType)
	if !ok {
		return "", fmt.Errorf("GeoArrow multilinestring storage must be a list of linestrings, got %s", multiLineStringType)
	}
	lineStringExpr, err := arrowIngestGeoArrowLineStringGeometryStagingExpr(lineStringType, "_ls")
	if err != nil {
		return "", err
	}
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _ls: " + lineStringExpr + ")))", nil
}

func arrowIngestGeoArrowMultiPolygonGeometryStagingExpr(multiPolygonType arrow.DataType, sql string) (string, error) {
	polygonType, ok := arrowListElemType(multiPolygonType)
	if !ok {
		return "", fmt.Errorf("GeoArrow multipolygon storage must be a list of polygons, got %s", multiPolygonType)
	}
	polygonExpr, err := arrowIngestGeoArrowPolygonGeometryStagingExpr(polygonType, "_poly")
	if err != nil {
		return "", err
	}
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _poly: " + polygonExpr + ")))", nil
}

func arrowIngestGeoArrowNativeGeometryStagingExpr(ext string, arrowField arrow.Field, sql string) (string, error) {
	storageType := arrowStorageType(arrowField.Type)
	switch ext {
	case "geoarrow.point":
		return arrowIngestGeoArrowPointGeometryStagingExpr(storageType, sql)
	case "geoarrow.linestring":
		return arrowIngestGeoArrowLineStringGeometryStagingExpr(storageType, sql)
	case "geoarrow.polygon":
		return arrowIngestGeoArrowPolygonGeometryStagingExpr(storageType, sql)
	case "geoarrow.multipoint":
		return arrowIngestGeoArrowMultiPointGeometryStagingExpr(storageType, sql)
	case "geoarrow.multilinestring":
		return arrowIngestGeoArrowMultiLineStringGeometryStagingExpr(storageType, sql)
	case "geoarrow.multipolygon":
		return arrowIngestGeoArrowMultiPolygonGeometryStagingExpr(storageType, sql)
	case "geoarrow.geometry", "geoarrow.geometrycollection":
		return "", fmt.Errorf("%s ingest is not supported from native union storage; send geoarrow.wkb, geoarrow.wkt, geoarrow.geojson, or a concrete GeoArrow coordinate layout", ext)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}
