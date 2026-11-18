package masterdata

import "github.com/linkedin/goavro/v2"

// ValueUnionSchema is the Avro schema of a single attribute or payload
// value. An Avro null marks a missing value; 0, false and "" are kept
// as-is.
const ValueUnionSchema = `["null","string","long","double","boolean"]`

// mapOfValuesSchema is the Avro schema of an attribute or payload map.
const mapOfValuesSchema = `{"type":"map","values":` + ValueUnionSchema + `}`

// BatchSchema is the Avro schema of the batches accepted by
// Store.ApplyBatch. A batch carries three operation lists, applied in this
// order inside one transaction:
//
//   - records: UPSERT replaces the whole attribute set of a source record
//     (a correction); DELETE removes the record and clears its links.
//   - links: LINK confirms an undirected duplicate relation between two
//     existing source records; UNLINK removes it (which may split a group).
//   - facts: UPSERT stores a fact with its original entity reference;
//     DELETE removes a fact.
const BatchSchema = `{
  "type": "record",
  "name": "Batch",
  "fields": [
    {"name": "records", "type": {"type": "array", "items": {
      "type": "record",
      "name": "RecordOp",
      "fields": [
        {"name": "op", "type": {"type": "enum", "name": "RecordOpKind", "symbols": ["UPSERT", "DELETE"]}},
        {"name": "source", "type": "string"},
        {"name": "id", "type": "string"},
        {"name": "attributes", "type": {"type": "map", "values": ` + ValueUnionSchema + `}}
      ]}}},
    {"name": "links", "type": {"type": "array", "items": {
      "type": "record",
      "name": "LinkOp",
      "fields": [
        {"name": "op", "type": {"type": "enum", "name": "LinkOpKind", "symbols": ["LINK", "UNLINK"]}},
        {"name": "a_source", "type": "string"},
        {"name": "a_id", "type": "string"},
        {"name": "b_source", "type": "string"},
        {"name": "b_id", "type": "string"}
      ]}}},
    {"name": "facts", "type": {"type": "array", "items": {
      "type": "record",
      "name": "FactOp",
      "fields": [
        {"name": "op", "type": {"type": "enum", "name": "FactOpKind", "symbols": ["UPSERT", "DELETE"]}},
        {"name": "fact_id", "type": "string"},
        {"name": "entity_source", "type": ["null", "string"]},
        {"name": "entity_id", "type": ["null", "string"]},
        {"name": "payload", "type": {"type": "map", "values": ` + ValueUnionSchema + `}}
      ]}}}
  ]
}`

// MasterSchema is the Avro schema of the records inside the OCF returned by
// Store.ExportMasters.
const MasterSchema = `{
  "type": "record",
  "name": "Master",
  "fields": [
    {"name": "master_source", "type": "string"},
    {"name": "master_id", "type": "string"},
    {"name": "attributes", "type": {"type": "map", "values": ` + ValueUnionSchema + `}},
    {"name": "members", "type": {"type": "array", "items": {
      "type": "record",
      "name": "Member",
      "fields": [
        {"name": "source", "type": "string"},
        {"name": "id", "type": "string"}
      ]}}}
  ]
}`

// FactSchema is the Avro schema of the records inside the OCF returned by
// Store.ExportFacts. entity_source/entity_id are the currently rebound
// master reference, or null when the original source record is gone (or was
// never set).
const FactSchema = `{
  "type": "record",
  "name": "FactView",
  "fields": [
    {"name": "fact_id", "type": "string"},
    {"name": "entity_source", "type": ["null", "string"]},
    {"name": "entity_id", "type": ["null", "string"]},
    {"name": "payload", "type": {"type": "map", "values": ` + ValueUnionSchema + `}}
  ]
}`

var (
	batchCodec    = mustCodec(BatchSchema)
	masterCodec   = mustCodec(MasterSchema)
	factCodec     = mustCodec(FactSchema)
	valueMapCodec = mustCodec(mapOfValuesSchema)
)

func mustCodec(schema string) *goavro.Codec {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		panic(err)
	}
	return codec
}
