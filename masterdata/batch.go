package masterdata

import "fmt"

// recordOp is a decoded correction (UPSERT) or deletion (DELETE) of one
// source record.
type recordOp struct {
	op    string
	who   identity
	attrs map[string]interface{}
}

// linkOp is a decoded confirmation (LINK) or removal (UNLINK) of an
// undirected duplicate relation between two source records.
type linkOp struct {
	op string
	a  identity
	b  identity
}

// factOp is a decoded upsert (UPSERT) or removal (DELETE) of one fact. The
// entity is the fact's original source identity; it may be nil.
type factOp struct {
	op      string
	factID  string
	entity  *identity
	payload map[string]interface{}
}

// batchOps is a decoded batch, already converted from goavro native values
// into typed operations.
type batchOps struct {
	records []recordOp
	links   []linkOp
	facts   []factOp
}

// parseBatch converts the native value decoded from BatchSchema into typed
// operations. The codec guarantees the shapes, so any mismatch here means
// the input was not encoded with BatchSchema.
func parseBatch(native interface{}) (*batchOps, error) {
	root, err := asRecord(native, "batch")
	if err != nil {
		return nil, err
	}
	ops := new(batchOps)

	records, err := asArray(root["records"], "batch.records")
	if err != nil {
		return nil, err
	}
	for i, item := range records {
		what := fmt.Sprintf("batch.records[%d]", i)
		m, err := asRecord(item, what)
		if err != nil {
			return nil, err
		}
		op, err := asString(m["op"], what+".op")
		if err != nil {
			return nil, err
		}
		if op != "UPSERT" && op != "DELETE" {
			return nil, fmt.Errorf("masterdata: %s.op is %q, want UPSERT or DELETE", what, op)
		}
		source, err := asString(m["source"], what+".source")
		if err != nil {
			return nil, err
		}
		id, err := asString(m["id"], what+".id")
		if err != nil {
			return nil, err
		}
		attrs, err := asValueMap(m["attributes"], what+".attributes")
		if err != nil {
			return nil, err
		}
		ops.records = append(ops.records, recordOp{op: op, who: identity{source: source, id: id}, attrs: attrs})
	}

	links, err := asArray(root["links"], "batch.links")
	if err != nil {
		return nil, err
	}
	for i, item := range links {
		what := fmt.Sprintf("batch.links[%d]", i)
		m, err := asRecord(item, what)
		if err != nil {
			return nil, err
		}
		op, err := asString(m["op"], what+".op")
		if err != nil {
			return nil, err
		}
		if op != "LINK" && op != "UNLINK" {
			return nil, fmt.Errorf("masterdata: %s.op is %q, want LINK or UNLINK", what, op)
		}
		a, err := parseEndpoint(m, what, "a")
		if err != nil {
			return nil, err
		}
		b, err := parseEndpoint(m, what, "b")
		if err != nil {
			return nil, err
		}
		ops.links = append(ops.links, linkOp{op: op, a: a, b: b})
	}

	facts, err := asArray(root["facts"], "batch.facts")
	if err != nil {
		return nil, err
	}
	for i, item := range facts {
		what := fmt.Sprintf("batch.facts[%d]", i)
		m, err := asRecord(item, what)
		if err != nil {
			return nil, err
		}
		op, err := asString(m["op"], what+".op")
		if err != nil {
			return nil, err
		}
		if op != "UPSERT" && op != "DELETE" {
			return nil, fmt.Errorf("masterdata: %s.op is %q, want UPSERT or DELETE", what, op)
		}
		factID, err := asString(m["fact_id"], what+".fact_id")
		if err != nil {
			return nil, err
		}
		source, sourceOK, err := asOptionalString(m["entity_source"], what+".entity_source")
		if err != nil {
			return nil, err
		}
		id, idOK, err := asOptionalString(m["entity_id"], what+".entity_id")
		if err != nil {
			return nil, err
		}
		if sourceOK != idOK {
			return nil, fmt.Errorf("masterdata: %s: entity_source and entity_id must both be null or both be set", what)
		}
		var entity *identity
		if sourceOK {
			entity = &identity{source: source, id: id}
		}
		payload, err := asValueMap(m["payload"], what+".payload")
		if err != nil {
			return nil, err
		}
		ops.facts = append(ops.facts, factOp{op: op, factID: factID, entity: entity, payload: payload})
	}

	return ops, nil
}

func parseEndpoint(m map[string]interface{}, what, prefix string) (identity, error) {
	source, err := asString(m[prefix+"_source"], what+"."+prefix+"_source")
	if err != nil {
		return identity{}, err
	}
	id, err := asString(m[prefix+"_id"], what+"."+prefix+"_id")
	if err != nil {
		return identity{}, err
	}
	return identity{source: source, id: id}, nil
}

func asRecord(v interface{}, what string) (map[string]interface{}, error) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("masterdata: %s is %T, want Avro record", what, v)
	}
	return m, nil
}

func asArray(v interface{}, what string) ([]interface{}, error) {
	a, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("masterdata: %s is %T, want Avro array", what, v)
	}
	return a, nil
}

func asString(v interface{}, what string) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("masterdata: %s is %T, want Avro string", what, v)
	}
	return s, nil
}

func asValueMap(v interface{}, what string) (map[string]interface{}, error) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("masterdata: %s is %T, want Avro map", what, v)
	}
	return m, nil
}

// asOptionalString decodes a ["null","string"] union value: nil becomes
// ("", false), a string union member becomes (s, true).
func asOptionalString(v interface{}, what string) (string, bool, error) {
	if v == nil {
		return "", false, nil
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return "", false, fmt.Errorf("masterdata: %s is %T, want null or string union", what, v)
	}
	s, ok := m["string"].(string)
	if !ok {
		return "", false, fmt.Errorf("masterdata: %s is %v, want a string union member", what, v)
	}
	return s, true, nil
}
