package masterdata

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/linkedin/goavro/v2"
)

// ExportMasters returns the current master data as an Avro Object Container
// File (MasterSchema) encoded with the real goavro Codec. Masters are
// ordered by master identity; members of each master are ordered by source
// identity.
func (s *Store) ExportMasters() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("masterdata: store is closed")
	}
	type masterRow struct {
		master identity
		attrs  string
	}
	var rows []masterRow
	err := s.db.query(
		`SELECT master_source, master_id, attributes FROM masters ORDER BY master_source, master_id`, nil,
		func(cols []sqlVal) error {
			rows = append(rows, masterRow{
				master: identity{source: cols[0].text, id: cols[1].text},
				attrs:  cols[2].text,
			})
			return nil
		})
	if err != nil {
		return nil, err
	}
	members := make(map[identity][]interface{})
	err = s.db.query(
		`SELECT master_source, master_id, member_source, member_id FROM members
		 ORDER BY master_source, master_id, member_source, member_id`, nil,
		func(cols []sqlVal) error {
			key := identity{source: cols[0].text, id: cols[1].text}
			members[key] = append(members[key], map[string]interface{}{
				"source": cols[2].text,
				"id":     cols[3].text,
			})
			return nil
		})
	if err != nil {
		return nil, err
	}
	items := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		attrs, _, err := valueMapCodec.NativeFromTextual([]byte(row.attrs))
		if err != nil {
			return nil, fmt.Errorf("masterdata: cannot decode stored attributes of master %s/%s: %w",
				row.master.source, row.master.id, err)
		}
		memberList := members[row.master]
		if memberList == nil {
			memberList = []interface{}{}
		}
		items = append(items, map[string]interface{}{
			"master_source": row.master.source,
			"master_id":     row.master.id,
			"attributes":    attrs,
			"members":       memberList,
		})
	}
	return encodeOCF(masterCodec, items)
}

// ExportFacts returns the current facts as an Avro Object Container File
// (FactSchema) encoded with the real goavro Codec, ordered by fact id. Each
// fact carries its currently rebound entity reference (null when cleared)
// and its untouched payload.
func (s *Store) ExportFacts() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("masterdata: store is closed")
	}
	var items []interface{}
	err := s.db.query(
		`SELECT fact_id, entity_source, entity_id, payload FROM facts ORDER BY fact_id`, nil,
		func(cols []sqlVal) error {
			payload, _, err := valueMapCodec.NativeFromTextual([]byte(cols[3].text))
			if err != nil {
				return fmt.Errorf("masterdata: cannot decode stored payload of fact %q: %w", cols[0].text, err)
			}
			items = append(items, map[string]interface{}{
				"fact_id":       cols[0].text,
				"entity_source": optionalUnionString(cols[1]),
				"entity_id":     optionalUnionString(cols[2]),
				"payload":       payload,
			})
			return nil
		})
	if err != nil {
		return nil, err
	}
	return encodeOCF(factCodec, items)
}

// optionalUnionString converts a nullable TEXT column into the native form
// of a ["null","string"] union.
func optionalUnionString(v sqlVal) interface{} {
	if v.null {
		return nil
	}
	return map[string]interface{}{"string": v.text}
}

// encodeOCF encodes items as an Avro Object Container File with codec.
func encodeOCF(codec *goavro.Codec, items []interface{}) ([]byte, error) {
	var buf bytes.Buffer
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{W: &buf, Codec: codec})
	if err != nil {
		return nil, fmt.Errorf("masterdata: cannot create OCF writer: %w", err)
	}
	if len(items) > 0 {
		if err := w.Append(items); err != nil {
			return nil, fmt.Errorf("masterdata: cannot encode export: %w", err)
		}
	}
	return buf.Bytes(), nil
}
