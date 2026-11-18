package masterdata

import (
	"errors"
	"fmt"
	"sync"
)

// Config configures a Store.
type Config struct {
	// SourcePriority lists source names from most to least preferred. When
	// resolving a master attribute, the value from the most preferred
	// source that has a non-NULL value wins. Sources not listed sort after
	// all listed ones, ties broken by source name and record id.
	SourcePriority []string
}

// Store is a customer master data store backed by a SQLite database file.
// It is safe for concurrent use.
type Store struct {
	mu     sync.Mutex
	db     *sqlDB
	rank   map[string]int
	closed bool
}

// Open creates the store at path (or reopens an existing one, continuing
// where the previous run left off) and ensures its schema exists.
func Open(path string, cfg *Config) (*Store, error) {
	rank := make(map[string]int)
	if cfg != nil {
		for i, source := range cfg.SourcePriority {
			if source == "" {
				return nil, errors.New("masterdata: SourcePriority contains an empty source name")
			}
			if _, dup := rank[source]; dup {
				return nil, fmt.Errorf("masterdata: SourcePriority lists source %q twice", source)
			}
			rank[source] = i
		}
	}
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}
	if err := db.initSchema(); err != nil {
		_ = db.close()
		return nil, err
	}
	return &Store{db: db, rank: rank}, nil
}

// Close closes the underlying SQLite database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("masterdata: store is already closed")
	}
	s.closed = true
	return s.db.close()
}

// rankOf returns the priority rank of a source: lower is more preferred.
// Unlisted sources sort after all listed ones.
func (s *Store) rankOf(source string) int {
	if r, ok := s.rank[source]; ok {
		return r
	}
	return len(s.rank)
}

// ApplyBatch decodes one Avro-binary batch (BatchSchema) and applies it:
// record corrections/deletions, duplicate link confirmations/removals and
// fact updates take effect together with the recomputed master data and the
// rebound fact references, inside a single SQLite transaction. If anything
// fails, the whole batch is rolled back and the previous state is kept.
func (s *Store) ApplyBatch(batch []byte) error {
	native, rest, err := batchCodec.NativeFromBinary(batch)
	if err != nil {
		return fmt.Errorf("masterdata: cannot decode batch: %w", err)
	}
	if len(rest) != 0 {
		return fmt.Errorf("masterdata: %d undecoded bytes after batch", len(rest))
	}
	ops, err := parseBatch(native)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("masterdata: store is closed")
	}
	if err := s.db.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if err := s.apply(ops); err != nil {
		_ = s.db.exec("ROLLBACK")
		return err
	}
	if err := s.db.exec("COMMIT"); err != nil {
		_ = s.db.exec("ROLLBACK")
		return err
	}
	return nil
}

// apply runs the batch inside the open transaction: first record ops, then
// link ops, then fact ops, then the master recomputation and the fact
// rebinding.
func (s *Store) apply(ops *batchOps) error {
	for i, r := range ops.records {
		if err := s.applyRecordOp(r); err != nil {
			return fmt.Errorf("masterdata: records[%d]: %w", i, err)
		}
	}
	for i, l := range ops.links {
		if err := s.applyLinkOp(l); err != nil {
			return fmt.Errorf("masterdata: links[%d]: %w", i, err)
		}
	}
	for i, f := range ops.facts {
		if err := s.applyFactOp(f); err != nil {
			return fmt.Errorf("masterdata: facts[%d]: %w", i, err)
		}
	}
	if err := s.recomputeMasters(); err != nil {
		return err
	}
	return s.rebindFacts()
}

func (s *Store) applyRecordOp(r recordOp) error {
	switch r.op {
	case "UPSERT":
		text, err := valueMapCodec.TextualFromNative(nil, r.attrs)
		if err != nil {
			return fmt.Errorf("cannot encode attributes of %s/%s: %w", r.who.source, r.who.id, err)
		}
		return s.db.exec(
			`INSERT OR REPLACE INTO source_records(source, id, attributes) VALUES(?, ?, ?)`,
			textVal(r.who.source), textVal(r.who.id), textVal(string(text)))
	case "DELETE":
		if err := s.db.exec(`DELETE FROM source_records WHERE source = ? AND id = ?`,
			textVal(r.who.source), textVal(r.who.id)); err != nil {
			return err
		}
		// Deleting a source record clears its duplicate links.
		return s.db.exec(
			`DELETE FROM links WHERE (a_source = ? AND a_id = ?) OR (b_source = ? AND b_id = ?)`,
			textVal(r.who.source), textVal(r.who.id), textVal(r.who.source), textVal(r.who.id))
	default:
		return fmt.Errorf("unknown record op %q", r.op)
	}
}

func (s *Store) applyLinkOp(l linkOp) error {
	a, b := normalizePair(l.a, l.b)
	switch l.op {
	case "LINK":
		for _, endpoint := range []identity{a, b} {
			exists, err := s.recordExists(endpoint)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("cannot link %s/%s: source record not found", endpoint.source, endpoint.id)
			}
		}
		return s.db.exec(
			`INSERT OR IGNORE INTO links(a_source, a_id, b_source, b_id) VALUES(?, ?, ?, ?)`,
			textVal(a.source), textVal(a.id), textVal(b.source), textVal(b.id))
	case "UNLINK":
		return s.db.exec(
			`DELETE FROM links WHERE a_source = ? AND a_id = ? AND b_source = ? AND b_id = ?`,
			textVal(a.source), textVal(a.id), textVal(b.source), textVal(b.id))
	default:
		return fmt.Errorf("unknown link op %q", l.op)
	}
}

func (s *Store) recordExists(id identity) (bool, error) {
	found := false
	err := s.db.query(`SELECT 1 FROM source_records WHERE source = ? AND id = ?`,
		[]sqlVal{textVal(id.source), textVal(id.id)},
		func([]sqlVal) error { found = true; return nil })
	return found, err
}

func (s *Store) applyFactOp(f factOp) error {
	switch f.op {
	case "UPSERT":
		text, err := valueMapCodec.TextualFromNative(nil, f.payload)
		if err != nil {
			return fmt.Errorf("cannot encode payload of fact %q: %w", f.factID, err)
		}
		origSource, origID := nullVal(), nullVal()
		if f.entity != nil {
			origSource, origID = textVal(f.entity.source), textVal(f.entity.id)
		}
		// The rebound entity reference is (re)computed by rebindFacts below.
		return s.db.exec(
			`INSERT OR REPLACE INTO facts(fact_id, orig_source, orig_id, entity_source, entity_id, payload)
			 VALUES(?, ?, ?, NULL, NULL, ?)`,
			textVal(f.factID), origSource, origID, textVal(string(text)))
	case "DELETE":
		return s.db.exec(`DELETE FROM facts WHERE fact_id = ?`, textVal(f.factID))
	default:
		return fmt.Errorf("unknown fact op %q", f.op)
	}
}

// recomputeMasters rebuilds the masters and members tables from the current
// source records and links.
func (s *Store) recomputeMasters() error {
	records := make(map[identity]map[string]interface{})
	err := s.db.query(`SELECT source, id, attributes FROM source_records`, nil,
		func(cols []sqlVal) error {
			native, _, err := valueMapCodec.NativeFromTextual([]byte(cols[2].text))
			if err != nil {
				return fmt.Errorf("masterdata: cannot decode stored attributes of %s/%s: %w",
					cols[0].text, cols[1].text, err)
			}
			attrs, ok := native.(map[string]interface{})
			if !ok {
				return fmt.Errorf("masterdata: stored attributes of %s/%s are %T, want a map",
					cols[0].text, cols[1].text, native)
			}
			records[identity{source: cols[0].text, id: cols[1].text}] = attrs
			return nil
		})
	if err != nil {
		return err
	}
	var links [][2]identity
	err = s.db.query(`SELECT a_source, a_id, b_source, b_id FROM links`, nil,
		func(cols []sqlVal) error {
			links = append(links, [2]identity{
				{source: cols[0].text, id: cols[1].text},
				{source: cols[2].text, id: cols[3].text},
			})
			return nil
		})
	if err != nil {
		return err
	}
	masters := computeMasters(records, links, s.rankOf)
	if err := s.db.exec(`DELETE FROM masters`); err != nil {
		return err
	}
	if err := s.db.exec(`DELETE FROM members`); err != nil {
		return err
	}
	for _, m := range masters {
		text, err := valueMapCodec.TextualFromNative(nil, m.attrs)
		if err != nil {
			return fmt.Errorf("masterdata: cannot encode attributes of master %s/%s: %w",
				m.master.source, m.master.id, err)
		}
		if err := s.db.exec(
			`INSERT INTO masters(master_source, master_id, attributes) VALUES(?, ?, ?)`,
			textVal(m.master.source), textVal(m.master.id), textVal(string(text))); err != nil {
			return err
		}
		for _, member := range m.members {
			if err := s.db.exec(
				`INSERT INTO members(master_source, master_id, member_source, member_id) VALUES(?, ?, ?, ?)`,
				textVal(m.master.source), textVal(m.master.id),
				textVal(member.source), textVal(member.id)); err != nil {
				return err
			}
		}
	}
	return nil
}

// rebindFacts points every fact's entity reference at the master of the
// group that currently contains the fact's original source identity. Facts
// whose original source record is gone (deleted) — or that never had one —
// get a NULL reference; their payload is never touched.
func (s *Store) rebindFacts() error {
	memberMaster := make(map[identity]identity)
	err := s.db.query(`SELECT master_source, master_id, member_source, member_id FROM members`, nil,
		func(cols []sqlVal) error {
			memberMaster[identity{source: cols[2].text, id: cols[3].text}] =
				identity{source: cols[0].text, id: cols[1].text}
			return nil
		})
	if err != nil {
		return err
	}
	type factRef struct {
		factID string
		orig   *identity
	}
	var facts []factRef
	err = s.db.query(`SELECT fact_id, orig_source, orig_id FROM facts`, nil,
		func(cols []sqlVal) error {
			f := factRef{factID: cols[0].text}
			if !cols[1].null && !cols[2].null {
				f.orig = &identity{source: cols[1].text, id: cols[2].text}
			}
			facts = append(facts, f)
			return nil
		})
	if err != nil {
		return err
	}
	for _, f := range facts {
		entitySource, entityID := nullVal(), nullVal()
		if f.orig != nil {
			if master, ok := memberMaster[*f.orig]; ok {
				entitySource, entityID = textVal(master.source), textVal(master.id)
			}
		}
		if err := s.db.exec(`UPDATE facts SET entity_source = ?, entity_id = ? WHERE fact_id = ?`,
			entitySource, entityID, textVal(f.factID)); err != nil {
			return err
		}
	}
	return nil
}
