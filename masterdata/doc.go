// Package masterdata implements customer master data merging on top of
// goavro and SQLite.
//
// A Store keeps the current state of customer records received from multiple
// source systems, of manually confirmed undirected duplicate links between
// those records, and of facts that reference customers. Batches of
// corrections, deletions, link confirmations/removals and fact updates
// arrive Avro-binary encoded and are decoded with a real goavro Codec (see
// BatchSchema). Every batch is applied inside a single SQLite transaction:
// the original records and links, the recomputed master data, and the
// rebound fact references all take effect together; on any failure the whole
// batch is rolled back and the previous state is kept. The state lives in a
// SQLite database file, so reopening the store continues applying
// corrections where the previous run left off.
//
// Master entities are the connected components of the link graph: links are
// transitive, removing a link can split a group again, and customers without
// any link still form their own singleton group. The master ID of a group is
// the lexicographically smallest source identity (source, then id) in the
// group. Each master attribute is resolved independently: group members are
// ordered by source priority (Config.SourcePriority; unlisted sources sort
// last, ties broken by source name and id) and the first non-NULL value
// wins. The values 0, false and "" are real values and are never treated as
// missing; only an explicit Avro null (or an absent key) is missing.
//
// Facts carry an immutable original source identity. After every batch the
// entity reference of each fact is rebound to the master of the group that
// currently contains the original identity. When a source record is deleted
// its links are cleared and facts that referenced it have their entity
// reference set to NULL; the rest of the fact payload is left unchanged.
//
// ExportMasters and ExportFacts return the current master data and facts as
// Avro Object Container Files encoded with the real goavro Codec (see
// MasterSchema and FactSchema). The package deliberately does no fuzzy
// matching and keeps no historical versions.
//
// The package requires cgo and links against the system SQLite library. It
// is purely additive: the existing goavro API is unchanged.
package masterdata
