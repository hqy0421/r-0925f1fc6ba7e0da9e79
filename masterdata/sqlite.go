package masterdata

/*
#cgo LDFLAGS: -l:libsqlite3.so.0

#include <stdlib.h>

// This package links against the system SQLite shared library. The SQLite
// C API is stable and versioned; the declarations below match sqlite3.h so
// the package builds even without the development headers installed.
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;

int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs);
int sqlite3_close_v2(sqlite3 *db);
int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail);
int sqlite3_step(sqlite3_stmt *pStmt);
int sqlite3_finalize(sqlite3_stmt *pStmt);
int sqlite3_bind_text(sqlite3_stmt *pStmt, int idx, const char *val, int n, void *destructor);
int sqlite3_bind_null(sqlite3_stmt *pStmt, int idx);
int sqlite3_column_count(sqlite3_stmt *pStmt);
int sqlite3_column_type(sqlite3_stmt *pStmt, int idx);
const unsigned char *sqlite3_column_text(sqlite3_stmt *pStmt, int idx);
const char *sqlite3_errmsg(sqlite3 *db);

static int md_bind_text(sqlite3_stmt *stmt, int idx, const char *val) {
	// SQLITE_TRANSIENT ((sqlite3_destructor_type)-1) makes SQLite take its
	// own copy of the value, so the C string may be freed right after bind.
	return sqlite3_bind_text(stmt, idx, val, -1, (void *)-1);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// SQLite result codes, column types and open flags used by this package.
const (
	sqliteOK   = 0
	sqliteRow  = 100
	sqliteDone = 101

	sqliteTypeNull = 5

	openReadWrite = 0x0002
	openCreate    = 0x0004
	openFullMutex = 0x10000
)

// sqlVal is a SQLite TEXT value that may be NULL. Every value this package
// stores is either TEXT (structured data is kept as Avro JSON) or NULL.
type sqlVal struct {
	text string
	null bool
}

func textVal(s string) sqlVal { return sqlVal{text: s} }
func nullVal() sqlVal         { return sqlVal{null: true} }

// sqlDB is a minimal SQLite handle: prepared statements with TEXT/NULL
// bindings only, which is all this package needs.
type sqlDB struct {
	h *C.sqlite3
}

func openDB(path string) (*sqlDB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var h *C.sqlite3
	if rc := C.sqlite3_open_v2(cpath, &h, openReadWrite|openCreate|openFullMutex, nil); rc != sqliteOK {
		msg := "out of memory"
		if h != nil {
			msg = C.GoString(C.sqlite3_errmsg(h))
			C.sqlite3_close_v2(h)
		}
		return nil, fmt.Errorf("masterdata: cannot open SQLite database %q: %s", path, msg)
	}
	return &sqlDB{h: h}, nil
}

func (d *sqlDB) close() error {
	if rc := C.sqlite3_close_v2(d.h); rc != sqliteOK {
		return d.lastError("close")
	}
	d.h = nil
	return nil
}

func (d *sqlDB) lastError(what string) error {
	return fmt.Errorf("masterdata: sqlite %s failed: %s", what, C.GoString(C.sqlite3_errmsg(d.h)))
}

// schemaStatements creates the tables of the store: the original source
// records and duplicate links, the derived master entities with their
// members, and the facts with their original and rebound entity references.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS source_records(
		source TEXT NOT NULL,
		id TEXT NOT NULL,
		attributes TEXT NOT NULL,
		PRIMARY KEY(source, id)
	)`,
	`CREATE TABLE IF NOT EXISTS links(
		a_source TEXT NOT NULL,
		a_id TEXT NOT NULL,
		b_source TEXT NOT NULL,
		b_id TEXT NOT NULL,
		PRIMARY KEY(a_source, a_id, b_source, b_id)
	)`,
	`CREATE TABLE IF NOT EXISTS facts(
		fact_id TEXT NOT NULL PRIMARY KEY,
		orig_source TEXT,
		orig_id TEXT,
		entity_source TEXT,
		entity_id TEXT,
		payload TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS masters(
		master_source TEXT NOT NULL,
		master_id TEXT NOT NULL,
		attributes TEXT NOT NULL,
		PRIMARY KEY(master_source, master_id)
	)`,
	`CREATE TABLE IF NOT EXISTS members(
		master_source TEXT NOT NULL,
		master_id TEXT NOT NULL,
		member_source TEXT NOT NULL,
		member_id TEXT NOT NULL,
		PRIMARY KEY(master_source, master_id, member_source, member_id)
	)`,
}

func (d *sqlDB) initSchema() error {
	for _, stmt := range schemaStatements {
		if err := d.exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// exec runs a statement that returns no rows.
func (d *sqlDB) exec(query string, args ...sqlVal) error {
	return d.withStmt(query, args, func(stmt *C.sqlite3_stmt) error {
		if rc := C.sqlite3_step(stmt); rc != sqliteDone {
			return d.lastError("exec")
		}
		return nil
	})
}

// query runs a statement and invokes fn for every result row.
func (d *sqlDB) query(query string, args []sqlVal, fn func(cols []sqlVal) error) error {
	return d.withStmt(query, args, func(stmt *C.sqlite3_stmt) error {
		for {
			rc := C.sqlite3_step(stmt)
			switch rc {
			case sqliteDone:
				return nil
			case sqliteRow:
				n := int(C.sqlite3_column_count(stmt))
				cols := make([]sqlVal, n)
				for i := 0; i < n; i++ {
					if C.sqlite3_column_type(stmt, C.int(i)) == sqliteTypeNull {
						cols[i] = nullVal()
						continue
					}
					txt := C.sqlite3_column_text(stmt, C.int(i))
					cols[i] = textVal(C.GoString((*C.char)(unsafe.Pointer(txt))))
				}
				if err := fn(cols); err != nil {
					return err
				}
			default:
				return d.lastError("query")
			}
		}
	})
}

func (d *sqlDB) withStmt(query string, args []sqlVal, fn func(stmt *C.sqlite3_stmt) error) error {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(d.h, cquery, -1, &stmt, nil); rc != sqliteOK {
		return d.lastError("prepare")
	}
	defer C.sqlite3_finalize(stmt)
	for i, arg := range args {
		var rc C.int
		if arg.null {
			rc = C.sqlite3_bind_null(stmt, C.int(i+1))
		} else {
			ctext := C.CString(arg.text)
			rc = C.md_bind_text(stmt, C.int(i+1), ctext)
			C.free(unsafe.Pointer(ctext))
		}
		if rc != sqliteOK {
			return d.lastError("bind")
		}
	}
	return fn(stmt)
}
