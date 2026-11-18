package masterdata

import "sort"

// identity is the source identity of a customer record: the pair of source
// system name and the record's id within that source.
type identity struct {
	source string
	id     string
}

// less orders identities lexicographically by (source, id). The master ID
// of a group is its smallest identity under this order.
func (a identity) less(b identity) bool {
	if a.source != b.source {
		return a.source < b.source
	}
	return a.id < b.id
}

// normalizePair orders the endpoints of an undirected link so each pair is
// stored exactly once.
func normalizePair(a, b identity) (identity, identity) {
	if b.less(a) {
		return b, a
	}
	return a, b
}

// unionFind is a disjoint-set forest over source identities, used to fold
// the transitive duplicate-link graph into connected groups.
type unionFind struct {
	parent map[identity]identity
}

func newUnionFind() *unionFind {
	return &unionFind{parent: make(map[identity]identity)}
}

func (u *unionFind) find(x identity) identity {
	if _, ok := u.parent[x]; !ok {
		u.parent[x] = x
		return x
	}
	root := x
	for u.parent[root] != root {
		root = u.parent[root]
	}
	// Path compression: point every node on the walk directly at the root.
	for u.parent[x] != root {
		u.parent[x], x = root, u.parent[x]
	}
	return root
}

func (u *unionFind) union(a, b identity) {
	ra, rb := u.find(a), u.find(b)
	if ra != rb {
		u.parent[ra] = rb
	}
}

// masterGroup is one computed master entity: its master identity, all
// member source identities, and the merged attributes.
type masterGroup struct {
	master  identity
	members []identity
	attrs   map[string]interface{}
}

// computeMasters folds records and links into master groups. Every record
// lands in exactly one group; records without any link form singleton
// groups, so isolated customers are preserved. The result is sorted by
// master identity for deterministic output.
func computeMasters(records map[identity]map[string]interface{}, links [][2]identity, rankOf func(string) int) []masterGroup {
	uf := newUnionFind()
	for id := range records {
		uf.find(id) // register every record, linked or not
	}
	for _, l := range links {
		// Links whose endpoint records are gone cannot happen (deleting a
		// record clears its links), but skip them defensively.
		if _, ok := records[l[0]]; !ok {
			continue
		}
		if _, ok := records[l[1]]; !ok {
			continue
		}
		uf.union(l[0], l[1])
	}
	groups := make(map[identity][]identity)
	for id := range records {
		root := uf.find(id)
		groups[root] = append(groups[root], id)
	}
	masters := make([]masterGroup, 0, len(groups))
	for _, members := range groups {
		sort.Slice(members, func(i, j int) bool { return members[i].less(members[j]) })
		masters = append(masters, masterGroup{
			master:  members[0], // smallest source identity of the group
			members: members,
			attrs:   mergeAttributes(members, records, rankOf),
		})
	}
	sort.Slice(masters, func(i, j int) bool { return masters[i].master.less(masters[j].master) })
	return masters
}

// mergeAttributes resolves every attribute key independently: members are
// ordered by source priority (rankOf; ties broken by identity order) and
// the first non-NULL value wins. Only an explicit nil (Avro null) or an
// absent key counts as missing; 0, false and "" are kept.
func mergeAttributes(members []identity, records map[identity]map[string]interface{}, rankOf func(string) int) map[string]interface{} {
	ordered := make([]identity, len(members))
	copy(ordered, members)
	sort.Slice(ordered, func(i, j int) bool {
		ri, rj := rankOf(ordered[i].source), rankOf(ordered[j].source)
		if ri != rj {
			return ri < rj
		}
		return ordered[i].less(ordered[j])
	})
	keys := make(map[string]struct{})
	for _, m := range ordered {
		for k := range records[m] {
			keys[k] = struct{}{}
		}
	}
	sortedKeys := make([]string, 0, len(keys))
	for k := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	attrs := make(map[string]interface{}, len(sortedKeys))
	for _, k := range sortedKeys {
		for _, m := range ordered {
			if v, ok := records[m][k]; ok && v != nil {
				attrs[k] = v
				break
			}
		}
	}
	return attrs
}
