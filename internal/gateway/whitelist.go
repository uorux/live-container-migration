package gateway

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Whitelist decides who may start an asleep backend. Without it, anyone who
// starts a login (or picks a server in the menu) wakes that server, so a
// stranger could wake every pack and fill the cluster's memory. The servers
// still authenticate players themselves; this only gates wakes and holds.
//
// Names and UUIDs in Login Start aren't authenticated (that happens at the
// server, with Mojang), so the list stops scanners and random bots, not
// someone who knows a whitelisted name.
type Whitelist struct {
	// files maps a backend (or "*" for every other backend) to the lists
	// whose union may wake it: whitelist.json / ops.json files (JSON arrays
	// of objects with "uuid" and "name"), as the servers use.
	files map[string][]string
	log   *slog.Logger

	mu    sync.Mutex
	cache map[string]*listFile
}

type listFile struct {
	checked time.Time
	mod     time.Time
	size    int64
	names   map[string]bool // lower-cased
	uuids   map[[16]byte]bool
	err     error
}

// listRecheck bounds how often a list file is stat'ed for changes (a
// ConfigMap update arrives as a new file behind a symlink).
var listRecheck = 5 * time.Second

// ParseWhitelist parses "backend=file[+file...],...", where backend "*"
// covers every backend not named. An empty spec returns nil (no whitelist).
func ParseWhitelist(spec string, log *slog.Logger) (*Whitelist, error) {
	files := map[string][]string{}
	for _, part := range strings.Split(spec, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		name, paths, ok := strings.Cut(part, "=")
		if !ok || name == "" || paths == "" {
			return nil, fmt.Errorf("whitelist entry %q: want backend=file[+file]", part)
		}
		for _, p := range strings.Split(paths, "+") {
			if p = strings.TrimSpace(p); p != "" {
				files[name] = append(files[name], p)
			}
		}
	}
	if len(files) == 0 {
		return nil, nil
	}
	if log == nil {
		log = slog.Default()
	}
	w := &Whitelist{files: files, log: log, cache: map[string]*listFile{}}
	for backend, paths := range files {
		n := 0
		for _, p := range paths {
			lf := w.load(p)
			if lf.err != nil {
				return nil, lf.err
			}
			n += len(lf.names)
		}
		if n == 0 {
			log.Warn("whitelist is empty: nobody can wake this backend", "backend", backend, "files", paths)
		} else {
			log.Info("whitelist loaded", "backend", backend, "entries", n)
		}
	}
	return w, nil
}

// Backends returns the backend names the whitelist has lists for ("*" for
// the default).
func (w *Whitelist) Backends() []string {
	if w == nil {
		return nil
	}
	var names []string
	for n := range w.files {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Allowed reports whether a player may wake backend. A backend with no list
// (and no "*") is open. A list that can't be read allows nobody, like a
// server whose whitelist is empty.
func (w *Whitelist) Allowed(backend, name string, uuid *[16]byte) bool {
	if w == nil {
		return true
	}
	paths, ok := w.files[backend]
	if !ok {
		paths, ok = w.files["*"]
	}
	if !ok {
		return true
	}
	lname := strings.ToLower(name)
	for _, p := range paths {
		lf := w.load(p)
		if lf.err != nil {
			continue
		}
		if lf.names[lname] || (uuid != nil && lf.uuids[*uuid]) {
			return true
		}
	}
	return false
}

func (w *Whitelist) load(path string) *listFile {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	lf := w.cache[path]
	if lf != nil && now.Sub(lf.checked) < listRecheck {
		return lf
	}
	fi, err := os.Stat(path)
	if err == nil && lf != nil && lf.err == nil && fi.ModTime().Equal(lf.mod) && fi.Size() == lf.size {
		lf.checked = now
		return lf
	}
	next := &listFile{checked: now}
	if err == nil {
		next.mod, next.size = fi.ModTime(), fi.Size()
		next.names, next.uuids, err = readList(path)
	}
	if err != nil {
		next.err = fmt.Errorf("whitelist %s: %w", path, err)
		if lf == nil || lf.err == nil {
			w.log.Warn("can't read whitelist; it allows nobody until fixed", "file", path, "err", err)
		}
	} else if lf != nil {
		w.log.Info("whitelist reloaded", "file", path, "entries", len(next.names))
	}
	w.cache[path] = next
	return next
}

func readList(path string) (map[string]bool, map[[16]byte]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var entries []struct {
		UUID string `json:"uuid"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, nil, err
	}
	names, uuids := map[string]bool{}, map[[16]byte]bool{}
	for _, e := range entries {
		if e.Name != "" {
			names[strings.ToLower(e.Name)] = true
		}
		if u, ok := parseUUID(e.UUID); ok {
			uuids[u] = true
		}
	}
	return names, uuids, nil
}

func parseUUID(s string) ([16]byte, bool) {
	var u [16]byte
	h := strings.ReplaceAll(s, "-", "")
	if len(h) != 32 {
		return u, false
	}
	if _, err := hex.Decode(u[:], []byte(h)); err != nil {
		return u, false
	}
	return u, true
}

// loginUUID returns the profile UUID a Login Start body carries after the
// name, if any: 1.20.2+ always sends it, 1.19.3-1.20.1 behind a "has UUID"
// boolean. nameLen is the encoded length of the name field.
func loginUUID(body []byte, nameLen int) *[16]byte {
	rest := body[nameLen:]
	var u [16]byte
	switch {
	case len(rest) == 16:
		copy(u[:], rest)
	case len(rest) == 17 && rest[0] == 1:
		copy(u[:], rest[1:])
	default:
		return nil
	}
	return &u
}
