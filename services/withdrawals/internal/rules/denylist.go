package rules

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync"
)

// Denylist is the local screening list.
//
// Deliberate simplification (SPEC §0, ADR 0011): screening checks the
// destination against a local file of addresses, one per line ("#" comments).
// A real deployment would call an AML provider; the asynchronous screening
// step and its outcomes (REJECTED, or onward to review/approval) are the same.
type Denylist struct {
	mu   sync.RWMutex
	path string
	set  map[string]bool
}

// LoadDenylist reads path (an empty path is an empty list).
func LoadDenylist(path string) (*Denylist, error) {
	d := &Denylist{path: path}
	return d, d.Reload()
}

// Reload re-reads the file.
func (d *Denylist) Reload() error {
	set := map[string]bool{}
	if d.path != "" {
		f, err := os.Open(d.path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		if set, err = parseDenylist(f); err != nil {
			return err
		}
	}
	d.mu.Lock()
	d.set = set
	d.mu.Unlock()
	return nil
}

func parseDenylist(r io.Reader) (map[string]bool, error) {
	set := map[string]bool{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line != "" {
			set[strings.ToLower(line)] = true
		}
	}
	return set, sc.Err()
}

// Listed reports whether address is on the list. Comparison ignores case:
// bech32 is lower case by definition and Ethereum hex is case-insensitive.
func (d *Denylist) Listed(address string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.set[strings.ToLower(strings.TrimSpace(address))]
}

// Len returns the number of listed addresses.
func (d *Denylist) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.set)
}
