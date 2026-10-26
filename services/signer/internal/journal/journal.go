// Package journal remembers every transaction the signer produced, one file
// per withdrawal, written atomically before the signed transaction is
// returned. A retried request gets the same transaction back, so a withdrawal
// can never be paid twice by re-signing, and spent BTC outputs and used ETH
// nonces are never reused for another withdrawal.
package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrConflict is returned when a withdrawal id is reused with other terms.
var ErrConflict = errors.New("journal: withdrawal already signed with different terms")

// Record is one signed withdrawal.
type Record struct {
	WithdrawalID int64    `json:"withdrawal_id"`
	Chain        string   `json:"chain"`
	Asset        string   `json:"asset"`
	Amount       string   `json:"amount"`
	Destination  string   `json:"destination"`
	TxID         string   `json:"txid"`
	RawTx        string   `json:"raw_tx"` // hex
	NetworkFee   string   `json:"network_fee"`
	FeeAsset     string   `json:"fee_asset"`
	Spent        []string `json:"spent,omitempty"` // BTC outpoints "txid:vout"
	Nonce        *uint64  `json:"nonce,omitempty"` // ETH
}

// SameTerms reports whether r pays the same thing as a request.
func (r Record) SameTerms(chain, asset, amount, destination string) bool {
	return r.Chain == chain && r.Asset == asset && r.Amount == amount && strings.EqualFold(r.Destination, destination)
}

// Journal is a directory of records.
type Journal struct {
	dir string
	mu  sync.Mutex
}

// Open opens (creating) the journal directory.
func Open(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Journal{dir: dir}, nil
}

func (j *Journal) path(id int64) string { return filepath.Join(j.dir, fmt.Sprintf("%d.json", id)) }

// Get returns the record of a withdrawal.
func (j *Journal) Get(id int64) (Record, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.get(id)
}

func (j *Journal) get(id int64) (Record, bool, error) {
	b, err := os.ReadFile(j.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, false, fmt.Errorf("journal %d: %w", id, err)
	}
	return r, true, nil
}

// Put stores a new record. It refuses to replace an existing one.
func (j *Journal) Put(r Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok, err := j.get(r.WithdrawalID); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("journal: withdrawal %d is already recorded", r.WithdrawalID)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	tmp := j.path(r.WithdrawalID) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // path is built from an int id inside the journal dir
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, j.path(r.WithdrawalID))
}

// All returns every record.
func (j *Journal) All() ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var id int64
		if _, err := fmt.Sscanf(e.Name(), "%d.json", &id); err != nil {
			continue
		}
		r, ok, err := j.get(id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, r)
		}
	}
	return out, nil
}

// SpentOutpoints returns every BTC outpoint used by a recorded transaction.
func (j *Journal) SpentOutpoints() (map[string]bool, error) {
	all, err := j.All()
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, r := range all {
		for _, o := range r.Spent {
			out[o] = true
		}
	}
	return out, nil
}

// NextNonce returns one above the highest recorded nonce of chain (0 if none).
func (j *Journal) NextNonce(chain string) (uint64, error) {
	all, err := j.All()
	if err != nil {
		return 0, err
	}
	var next uint64
	for _, r := range all {
		if r.Chain == chain && r.Nonce != nil && *r.Nonce+1 > next {
			next = *r.Nonce + 1
		}
	}
	return next, nil
}
