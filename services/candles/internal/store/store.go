// Package store keeps trades and OHLCV candles in ClickHouse.
//
// Trades are inserted into a plain MergeTree ordered by (market, time). Six
// materialized views (one per interval) fold every inserted block into
// AggregatingMergeTree tables, so candles are always exactly the aggregation of
// the stored trades and need no separate job (ADR 0008).
//
// Inserts are idempotent per market: the matching engine numbers trades with a
// per-market monotonic trade_id and replays re-emit identical trades, so the
// store only inserts trade_id greater than the highest one already stored.
package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Intervals maps the API name of a candle interval to its ClickHouse INTERVAL
// and its length in seconds. Table names derive from the key, so only these
// literals are ever interpolated into SQL.
var Intervals = map[string]struct {
	Clause  string
	Seconds int64
}{
	"1m":  {"INTERVAL 1 MINUTE", 60},
	"5m":  {"INTERVAL 5 MINUTE", 300},
	"15m": {"INTERVAL 15 MINUTE", 900},
	"1h":  {"INTERVAL 1 HOUR", 3600},
	"4h":  {"INTERVAL 4 HOUR", 14400},
	"1d":  {"INTERVAL 1 DAY", 86400},
}

// IntervalNames lists the supported intervals shortest first.
var IntervalNames = []string{"1m", "5m", "15m", "1h", "4h", "1d"}

// Trade is one execution to store (ticks / lots).
type Trade struct {
	Market    string
	TradeID   uint64
	Seq       uint64
	Time      time.Time
	Price     int64
	Qty       int64
	TakerBuy  bool
	TakerUser uint64
	MakerUser uint64
}

// Candle is one OHLCV bucket (ticks / lots).
type Candle struct {
	OpenTime int64
	Open     int64
	High     int64
	Low      int64
	Close    int64
	Lots     int64
	Turnover string
	Trades   int64
}

// Store is the ClickHouse repository.
type Store struct {
	conn driver.Conn
	mu   sync.Mutex
	last map[string]uint64 // market -> highest stored trade_id (cache)
}

// Options connect to ClickHouse's native protocol.
type Options struct {
	Addr, Database, User, Password string
}

// Open connects and creates the schema if needed.
func Open(ctx context.Context, o Options) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{o.Addr},
		Auth: clickhouse.Auth{Database: o.Database, Username: o.User, Password: o.Password},
		Settings: clickhouse.Settings{
			"max_execution_time": 30,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	s := &Store{conn: conn, last: map[string]uint64{}}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse schema: %w", err)
	}
	return s, nil
}

// Close closes the connection.
func (s *Store) Close() error { return s.conn.Close() }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	if err := s.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS trades (
		    market     LowCardinality(String),
		    trade_id   UInt64,
		    seq        UInt64,
		    ts         DateTime64(9, 'UTC'),
		    price      Int64,
		    qty        Int64,
		    taker_buy  UInt8,
		    taker_user UInt64,
		    maker_user UInt64
		) ENGINE = MergeTree
		ORDER BY (market, ts, trade_id)`); err != nil {
		return err
	}
	for _, name := range IntervalNames {
		iv := Intervals[name]
		if err := s.conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS candles_%[1]s (
			    market   LowCardinality(String),
			    bucket   DateTime('UTC'),
			    open     AggregateFunction(argMin, Int64, Tuple(DateTime64(9, 'UTC'), UInt64)),
			    high     SimpleAggregateFunction(max, Int64),
			    low      SimpleAggregateFunction(min, Int64),
			    close    AggregateFunction(argMax, Int64, Tuple(DateTime64(9, 'UTC'), UInt64)),
			    lots     SimpleAggregateFunction(sum, Int64),
			    turnover SimpleAggregateFunction(sum, Int128),
			    trades   SimpleAggregateFunction(sum, UInt64)
			) ENGINE = AggregatingMergeTree
			ORDER BY (market, bucket)`, name)); err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS candles_%[1]s_mv TO candles_%[1]s AS
			SELECT market,
			       toStartOfInterval(ts, %[2]s) AS bucket,
			       argMinState(price, (ts, trade_id)) AS open,
			       max(price) AS high,
			       min(price) AS low,
			       argMaxState(price, (ts, trade_id)) AS close,
			       sum(qty) AS lots,
			       sum(toInt128(price) * toInt128(qty)) AS turnover,
			       count() AS trades
			FROM trades
			GROUP BY market, bucket`, name, iv.Clause)); err != nil {
			return err
		}
	}
	return nil
}

// LastTradeID returns the highest trade_id stored for a market (0 = none),
// cached after the first lookup.
func (s *Store) LastTradeID(ctx context.Context, market string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLocked(ctx, market)
}

func (s *Store) lastLocked(ctx context.Context, market string) (uint64, error) {
	if v, ok := s.last[market]; ok {
		return v, nil
	}
	var v uint64
	if err := s.conn.QueryRow(ctx, `SELECT max(trade_id) FROM trades WHERE market = ?`, market).Scan(&v); err != nil {
		return 0, err
	}
	s.last[market] = v
	return v, nil
}

// ForgetCache drops the cached high-water marks (call when partition
// ownership changes: another instance may have inserted since).
func (s *Store) ForgetCache() {
	s.mu.Lock()
	s.last = map[string]uint64{}
	s.mu.Unlock()
}

// Insert stores the trades that are new (trade_id above the market high-water
// mark) in ONE block and returns the inserted ones. Trades must be in
// ascending trade_id order per market.
func (s *Store) Insert(ctx context.Context, trades []Trade) ([]Trade, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := map[string]uint64{}
	var fresh []Trade
	for _, t := range trades {
		last, err := s.lastLocked(ctx, t.Market)
		if err != nil {
			return nil, err
		}
		if hw := next[t.Market]; hw > last {
			last = hw
		}
		if t.TradeID <= last {
			continue // already stored (replay or duplicate delivery)
		}
		next[t.Market] = t.TradeID
		fresh = append(fresh, t)
	}
	if len(fresh) == 0 {
		return nil, nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO trades`)
	if err != nil {
		return nil, err
	}
	for _, t := range fresh {
		var tb uint8
		if t.TakerBuy {
			tb = 1
		}
		if err := batch.Append(t.Market, t.TradeID, t.Seq, t.Time.UTC(), t.Price, t.Qty, tb, t.TakerUser, t.MakerUser); err != nil {
			_ = batch.Abort()
			return nil, err
		}
	}
	if err := batch.Send(); err != nil {
		return nil, err
	}
	for m, id := range next {
		s.last[m] = id
	}
	return fresh, nil
}

// Candles returns candles newest first. fromUnix/toUnix (0 = open) bound the
// bucket start: from <= bucket < to.
func (s *Store) Candles(ctx context.Context, market, interval string, fromUnix, toUnix int64, limit int) ([]Candle, error) {
	if _, ok := Intervals[interval]; !ok {
		return nil, fmt.Errorf("unsupported interval %q", interval)
	}
	if limit <= 0 {
		limit = 500
	}
	limit = min(limit, 2000)
	q := fmt.Sprintf(`
		SELECT toInt64(toUnixTimestamp(bucket)), argMinMerge(open), max(high), min(low), argMaxMerge(close),
		       sum(lots), toString(sum(turnover)), sum(trades)
		FROM candles_%s
		WHERE market = ? AND (? = 0 OR bucket >= toDateTime(?, 'UTC')) AND (? = 0 OR bucket < toDateTime(?, 'UTC'))
		GROUP BY bucket ORDER BY bucket DESC LIMIT ?`, interval)
	rows, err := s.conn.Query(ctx, q, market, fromUnix, fromUnix, toUnix, toUnix, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Candle
	for rows.Next() {
		var c Candle
		var trades uint64
		if err := rows.Scan(&c.OpenTime, &c.Open, &c.High, &c.Low, &c.Close, &c.Lots, &c.Turnover, &trades); err != nil {
			return nil, err
		}
		c.Trades = int64(trades)
		out = append(out, c)
	}
	return out, rows.Err()
}
