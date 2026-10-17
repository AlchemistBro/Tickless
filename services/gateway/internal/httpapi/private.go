package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/wshub"
)

// Hijack lets WebSocket upgrades pass through the metrics wrapper.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(r.ResponseWriter).Hijack()
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Flush implements http.Flusher for streamed responses.
func (r *statusRecorder) Flush() { _ = http.NewResponseController(r.ResponseWriter).Flush() }

const defaultAuthDeadline = 10 * time.Second

// privateWS serves /ws/private. The first client message must authenticate:
//
//	{"op":"auth","token":"<access token>"}
//
// (browsers cannot set headers on WebSocket requests, and putting the token in
// the URL would leak it into logs). After {"type":"authenticated"} the client
// receives only its own events on the channels orders, fills and balances.
func (s *Server) privateWS(w http.ResponseWriter, r *http.Request) {
	s.priv.Serve(w, r, wshub.Handler{
		Origins: s.d.WSOrigins,
		OnOpen: func(c *wshub.Conn) {
			deadline := s.d.WSAuthTimeout
			if deadline == 0 {
				deadline = defaultAuthDeadline
			}
			go func() { // unauthenticated connections do not live long
				time.Sleep(deadline)
				if c.Data.Load() == nil {
					c.Close("authentication timeout")
				}
			}()
		},
		OnMessage: s.privateMessage,
	})
}

func (s *Server) privateMessage(c *wshub.Conn, data []byte) {
	var m struct {
		Op    string `json:"op"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		c.Send(wsError("malformed message"), time.Time{})
		return
	}
	switch m.Op {
	case "ping":
		c.Send([]byte(`{"type":"pong"}`), time.Time{})
	case "auth":
		if c.Data.Load() != nil {
			c.Send(wsError("already authenticated"), time.Time{})
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p, err := s.verifyToken(ctx, m.Token)
		if err != nil {
			c.SendAndClose(wsError("invalid token"), "unauthenticated")
			return
		}
		if frozen, ferr := s.isFrozen(ctx, p.UserID); ferr != nil || frozen {
			c.SendAndClose(wsError("not allowed"), "forbidden") // fail closed on errors as well
			return
		}
		c.Data.Store(p)
		for _, ch := range []string{"orders", "fills", "balances"} {
			s.priv.Subscribe(c, userChannel(p.UserID, ch))
		}
		c.Send([]byte(fmt.Sprintf(`{"type":"authenticated","user_id":"%d","channels":["orders","fills","balances"]}`, p.UserID)), time.Time{})
	default:
		if c.Data.Load() == nil {
			c.Send(wsError("authenticate first"), time.Time{})
			return
		}
		c.Send(wsError("unknown op"), time.Time{})
	}
}

func wsError(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"type": "error", "message": msg})
	return b
}

func userChannel(uid int64, kind string) string { return fmt.Sprintf("u.%d.%s", uid, kind) }

// ---- feed from Kafka (called by the app's consumers) ----

type orderMsg struct {
	Type    string `json:"type"`
	Event   string `json:"event"`
	OrderID string `json:"order_id"`
	Market  string `json:"market"`
	Side    string `json:"side,omitempty"`
	Price   string `json:"price,omitempty"`
	Qty     string `json:"qty,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type fillMsg struct {
	Type    string `json:"type"`
	OrderID string `json:"order_id"`
	Market  string `json:"market"`
	Side    string `json:"side"`
	Role    string `json:"role"`
	Price   string `json:"price"`
	Qty     string `json:"qty"`
	Leaves  string `json:"leaves"`
	TradeID string `json:"trade_id"`
}

type balanceMsg struct {
	Type      string `json:"type"`
	Asset     string `json:"asset"`
	Available string `json:"available"`
	Held      string `json:"held"`
}

func sideName(s commonv1.Side) string {
	if s == commonv1.Side_SIDE_SELL {
		return "sell"
	}
	return "buy"
}

func opposite(s commonv1.Side) commonv1.Side {
	if s == commonv1.Side_SIDE_SELL {
		return commonv1.Side_SIDE_BUY
	}
	return commonv1.Side_SIDE_SELL
}

// PublishMatching routes a matching event to the users it concerns. ts is the
// time the event was produced (for delivery latency).
func (s *Server) PublishMatching(ev *matchingv1.Event, ts time.Time) {
	if s.d.Markets == nil {
		return
	}
	spec, ok := s.d.Markets.Spec(ev.Market)
	if !ok {
		return
	}
	marshal := func(v any) []byte { b, _ := json.Marshal(v); return b }
	switch ev.Type {
	case matchingv1.EventType_EVENT_TYPE_ACCEPTED:
		s.priv.Publish(userChannel(int64(ev.UserId), "orders"), marshal(orderMsg{Type: "order", Event: "accepted", OrderID: fmt.Sprint(ev.OrderId),
			Market: ev.Market, Side: sideName(ev.Side), Price: priceOrEmpty(spec, ev.Price), Qty: spec.FormatQty(ev.Qty)}), ts)
	case matchingv1.EventType_EVENT_TYPE_CANCELLED:
		s.priv.Publish(userChannel(int64(ev.UserId), "orders"), marshal(orderMsg{Type: "order", Event: "cancelled", OrderID: fmt.Sprint(ev.OrderId),
			Market: ev.Market, Qty: spec.FormatQty(ev.Qty), Reason: ev.Reason.String()}), ts)
	case matchingv1.EventType_EVENT_TYPE_REJECTED:
		if ev.Reason == matchingv1.Reason_REASON_UNKNOWN_ORDER || ev.Reason == matchingv1.Reason_REASON_DUPLICATE_ORDER {
			return // not an outcome of the user's order
		}
		s.priv.Publish(userChannel(int64(ev.UserId), "orders"), marshal(orderMsg{Type: "order", Event: "rejected", OrderID: fmt.Sprint(ev.OrderId),
			Market: ev.Market, Reason: ev.Reason.String()}), ts)
	case matchingv1.EventType_EVENT_TYPE_TRADE:
		taker := fillMsg{Type: "fill", OrderID: fmt.Sprint(ev.OrderId), Market: ev.Market, Side: sideName(ev.Side), Role: "taker",
			Price: spec.FormatPrice(ev.Price), Qty: spec.FormatQty(ev.Qty), Leaves: spec.FormatQty(ev.TakerLeaves), TradeID: fmt.Sprint(ev.TradeId)}
		maker := fillMsg{Type: "fill", OrderID: fmt.Sprint(ev.MakerOrderId), Market: ev.Market, Side: sideName(opposite(ev.Side)), Role: "maker",
			Price: taker.Price, Qty: taker.Qty, Leaves: spec.FormatQty(ev.MakerLeaves), TradeID: taker.TradeID}
		s.priv.Publish(userChannel(int64(ev.UserId), "fills"), marshal(taker), ts)
		s.priv.Publish(userChannel(int64(ev.MakerUserId), "fills"), marshal(maker), ts)
	}
}

func priceOrEmpty(spec interface{ FormatPrice(int64) string }, p int64) string {
	if p <= 0 {
		return ""
	}
	return spec.FormatPrice(p)
}

// PublishBalance routes a ledger BalanceChanged event to its user.
func (s *Server) PublishBalance(ev *ledgerv1.BalanceChanged, ts time.Time) {
	a, known := money.LookupAsset(ev.Asset)
	avail, held := ev.Available.GetUnits(), ev.Held.GetUnits()
	if known {
		avail, held = formatUnits(avail, a.Decimals, true), formatUnits(held, a.Decimals, true)
	}
	b, _ := json.Marshal(balanceMsg{Type: "balance", Asset: ev.Asset, Available: avail, Held: held})
	s.priv.Publish(userChannel(ev.UserId, "balances"), b, ts)
}

// PrivateConnections reports open private sockets (tests, metrics).
func (s *Server) PrivateConnections() int64 { return s.priv.Connections() }
