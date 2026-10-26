package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	walletv1 "github.com/tickless/pkg/gen/tickless/wallet/v1"
)

// chainAssets lists the assets a deposit address of each chain receives.
var chainAssets = map[string][]string{"btc": {"BTC"}, "eth": {"ETH", "USDT"}}

type depositAddressBody struct {
	Chain   string   `json:"chain"`
	Address string   `json:"address"`
	Assets  []string `json:"assets"`
}

func (s *Server) depositAddress(w http.ResponseWriter, r *http.Request) {
	if s.d.Wallet == nil {
		writeErr(w, http.StatusServiceUnavailable, "deposits_unavailable", "deposits are not enabled")
		return
	}
	chain := strings.ToLower(r.URL.Query().Get("chain"))
	assets, ok := chainAssets[chain]
	if !ok {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_chain", "chain must be btc or eth")
		return
	}
	p, _ := principalFrom(r.Context())
	resp, err := s.d.Wallet.GetDepositAddress(r.Context(), &walletv1.GetDepositAddressRequest{UserId: p.UserID, Chain: chain})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, depositAddressBody{Chain: resp.Chain, Address: resp.Address, Assets: assets})
}

type depositBody struct {
	ID                    string `json:"id"`
	Chain                 string `json:"chain"`
	TxID                  string `json:"txid"`
	Index                 int64  `json:"index"`
	Asset                 string `json:"asset"`
	Amount                string `json:"amount"`
	Address               string `json:"address"`
	Status                string `json:"status"`
	BlockHeight           int64  `json:"block_height"`
	Confirmations         int64  `json:"confirmations"`
	RequiredConfirmations int64  `json:"required_confirmations"`
	DetectedAt            string `json:"detected_at"`
	ConfirmedAt           string `json:"confirmed_at,omitempty"`
}

var depositStatus = map[chainwatcherv1.DepositStatus]string{
	chainwatcherv1.DepositStatus_DEPOSIT_STATUS_PENDING:   "pending",
	chainwatcherv1.DepositStatus_DEPOSIT_STATUS_CONFIRMED: "confirmed",
	chainwatcherv1.DepositStatus_DEPOSIT_STATUS_REORGED:   "reorged",
}

func (s *Server) listDeposits(w http.ResponseWriter, r *http.Request) {
	if s.d.Chainwatcher == nil {
		writeErr(w, http.StatusServiceUnavailable, "deposits_unavailable", "deposits are not enabled")
		return
	}
	p, _ := principalFrom(r.Context())
	before, _ := strconv.ParseInt(r.URL.Query().Get("before_id"), 10, 64)
	resp, err := s.d.Chainwatcher.ListDeposits(r.Context(), &chainwatcherv1.ListDepositsRequest{UserId: p.UserID, BeforeId: before, Limit: queryInt32(r, "limit")})
	if err != nil {
		writeGRPCErr(w, r, err)
		return
	}
	out := make([]depositBody, 0, len(resp.Deposits))
	for _, d := range resp.Deposits {
		dec, known := assetDecimals(d.Asset)
		b := depositBody{
			ID: itoa64(d.Id), Chain: d.Chain, TxID: d.Txid, Index: d.Index, Asset: d.Asset,
			Amount: formatUnits(d.Amount.GetUnits(), dec, known), Address: d.Address, Status: depositStatus[d.Status],
			BlockHeight: d.BlockHeight, Confirmations: d.Confirmations, RequiredConfirmations: d.RequiredConfirmations,
			DetectedAt: time.Unix(0, d.DetectedAtUnixNano).UTC().Format(time.RFC3339Nano),
		}
		if d.ConfirmedAtUnixNano > 0 {
			b.ConfirmedAt = time.Unix(0, d.ConfirmedAtUnixNano).UTC().Format(time.RFC3339Nano)
		}
		out = append(out, b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"deposits": out})
}
