// Command seed credits a user with funds in the LOCAL development stack so the
// terminal can be tried by hand (there are no deposits until Phase 5).
//
//	go run ./cmd/seed -email me@example.com -asset USDT -amount 100000
//
// It finds the user through the admin API and credits the ledger over gRPC.
// It only works against a stack started with LEDGER_ALLOW_SEED=true (the local
// compose file); production-like deployments refuse the call.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	"github.com/tickless/pkg/money"
)

func main() {
	email := flag.String("email", "", "email of the user to credit (required)")
	asset := flag.String("asset", "USDT", "asset symbol (BTC, ETH, USDT)")
	amount := flag.String("amount", "10000", "amount in whole units, e.g. 0.5")
	gateway := flag.String("gateway", env("E2E_GATEWAY", "http://localhost:18080"), "gateway URL")
	ledgerAddr := flag.String("ledger", env("E2E_LEDGER_GRPC", "127.0.0.1:9101"), "ledger gRPC address")
	flag.Parse()
	if *email == "" {
		fatal("-email is required")
	}
	a, ok := money.LookupAsset(strings.ToUpper(*asset))
	if !ok {
		fatal("unknown asset " + *asset)
	}
	units, err := a.Parse(*amount)
	if err != nil {
		fatal("invalid amount: " + err.Error())
	}

	token := adminLogin(*gateway)
	uid := findUser(*gateway, token, strings.ToLower(*email))

	conn, err := grpc.NewClient(*ledgerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err.Error())
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = ledgerv1.NewLedgerServiceClient(conn).AdminCredit(ctx, &ledgerv1.AdminCreditRequest{
		BusinessKey: fmt.Sprintf("devseed-%d-%s-%d", uid, a.Symbol, time.Now().UnixNano()),
		UserId:      uid, Asset: a.Symbol, Amount: &commonv1.Amount{Units: units.String()},
	})
	if err != nil {
		fatal("credit failed (is LEDGER_ALLOW_SEED=true?): " + err.Error())
	}
	fmt.Printf("credited %s %s to %s (user %d)\n", *amount, a.Symbol, *email, uid)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "seed:", msg)
	os.Exit(1)
}

func call(method, url, token string, body any, out any) int {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		fatal(err.Error())
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal("cannot reach the gateway: " + err.Error())
	}
	defer func() { _ = res.Body.Close() }()
	if out != nil {
		_ = json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode
}

func adminLogin(gw string) string {
	var tk struct {
		Access string `json:"access_token"`
	}
	st := call("POST", gw+"/v1/auth/login", "", map[string]string{
		"email": "admin@tickless.local", "password": env("TICKLESS_ADMIN_PASSWORD", "dev-admin-password")}, &tk)
	if st != 200 {
		fatal("admin login failed (status " + strconv.Itoa(st) + "); set TICKLESS_ADMIN_PASSWORD if you changed it")
	}
	return tk.Access
}

func findUser(gw, token, email string) int64 {
	var after int64
	for {
		var page struct {
			Users []struct {
				ID    string `json:"id"`
				Email string `json:"email"`
			} `json:"users"`
		}
		if st := call("GET", fmt.Sprintf("%s/v1/admin/users?after_id=%d&limit=200", gw, after), token, nil, &page); st != 200 {
			fatal("listing users failed, status " + strconv.Itoa(st))
		}
		if len(page.Users) == 0 {
			fatal("no user with email " + email)
		}
		for _, u := range page.Users {
			id, _ := strconv.ParseInt(u.ID, 10, 64)
			if strings.EqualFold(u.Email, email) {
				return id
			}
			after = id
		}
	}
}
