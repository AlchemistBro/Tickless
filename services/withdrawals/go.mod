module github.com/tickless/withdrawals

go 1.27.2

replace github.com/tickless/pkg => ../../pkg

require (
	github.com/btcsuite/btcd v0.24.2
	github.com/btcsuite/btcd/btcutil v1.2.0
	github.com/ethereum/go-ethereum v1.17.10
	github.com/stretchr/testify v1.12.1
	github.com/tickless/pkg v0.0.0-00010101000000-000000000000
)

require (
	github.com/btcsuite/btcd/btcec/v2 v2.3.5 // indirect
	github.com/btcsuite/btcd/chaincfg/chainhash v1.1.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/kcalvinalvin/anet v0.0.0-20251112173137-d8ddc1f6dbee // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
