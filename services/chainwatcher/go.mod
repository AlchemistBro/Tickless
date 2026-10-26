module github.com/tickless/chainwatcher

go 1.27.2

require (
	github.com/stretchr/testify v1.12.1
	github.com/tickless/pkg v0.0.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

replace github.com/tickless/pkg => ../../pkg
