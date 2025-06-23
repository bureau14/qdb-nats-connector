module github.com/bureau14/qdb-nats-connector

go 1.24

require (
	github.com/bureau14/qdb-api-go/v3 v3.14.1
	github.com/nats-io/nats.go v1.41.0
)

// For development purposes only
replace github.com/bureau14/qdb-api-go/v3 => ../qdb-api-go

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.9 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	pgregory.net/rapid v1.2.0 // indirect
)
