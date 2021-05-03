LOCAL_BIN=$(CURDIR)/bin

check:
	@${HOME}/go/bin/golangci-lint run

build:
	@mkdir -p ${LOCAL_BIN}
	@go build -o ${LOCAL_BIN}/querier $(CURDIR)/cmd/querier 
