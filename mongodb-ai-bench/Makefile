.PHONY: build run test clean lint quick-test full-run

BINARY=mongodb-ai-bench
BUILD_DIR=bin
CONFIGS_DIR=configs

build:
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/bench

run: build
	./$(BUILD_DIR)/$(BINARY) -config $(CONFIGS_DIR)/default.yaml

quick-test: build
	./$(BUILD_DIR)/$(BINARY) -config $(CONFIGS_DIR)/quick-test.yaml

full-run: build
	./$(BUILD_DIR)/$(BINARY) -config $(CONFIGS_DIR)/full-run.yaml

test:
	go test ./... -v -race -count=1

lint:
	golangci-lint run ./...

clean:
	rm -rf $(BUILD_DIR) results/

deps:
	go mod tidy

cross-linux: 
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY)-linux-amd64 ./cmd/bench
