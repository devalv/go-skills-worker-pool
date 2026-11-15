.PHONY: fmt lint test test-simple cover cover-simple run-simple

fmt:
	go mod tidy
	go fix -diff ./...
	golangci-lint run --fix ./...

lint:
	golangci-lint run ./...

test: test-simple

test-simple:
	go test ./simple -race -timeout=5m -v -count=10

cover: cover-simple

cover-simple:
	go test ./simple -coverprofile=coverage.out
	go tool cover -func=coverage.out

run-simple:
	go run ./examples/simple
