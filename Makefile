run: build
	@./bin/db

build:
	@go build -o bin/db cmd/main.go

test:
	@go test -v ./...