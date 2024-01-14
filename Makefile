build:
	@go build -o bin/db cmd/main.go

run:
	@./bin/db

test:
	@go test -v ./...