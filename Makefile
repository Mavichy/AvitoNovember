.PHONY: build run docker-up docker-down lint

build:
	go build -o bin/server ./cmd/server

run:
	HTTP_PORT=8080 DB_DSN="postgres://pr_user:pr_password@localhost:5432/pr_db?sslmode=disable" \
	go run ./cmd/server

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

lint:
	golangci-lint run ./...