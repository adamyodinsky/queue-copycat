docker-build-local:
	docker build -t queue-copycat-go .

test:
	go test -v

compose-up:
	docker compose up -d

compose-down:
	docker compose down
