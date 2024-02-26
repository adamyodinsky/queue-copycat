FROM golang:1.22.0-alpine as builder

COPY go.mod go.sum main.go /app/ 
WORKDIR /app


RUN go build

FROM builder as final

COPY --from=builder /app/queue-copycat /app/queue-copycat
WORKDIR /app
