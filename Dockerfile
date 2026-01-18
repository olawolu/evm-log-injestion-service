# --- build stage
FROM golang:1.25.5-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o tx_watcher ./cmd/listener

# --- runtime stage
FROM gcr.io/distroless/base-debian12:debug
WORKDIR /app
COPY --from=build /app/tx_watcher /app/tx_watcher
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/tx_watcher"]
