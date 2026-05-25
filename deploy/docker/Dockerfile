# deploy/Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/server ./cmd/server/

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /bin/server /server
ENTRYPOINT ["/server"]
