FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/processor ./cmd/processor

FROM gcr.io/distroless/static-debian12:nonroot AS api

COPY --from=builder /out/api /api

EXPOSE 8080

ENTRYPOINT ["/api"]

FROM gcr.io/distroless/static-debian12:nonroot AS processor

COPY --from=builder /out/processor /processor

ENTRYPOINT ["/processor"]