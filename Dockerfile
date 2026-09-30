# Build stage: static binary, no cgo, reproducible flags.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/etl ./cmd/etl

# Runtime stage: distroless, non-root, nothing but the binary and the registry.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/etl /app/etl
COPY reference/trustwallet_assets.csv /app/trustwallet_assets.csv
COPY reference/symbol_map.csv /app/symbol_map.csv

ENV ETL_REGISTRY_PATH=/app/trustwallet_assets.csv \
    ETL_SYMBOL_MAP_PATH=/app/symbol_map.csv \
    ETL_DATA_DIR=/app/data \
    ETL_LOG_PATH=/app/logs/etl.log \
    ETL_HTTP_ADDR=:8080

# data/ and logs/ are the persistent volumes; mount them from the host.
VOLUME ["/app/data", "/app/logs"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/etl"]
