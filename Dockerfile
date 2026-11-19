# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/etcdflake ./cmd/etcdflake

# distroless/static has CA certificates and runs as a non-root user.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/etcdflake /etcdflake
USER nonroot:nonroot
EXPOSE 7669 8001
ENTRYPOINT ["/etcdflake"]
