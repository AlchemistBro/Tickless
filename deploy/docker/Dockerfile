# One Dockerfile for every Go service: docker build --build-arg SERVICE=ledger -f deploy/docker/Dockerfile .
# Build context is the repository root (go.work + replace directives need pkg/).
FROM golang:1.27 AS build
ARG SERVICE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd services/${SERVICE} && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}
RUN mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
# /data is the (volume-backed) working area of the matching service checkpoints.
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
ENTRYPOINT ["/app"]
