# syntax=docker/dockerfile:1.7
# ^ enables BuildKit features (cache mounts, here-docs).

# ---------- Stage 1: builder ----------
# We use the official Go image to compile the binary.
# `alpine` keeps the builder small; the final image doesn't depend on it.
FROM golang:1.22-alpine AS builder

# BINARY is a build-time variable selecting which cmd/<name> to build.
# Pass with: docker build --build-arg BINARY=api .
ARG BINARY=api

# Install git — `go mod download` needs it for VCS-based dependencies.
RUN apk add --no-cache git

WORKDIR /src

# Copy go.mod / go.sum first so that `go mod download` is cached
# until those files actually change. This is the key Docker optimization
# for Go projects: dependencies usually change much less often than code.
COPY go.mod ./
# go.sum doesn't exist yet (Stage 0); it will appear once we add deps.
COPY go.su[m] ./

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/go/pkg/mod \
    go mod download

# Copy the rest of the source. This invalidates the cache only when
# source changes — but `go mod download` above is still cached.
COPY . .

# Build the requested binary.
# CGO_ENABLED=0 -> static binary (no libc dependency); required for distroless.
# -trimpath     -> remove machine-specific paths from the binary; reproducible.
# -ldflags '-s -w' -> strip debug symbols; smaller binary.
# -ldflags '-X main.binaryName=...' -> sets the binaryName var at build time.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -ldflags="-s -w -X main.binaryName=${BINARY}" \
        -o /out/app \
        ./cmd/${BINARY}

# ---------- Stage 2: runtime ----------
# Distroless: no shell, no package manager, ~2MB.
# `static-debian12` is the right one for CGO_ENABLED=0 binaries.
FROM gcr.io/distroless/static-debian12:nonroot

# Copy only the compiled binary. Nothing else from the builder image
# carries forward — no Go toolchain, no apk cache, no sources.
COPY --from=builder /out/app /app

# Run as a non-root user (UID 65532, provided by the :nonroot tag).
# Container security best practice.
USER nonroot:nonroot

# The default command. Override with `docker run <image> --some-flag`.
ENTRYPOINT ["/app"]
