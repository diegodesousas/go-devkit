# Toolchain image for the `dev` service in compose.yaml. Go, gofmt and
# golangci-lint run here instead of on the host; git and gh stay on the host.
#
# Bumping Go? The version also lives in .github/workflows/ci.yml and lint.yml.
FROM golang:1.27

ARG GOLANGCI_LINT_VERSION=v2.14.0

# confluent-kafka-go v1.9.2 bundles an x86-64 librdkafka only, which does not
# link on arm64. Linking the system library (build tag `dynamic`, set through
# GOFLAGS below) keeps the image native on both architectures.
# jq parses the Claude Code hook payload in .claude/hooks/gofmt.sh.
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev jq \
    && rm -rf /var/lib/apt/lists/*

# Built from source so golangci-lint is always compiled with the image's Go,
# which it requires to lint a module targeting that version.
RUN go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION} \
    && go clean -cache -modcache

ENV CGO_ENABLED=1 \
    GOFLAGS=-tags=dynamic

# The bind-mounted repo is not owned by root, which makes git (and go's VCS
# stamping) refuse it without this.
RUN git config --system --add safe.directory /workspace

WORKDIR /workspace
