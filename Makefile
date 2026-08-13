.PHONY: test test-integration test-all fmt lint next-version release

BUMP ?= patch
VERSION ?=

test:
	go test ./pkg/... -race

test-integration:
	go test ./pkg/database/sql/... -race -tags=integration -count=1

test-all: test test-integration

fmt:
	gofmt -w ./pkg ./examples

lint:
	gofmt -l ./pkg ./examples
	go vet ./...
	golangci-lint run --timeout=5m

next-version:
	@version="$(VERSION)"; \
	if [ -n "$$version" ]; then \
		echo "$$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$$' || \
			{ echo "next-version: VERSION must look like v1.2.3 (got '$$version')" >&2; exit 1; }; \
	else \
		case "$(BUMP)" in \
			major|minor|patch) ;; \
			*) echo "next-version: BUMP must be major, minor or patch (got '$(BUMP)')" >&2; exit 1;; \
		esac; \
		current=$$(git tag -l 'v*' --sort=-v:refname | head -n1); \
		current=$${current:-v0.0.0}; \
		number=$${current#v}; \
		major=$$(echo "$$number" | cut -d. -f1); \
		minor=$$(echo "$$number" | cut -d. -f2); \
		patch=$$(echo "$$number" | cut -d. -f3); \
		case "$(BUMP)" in \
			major) version="v$$((major + 1)).0.0";; \
			minor) version="v$$major.$$((minor + 1)).0";; \
			patch) version="v$$major.$$minor.$$((patch + 1))";; \
		esac; \
	fi; \
	echo "$$version"

release:
	@set -e; \
	version=$$($(MAKE) --no-print-directory next-version) || exit 1; \
	branch=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$branch" != "main" ]; then \
		echo "release: must run on main (currently on $$branch)" >&2; exit 1; \
	fi; \
	if [ -n "$$(git status --porcelain)" ]; then \
		echo "release: working tree is not clean" >&2; exit 1; \
	fi; \
	git fetch --quiet --tags origin; \
	if [ "$$(git rev-parse HEAD)" != "$$(git rev-parse origin/main)" ]; then \
		echo "release: main is out of sync with origin/main" >&2; exit 1; \
	fi; \
	if git rev-parse -q --verify "refs/tags/$$version" >/dev/null || \
		git ls-remote --exit-code --tags origin "$$version" >/dev/null 2>&1; then \
		echo "release: tag $$version already exists" >&2; exit 1; \
	fi; \
	echo "release: cutting $$version"; \
	$(MAKE) test; \
	$(MAKE) lint; \
	git tag -a "$$version" -m "release $$version"; \
	git push origin "$$version"; \
	gh release create "$$version" --title "$$version" --generate-notes; \
	echo "release: published $$version"
