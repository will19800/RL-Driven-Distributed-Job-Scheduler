SHELL := /bin/sh

GO ?= go
PYTHON ?= python3
GOFMT ?= gofmt

PROJECT_ROOT := $(CURDIR)
BUILD_DIR := $(PROJECT_ROOT)/bin
GOCACHE ?= $(PROJECT_ROOT)/.cache/go-build
GOMODCACHE ?= $(PROJECT_ROOT)/.cache/go-mod
PYTHONPATH := $(PROJECT_ROOT)/python

export GOCACHE
export GOMODCACHE
export PYTHONPATH

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check docs-check lint test test-go test-python build \
	train-smoke benchmark-smoke clean

help: ## Show available development commands.
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format Go source files.
	$(GOFMT) -w cmd internal

fmt-check: ## Fail if Go source files need formatting.
	@files="$$($(GOFMT) -l cmd internal)"; \
	if [ -n "$$files" ]; then \
		echo "Go files need formatting:"; \
		echo "$$files"; \
		exit 1; \
	fi

docs-check: ## Validate Markdown formatting and local links.
	$(PYTHON) scripts/check_docs.py

lint: fmt-check docs-check ## Run dependency-free static checks.
	$(GO) vet ./...
	$(GO) mod tidy -diff
	$(PYTHON) -m compileall -q python/scheduler_rl python/tests
	$(PYTHON) -c 'import pathlib, tomllib; tomllib.loads(pathlib.Path("python/pyproject.toml").read_text())'

test: test-go test-python ## Run all unit tests.

test-go: ## Run Go unit tests.
	$(GO) test ./...

test-python: ## Run Python unit tests.
	$(PYTHON) -m unittest discover -s python/tests -v

build: ## Build both Go command-line programs.
	mkdir -p "$(BUILD_DIR)"
	$(GO) build -o "$(BUILD_DIR)/scheduler-sim" ./cmd/scheduler-sim
	$(GO) build -o "$(BUILD_DIR)/result-check" ./cmd/result-check

train-smoke: ## Check that the Python training package starts.
	$(PYTHON) -m scheduler_rl

benchmark-smoke: ## Check that the simulator command starts.
	$(GO) run ./cmd/scheduler-sim

clean: ## Remove generated local build and cache artifacts.
	$(GO) clean -modcache
	rm -rf "$(BUILD_DIR)" "$(PROJECT_ROOT)/.cache"
	find python -type d -name __pycache__ -prune -exec rm -rf {} +
