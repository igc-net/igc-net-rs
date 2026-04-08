.PHONY: help build test itest vet docs clean

help: ## List all available targets with descriptions
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

build: ## Build all workspace crates
	cargo build --workspace

test: ## Run all workspace tests
	cargo test --workspace

itest: ## Run workspace integration test targets only (files under */tests; ignored tests excluded)
	@set -e; \
	for test_file in $$(find . -path './*/tests/*.rs' | sort); do \
		crate=$$(echo "$$test_file" | awk -F/ '{print $$2}'); \
		test_name=$$(basename "$$test_file" .rs); \
		echo "==> cargo test -p $$crate --test $$test_name"; \
		cargo test -p "$$crate" --test "$$test_name"; \
	done

vet: ## Run clippy (warnings as errors) and check formatting across all crates
	cargo clippy --workspace --all-targets -- -D warnings
	cargo fmt --all --check

docs: ## Generate API documentation for all public crates (set OPEN=1 to open in browser)
	cargo doc --no-deps --workspace
	@[ "$(OPEN)" = "1" ] && cargo doc --no-deps --workspace --open || true

clean: ## Remove all generated build artefacts
	cargo clean
