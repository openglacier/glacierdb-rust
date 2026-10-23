VERSION ?= dev
CODENAME ?= development

CARGO ?= cargo
PACKAGE_NAME := glacierdb-rust-$(VERSION).tar.gz

.PHONY: help version build test package clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[32m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

version: ## Show package version
	@echo "glacierdb-rust $(VERSION) ($(CODENAME))"

build: ## Build the crate
	$(CARGO) build --release

test: ## Run the tests (GLACIERDB_TEST_ADDR=host:port also runs the live test)
	$(CARGO) test

package: ## Create the crate archive (cargo package) and a source archive
	$(CARGO) package --allow-dirty --no-verify
	tar czf $(PACKAGE_NAME) Cargo.toml README.md src examples tests

clean: ## Remove build files
	$(CARGO) clean
	rm -f $(PACKAGE_NAME)
