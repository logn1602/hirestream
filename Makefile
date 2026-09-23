# HireStream — task runner. Targets are filled in by the tasks noted below (docs/PROGRESS.md).
.DEFAULT_GOAL := help
.PHONY: help setup fmt lint test e2e up down generate pipeline

PRESET ?= tiny

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

setup: ## Install the environment (pre-commit hooks arrive in T0.2)
	uv sync

fmt: ## Format code (T0.2)
	@echo "fmt: not implemented yet (T0.2)"

lint: ## Lint and type-check (T0.2)
	@echo "lint: not implemented yet (T0.2)"

test: ## Run tests (smoke import until pytest arrives in T0.2)
	uv run python -c "import hirestream; print('hirestream', hirestream.__version__)"

e2e: ## End-to-end pipeline at tiny (T2.15)
	@echo "e2e: not implemented yet (T2.15)"

up: ## Start the local stack (T0.3)
	@echo "up: not implemented yet (T0.3)"

down: ## Stop the local stack (T0.3)
	@echo "down: not implemented yet (T0.3)"

generate: ## Generate data: make generate PRESET=tiny|dev|full (T1.1)
	@echo "generate PRESET=$(PRESET): not implemented yet (T1.1)"

pipeline: ## Run the local pipeline: make pipeline PRESET=dev (T2.15)
	@echo "pipeline PRESET=$(PRESET): not implemented yet (T2.15)"
