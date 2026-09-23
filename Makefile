# HireStream — task runner. Targets are filled in by the tasks noted below (docs/PROGRESS.md).
.DEFAULT_GOAL := help
.PHONY: help setup fmt lint test e2e up down generate pipeline

PRESET ?= tiny

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

setup: ## Install the environment and git hooks
	uv sync
	uv run pre-commit install

fmt: ## Format code and apply safe lint fixes
	uv run ruff format .
	uv run ruff check --fix .

lint: ## Lint, check formatting, and type-check
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

test: ## Run fast tests with a coverage report (gate arrives in T2.15)
	uv run pytest -m "not slow" --cov --cov-report=term-missing --cov-report=xml

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
