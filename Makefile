.PHONY: sync lint format test hooks hooks-run

sync:
	uv sync

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest -q

hooks:
	uv run pre-commit install
	uv run pre-commit install --hook-type pre-push

hooks-run:
	uv run pre-commit run --all-files
