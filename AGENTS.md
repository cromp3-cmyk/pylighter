# Repository Guidelines

## Project Structure & Module Organization
Core SDK code lives in `pylighter/` (async `Lighter` client, HTTP adapter, and market/order managers). Strategies and entry points such as `grid_strategy.py`, `cross_exchange_arbitrage.py`, and `main.py` sit in the repository root; treat `grid_strategy.py` as the reference workflow. Reusable helpers are in `utils/` (notably `logger_config`), example scripts in `examples/`, docs in `docs/`, and runtime logs default to `log/`. Keep credentials in a local `.env`; never commit secrets and clean generated artifacts from `log/`.

## Build, Test, and Development Commands
Install and sync dependencies with `uv sync`. Use `uv run` for every script: `uv run main.py` to sanity-check API wiring, `uv run grid_strategy.py --dry-run --symbol TON` for safe strategy validation, and `uv run examples/create_market_order.py` for targeted flows. Before shipping trading changes, tail `log/grid_strategy.log` to confirm threshold handling.

## Coding Style & Naming Conventions
Stick to Python 3.13 and PEP 8: four-space indentation, snake_case functions, UpperCamelCase classes. Existing modules mix synchronous helpers and async workflows; prefer async/await and type hints when touching the SDK. Centralize logging through `utils.logger_config.get_strategy_logger` rather than ad hoc `print`, and keep inline comments purposeful. Preserve existing bilingual docstrings when editing shared modules.

## Testing Guidelines
There is no dedicated test suite yet; add new coverage under `tests/` with `test_*.py` modules. Target `pytest` (invoke as `uv run pytest`) and use `pytest.mark.asyncio` for coroutine-heavy code with the `Lighter` client. For strategy tweaks, capture dry-run transcripts such as `uv run grid_strategy.py --dry-run --max-orders 5` and attach key log excerpts in the PR. Avoid hitting live endpoints in automated tests by mocking HTTP or WebSocket layers, or exercising managers with stub responses.

## Commit & Pull Request Guidelines
Follow the existing history: single-sentence, Title Case imperatives such as "Optimize grid strategy parameters and replace account tier utility"; keep them under about 72 characters and group related changes. Each PR should explain strategy impact, list validation commands with outcomes, and reference issues when available. Include screenshots or log snippets if behavior changes, and call out live-trading risks plus required `.env` variables in the description.

## Security & Configuration Tips
Always load credentials from `.env` (`LIGHTER_KEY`, `LIGHTER_SECRET`, optional `API_KEY_INDEX`) and confirm `.gitignore` keeps the file out of commits. When testing against mainnet, double-check leverage and order sizing constants in `grid_strategy.py` and document any default adjustments. Rotate API keys on suspicion of leaks and avoid committing responses or logs containing wallet addresses or order identifiers.
