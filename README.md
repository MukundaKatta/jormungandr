# jormungandr — Data Pipeline Framework. End-to-end data pipeline framework

*Jörmungandr — the world serpent of Norse myth.*

jormungandr takes its name in that spirit. Data Pipeline Framework. End-to-end data pipeline framework.

## Why jormungandr

jormungandr exists to make this workflow practical. Data pipeline framework. end-to-end data pipeline framework. It favours a small, inspectable surface over sprawling configuration.

## Features

- `StageStatus` — exported from `src/jormungandr/core.py`
- `StageResult` — exported from `src/jormungandr/core.py`
- Included test suite

## Tech Stack

- **Runtime:** Python

## How It Works

The codebase is organised into `src/`, `tests/`. The primary entry points are `src/jormungandr/core.py`, `src/jormungandr/__init__.py`. `src/jormungandr/core.py` exposes `StageStatus`, `StageResult` — the core types that drive the behaviour.

## Getting Started

```bash
pip install -e .
```

## Usage

```python
from jormungandr.core import StageStatus

instance = StageStatus()
# See the source for the full API
```

## Project Structure

```
jormungandr/
├── CLAUDE.md
├── LICENSE
├── README.md
├── index.html
├── pyproject.toml
├── src/
├── tests/
```
