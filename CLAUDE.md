# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a toolkit for [TWRH (Taiwan Rental House Watch)](https://rentalhouse.g0v.ddio.io) — a collection of CLI utilities for searching and filtering rental house datasets. The project is part of the g0v civic tech community.

## Repository Structure

- `house-grep/` — Main tool directory containing Node.js CLI scripts
  - `hgrep.js` — Grep-like search over TWRH CSV datasets (matches patterns against title/description fields)
  - `filter-dataset.js` — Filters full dataset CSVs to only rows matching IDs from hgrep output

## Commands

```bash
# Install dependencies
cd house-grep && npm install

# Search for patterns in dataset files (supports gzip)
node house-grep/hgrep.js --gzip -p PATTERN [FILE...]

# Filter dataset by ID list (from hgrep output)
node house-grep/filter-dataset.js -d DATASET_CSV... -o OUTPUT_DIR ID_FILE...
```

No test suite, linter, or build step is configured.

## Key Details

- Dataset CSV files are not public — text fields (title, description) must be obtained by running the TWRH crawler yourself
- Raw dataset files live in `house-grep/raw/` (deduplicated CSVs by month/quarter)
- The CSV column at index `[2]` contains JSON-encoded detail objects; `hgrep.js` parses `detail.title` and `detail.remark.content` (or legacy `detail.desp`) for matching
- Both scripts use `command-line-args` for CLI parsing and `csv-reader`/`csv-writer` for CSV I/O with `autodetect-decoder-stream` for encoding detection
- The project uses CommonJS (`require`) — not ES modules
