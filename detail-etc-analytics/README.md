# detail-csv-cli

CLI tool for searching and counting rows in TWRH `detail_dict` CSV datasets. Supports `.gz` compressed files and parallel processing of multiple files via `worker_threads`.

## Install

```bash
npm install
```

## Usage

```
detail-csv-cli.js <command> [options] FILE...
```

### `head` — Preview rows

```bash
node detail-csv-cli.js head -n 5 house_etc_full.csv.gz
```

| Option | Description |
|--------|-------------|
| `-n NUM` | Number of rows to show (default: 10) |

### `count` — Count rows by date

```bash
# Total rows created in 2024
node detail-csv-cli.js count --by created --after 2024-01-01 --before 2024-12-31 house_etc_full.csv.gz

# Count by month
node detail-csv-cli.js count --by updated --group-by month house_etc_full.csv.gz
```

| Option | Description |
|--------|-------------|
| `--by created\|updated` | **(required)** Which date column to use |
| `--after DATE` | Include only rows on or after this date (YYYY-MM-DD) |
| `--before DATE` | Include only rows on or before this date (YYYY-MM-DD) |
| `--group-by year\|month\|day` | Group counts by time period |
| `-j NUM` | Number of parallel workers (default: CPU count). Applies when multiple files are given |

### `query` — Search inside `detail_dict` JSON

```bash
# Find rows where title contains "套房"
node detail-csv-cli.js query --path title --match 套房 house_etc_full.csv.gz

# Count matches grouped by month
node detail-csv-cli.js query --path title --match 套房 --count --by created --group-by month house_etc_full.csv.gz

# Search nested fields (remark.content) or array fields (tags[].value)
node detail-csv-cli.js query --path remark.content --match 寵物 house_etc_full.csv.gz
node detail-csv-cli.js query --path tags[].value --match 可養寵 house_etc_full.csv.gz
```

| Option | Description |
|--------|-------------|
| `--path JSONPATH` | **(required)** JSON path to search (e.g. `title`, `remark.content`, `tags[].value`) |
| `--match STRING` | **(required)** Substring to match against resolved values |
| `--count` | Output a count table instead of matching rows |
| `--by created\|updated` | Date column for `--group-by` and date range filtering |
| `--after DATE` | Include only rows on or after this date |
| `--before DATE` | Include only rows on or before this date |
| `--group-by year\|month\|day` | Group counts by time period (requires `--by`) |
| `-j NUM` | Number of parallel workers (default: CPU count). Applies when multiple files are given |

### `split` — Split a large file into chunks

```bash
# Split into 500,000-row chunks
node detail-csv-cli.js split --rows 500000 house_etc_full.csv.gz

# Specify output directory
node detail-csv-cli.js split --rows 500000 --output-dir ./chunks house_etc_full.csv.gz
```

Output files are named `<basename>.part-001.csv.gz`, `<basename>.part-002.csv.gz`, etc. Each chunk includes a header row.

| Option | Description |
|--------|-------------|
| `--rows N`, `-n N` | **(required)** Number of rows per chunk |
| `--output-dir DIR`, `-o DIR` | Output directory (default: same as input file) |

### Parallel Processing

When multiple files are passed to `count` or `query`, they are processed in parallel using `worker_threads`. Use `split` first to break a large file into chunks, then run queries on the chunks:

```bash
# Split once
node detail-csv-cli.js split --rows 500000 house_etc_full.csv.gz

# Query in parallel across chunks
node detail-csv-cli.js count --by created --group-by month house_etc_full.part-*.csv.gz
node detail-csv-cli.js query --path title --match 套房 --count --by created house_etc_full.part-*.csv.gz
```

All commands show a progress indicator on stderr with row count, elapsed time, and file progress.

## JSON Path Syntax

The `--path` option supports dot-separated keys with `[]` for array expansion:

- `title` — top-level key
- `remark.content` — nested object
- `tags[].value` — iterate array elements, then access `.value` on each
- `info[].name` — same pattern for any array field

## CSV Format

Input CSVs are expected to have these columns (no header row):

| Index | Column |
|-------|--------|
| 0 | `house_id` |
| 1 | `vendor_house_id` |
| 2 | `detail_dict` (JSON string) |
| 3 | `created` |
| 4 | `updated` |
