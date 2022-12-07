# Grep for house

This toolkit provide grep-like functionality for [TWRH dataset](https://rentalhouse.g0v.ddio.io).

As text related field, including title and description of the rental house, is not release publicly.
Please run TWRH crawler yourself, so to download required field.

## Example usage

```bash
# grep PATTERNS in given FILEs
node ./hgrep.js -p PATTERNS [FILE...]
```

```bash
# support gzip
node ./hgrep.js --gzip -p PATTERNS [FILE...]
```

```bash
# trial run, get first 10 matched line
# due to internal implementation, #result may still larger than given number
node ./hgrep.js --gzip -p PATTERNS -l 10 [FILE...]
```

```bash
# match multiple pattern (or)
node ./hgrep.js --gzip -p 社會住宅 -p 包租代管 [FILE...]
```
