# Grep for house

This toolkit provide grep-like functionality for [TWRH dataset](https://rentalhouse.g0v.ddio.io).

As text related field, including title and description of the rental house, is not release publicly.
Please run TWRH crawler yourself, so to download required field.

## Example usage

```bash
# grep PATTERNS in given FILEs
node ./hgrep.js PATTERNS [FILE...]
```

```bash
# support gzip
node ./hgrep.js --gzip PATTERNS [FILE...]
```
