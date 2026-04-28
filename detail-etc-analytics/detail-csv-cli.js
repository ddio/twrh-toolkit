#!/usr/bin/env node

const fs = require('fs')
const zlib = require('zlib')
const { finished } = require('stream')
const parseArgs = require('command-line-args')
const CsvReadableStream = require('csv-reader')
const AutoDetectDecoderStream = require('autodetect-decoder-stream')
const { createArrayCsvStringifier } = require('csv-writer')

const HEADER = ['house_id', 'vendor_house_id', 'detail_dict', 'created', 'updated']
const COL = { detail_dict: 2, created: 3, updated: 4 }

// --- JSON path resolver ---
// Supports: "title", "remark.content", "tags[].value", "info[].name"
function resolveJsonPath (obj, path) {
  const segments = []
  for (const part of path.split('.')) {
    const m = part.match(/^(.+)\[\]$/)
    if (m) {
      segments.push({ key: m[1], wildcard: true })
    } else {
      segments.push({ key: part, wildcard: false })
    }
  }

  let current = [obj]
  for (const seg of segments) {
    const next = []
    for (const node of current) {
      if (node == null) continue
      const val = node[seg.key]
      if (val == null) continue
      if (seg.wildcard) {
        if (Array.isArray(val)) {
          next.push(...val)
        }
      } else {
        next.push(val)
      }
    }
    current = next
  }

  return current
}

// --- Date helpers ---
function getDateKey (dateStr, groupBy) {
  if (!dateStr) return null
  const d = dateStr.slice(0, 10) // "2022-01-18"
  if (groupBy === 'year') return d.slice(0, 4)
  if (groupBy === 'month') return d.slice(0, 7)
  if (groupBy === 'day') return d
  return null
}

function inDateRange (dateStr, after, before) {
  if (!dateStr) return false
  const d = dateStr.slice(0, 10)
  if (after && d < after) return false
  if (before && d > before) return false
  return true
}

// --- Table formatter ---
function printCountTable (counts, groupBy) {
  if (!groupBy) {
    console.log(`Total: ${counts.total}`)
    return
  }

  const entries = Object.entries(counts)
    .filter(([k]) => k !== 'total')
    .sort(([a], [b]) => a.localeCompare(b))

  if (entries.length === 0) {
    console.log('No matching rows.')
    return
  }

  const maxKeyLen = Math.max(...entries.map(([k]) => k.length), groupBy.length)
  const maxValLen = Math.max(...entries.map(([, v]) => String(v).length), 5)

  console.log(`${groupBy.padEnd(maxKeyLen)}  ${'count'.padStart(maxValLen)}`)
  console.log(`${'─'.repeat(maxKeyLen)}  ${'─'.repeat(maxValLen)}`)
  for (const [key, val] of entries) {
    console.log(`${key.padEnd(maxKeyLen)}  ${String(val).padStart(maxValLen)}`)
  }
  console.log(`${'─'.repeat(maxKeyLen)}  ${'─'.repeat(maxValLen)}`)
  console.log(`${'total'.padEnd(maxKeyLen)}  ${String(counts.total).padStart(maxValLen)}`)
}

// --- CSV output helper ---
function csvStringify (row) {
  const stringifier = createArrayCsvStringifier({ header: [] })
  return stringifier.stringifyRecords([row]).trimEnd()
}

// --- Stream builder ---
function buildCsvStream (file) {
  let readStream = fs.createReadStream(file)
  if (file.endsWith('.gz')) {
    readStream = readStream.pipe(zlib.createGunzip())
  }
  return readStream
    .pipe(new AutoDetectDecoderStream())
    .pipe(new CsvReadableStream({ skipHeader: true }))
}

// --- Subcommand definitions ---
const SUBCMDS = {
  head: [
    { name: 'num', alias: 'n', type: Number, defaultValue: 10 },
    { name: 'files', type: String, multiple: true, defaultOption: true, defaultValue: [] }
  ],
  count: [
    { name: 'by', type: String },
    { name: 'after', type: String },
    { name: 'before', type: String },
    { name: 'group-by', type: String },
    { name: 'files', type: String, multiple: true, defaultOption: true, defaultValue: [] }
  ],
  query: [
    { name: 'path', type: String },
    { name: 'match', type: String },
    { name: 'count', type: Boolean, defaultValue: false },
    { name: 'by', type: String },
    { name: 'after', type: String },
    { name: 'before', type: String },
    { name: 'group-by', type: String },
    { name: 'files', type: String, multiple: true, defaultOption: true, defaultValue: [] }
  ]
}

// --- Usage ---
function usage () {
  console.error(`Usage: detail-csv-cli.js <command> [options] FILE...

Commands:
  head   -n NUM                                    Show first N rows (default 10)
  count  --by created|updated [--after DATE] [--before DATE] [--group-by year|month|day]
  query  --path JSONPATH --match STRING [--count] [--by created|updated] [--after DATE] [--before DATE] [--group-by year|month|day]

Files ending in .gz are decompressed automatically.`)
}

// --- Main ---
async function main () {
  const mainArgs = parseArgs(
    [{ name: 'command', defaultOption: true }],
    { stopAtFirstUnknown: true }
  )

  const cmd = mainArgs.command
  if (!cmd || !SUBCMDS[cmd]) {
    usage()
    process.exit(1)
  }

  const argv = mainArgs._unknown || []
  const args = parseArgs(SUBCMDS[cmd], { argv })
  const files = args.files

  if (files.length === 0) {
    console.error('Error: no input files specified')
    process.exit(1)
  }

  if (cmd === 'head') {
    await cmdHead(files, args.num)
  } else if (cmd === 'count') {
    await cmdCount(files, args)
  } else if (cmd === 'query') {
    await cmdQuery(files, args)
  }
}

// --- head command ---
async function cmdHead (files, n) {
  console.log(HEADER.join(','))
  let remaining = n

  for (const file of files) {
    if (remaining <= 0) break
    const csvStream = buildCsvStream(file)

    await new Promise((resolve, reject) => {
      csvStream.on('data', (row) => {
        if (remaining <= 0) {
          csvStream.destroy()
          return
        }
        console.log(csvStringify(row))
        remaining--
        if (remaining <= 0) {
          csvStream.destroy()
        }
      })
      csvStream.on('end', resolve)
      csvStream.on('close', resolve)
      csvStream.on('error', reject)
    })
  }
}

// --- count command ---
async function cmdCount (files, args) {
  const byField = args.by
  if (!byField || !COL[byField]) {
    console.error('Error: --by must be "created" or "updated"')
    process.exit(1)
  }

  const colIdx = COL[byField]
  const after = args.after || null
  const before = args.before || null
  const groupBy = args['group-by'] || null
  const counts = { total: 0 }

  for (const file of files) {
    const csvStream = buildCsvStream(file)

    await new Promise((resolve, reject) => {
      csvStream.on('data', (row) => {
        const dateStr = row[colIdx]
        if (after || before) {
          if (!inDateRange(dateStr, after, before)) return
        }
        counts.total++
        if (groupBy) {
          const key = getDateKey(dateStr, groupBy)
          if (key) counts[key] = (counts[key] || 0) + 1
        }
      })
      csvStream.on('end', resolve)
      csvStream.on('error', reject)
    })
  }

  printCountTable(counts, groupBy)
}

// --- query command ---
async function cmdQuery (files, args) {
  if (!args.path) {
    console.error('Error: --path is required')
    process.exit(1)
  }
  if (!args.match) {
    console.error('Error: --match is required')
    process.exit(1)
  }

  const jsonPath = args.path
  const matchStr = args.match
  const doCount = args.count
  const byField = args.by || null
  const after = args.after || null
  const before = args.before || null
  const groupBy = args['group-by'] || null

  if (doCount && groupBy && !byField) {
    console.error('Error: --group-by requires --by')
    process.exit(1)
  }
  if (byField && !COL[byField]) {
    console.error('Error: --by must be "created" or "updated"')
    process.exit(1)
  }

  const counts = { total: 0 }
  let headerPrinted = false

  for (const file of files) {
    const csvStream = buildCsvStream(file)

    await new Promise((resolve, reject) => {
      csvStream.on('data', (row) => {
        // Date range filter (applies to both count and raw output modes)
        if (byField && (after || before)) {
          const dateStr = row[COL[byField]]
          if (!inDateRange(dateStr, after, before)) return
        }

        // JSON path match
        let detail
        try {
          detail = JSON.parse(row[COL.detail_dict])
        } catch (e) {
          return
        }

        const values = resolveJsonPath(detail, jsonPath)
        const matched = values.some(v => {
          const s = typeof v === 'string' ? v : JSON.stringify(v)
          return s.includes(matchStr)
        })
        if (!matched) return

        if (doCount) {
          counts.total++
          if (groupBy && byField) {
            const key = getDateKey(row[COL[byField]], groupBy)
            if (key) counts[key] = (counts[key] || 0) + 1
          }
        } else {
          if (!headerPrinted) {
            console.log(HEADER.join(','))
            headerPrinted = true
          }
          console.log(csvStringify(row))
        }
      })
      csvStream.on('end', resolve)
      csvStream.on('error', reject)
    })
  }

  if (doCount) {
    printCountTable(counts, groupBy)
  }
}

main().catch(err => {
  console.error(err.message)
  process.exit(1)
})
