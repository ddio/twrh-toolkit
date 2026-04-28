#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const os = require('os')
const parseArgs = require('command-line-args')
const CsvReadableStream = require('csv-reader')
const AutoDetectDecoderStream = require('autodetect-decoder-stream')
const { createArrayCsvStringifier } = require('csv-writer')
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads')

const HEADER = ['house_id', 'vendor_house_id', 'detail_dict', 'created', 'updated']
const COL = { detail_dict: 2, created: 3, updated: 4 }

// --- JSON path resolver ---
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
  const d = dateStr.slice(0, 10)
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

// --- Progress indicator ---
function createProgress (totalFiles) {
  const isTTY = process.stderr.isTTY
  const state = { rows: 0, matched: 0, filesDone: 0, totalFiles, startTime: Date.now() }
  let timer = null

  function format () {
    const elapsed = ((Date.now() - state.startTime) / 1000).toFixed(1)
    const parts = [`${state.rows.toLocaleString()} rows`]
    if (state.matched > 0) parts.push(`${state.matched.toLocaleString()} matched`)
    if (state.totalFiles > 1) parts.push(`${state.filesDone}/${state.totalFiles} files`)
    parts.push(`${elapsed}s`)
    return parts.join(' | ')
  }

  function render () {
    if (!isTTY) return
    process.stderr.write(`\r\x1b[KProcessing: ${format()}`)
  }

  function start () {
    if (timer) return
    timer = setInterval(render, 500)
  }

  function tick (rows, matched) {
    state.rows += rows
    state.matched += (matched || 0)
  }

  function fileDone () {
    state.filesDone++
  }

  function stop () {
    if (timer) { clearInterval(timer); timer = null }
    if (isTTY) process.stderr.write('\r\x1b[K')
    process.stderr.write(`Done: ${format()}\n`)
  }

  return { start, tick, fileDone, stop, state }
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
    { name: 'parallel', alias: 'j', type: Number },
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
    { name: 'parallel', alias: 'j', type: Number },
    { name: 'files', type: String, multiple: true, defaultOption: true, defaultValue: [] }
  ],
  split: [
    { name: 'rows', alias: 'n', type: Number },
    { name: 'output-dir', alias: 'o', type: String },
    { name: 'files', type: String, multiple: true, defaultOption: true, defaultValue: [] }
  ]
}

// --- Usage ---
function usage () {
  console.error(`Usage: detail-csv-cli.js <command> [options] FILE...

Commands:
  head   -n NUM                                    Show first N rows (default 10)
  count  --by created|updated [--after DATE] [--before DATE] [--group-by year|month|day] [-j NUM]
  query  --path JSONPATH --match STRING [--count] [--by created|updated] [--after DATE] [--before DATE] [--group-by year|month|day] [-j NUM]
  split  --rows N [--output-dir DIR]               Split a file into chunks of N rows each

Files ending in .gz are decompressed automatically.
Multiple files are processed in parallel (-j defaults to CPU count).`)
}

// --- Worker pool ---
function runWorkerPool (files, cmd, args, parallelism) {
  return new Promise((resolve, reject) => {
    const results = []
    const rows = []
    let fileIdx = 0
    let active = 0
    let totalRows = 0
    let totalMatched = 0
    let filesDone = 0

    const progress = createProgress(files.length)
    progress.start()

    function spawnNext () {
      if (fileIdx >= files.length) {
        if (active === 0) {
          progress.stop()
          resolve({ results, rows })
        }
        return
      }

      const file = files[fileIdx++]
      active++

      const worker = new Worker(__filename, {
        workerData: { file, cmd, args }
      })

      worker.on('message', (msg) => {
        if (msg.type === 'progress') {
          progress.tick(msg.rows, msg.matched)
        } else if (msg.type === 'result') {
          results.push(msg.counts)
        } else if (msg.type === 'row') {
          rows.push(msg.row)
        }
      })

      worker.on('exit', (code) => {
        active--
        progress.fileDone()
        if (code !== 0) {
          reject(new Error(`Worker exited with code ${code} for ${file}`))
          return
        }
        spawnNext()
      })

      worker.on('error', reject)
    }

    for (let i = 0; i < parallelism; i++) {
      spawnNext()
    }
  })
}

// --- Worker entry point ---
function workerMain () {
  const { file, cmd, args } = workerData

  if (cmd === 'count') {
    workerCount(file, args)
  } else if (cmd === 'query') {
    workerQuery(file, args)
  }
}

function workerCount (file, args) {
  const colIdx = COL[args.by]
  const after = args.after || null
  const before = args.before || null
  const groupBy = args['group-by'] || null
  const counts = { total: 0 }
  let batchRows = 0

  const csvStream = buildCsvStream(file)
  csvStream.on('data', (row) => {
    const dateStr = row[colIdx]
    if (after || before) {
      if (!inDateRange(dateStr, after, before)) {
        batchRows++
        if (batchRows >= 10000) {
          parentPort.postMessage({ type: 'progress', rows: batchRows, matched: 0 })
          batchRows = 0
        }
        return
      }
    }
    counts.total++
    batchRows++
    if (groupBy) {
      const key = getDateKey(dateStr, groupBy)
      if (key) counts[key] = (counts[key] || 0) + 1
    }
    if (batchRows >= 10000) {
      parentPort.postMessage({ type: 'progress', rows: batchRows, matched: 0 })
      batchRows = 0
    }
  })
  csvStream.on('end', () => {
    if (batchRows > 0) parentPort.postMessage({ type: 'progress', rows: batchRows, matched: 0 })
    parentPort.postMessage({ type: 'result', counts })
  })
  csvStream.on('error', (err) => {
    throw err
  })
}

function workerQuery (file, args) {
  const jsonPath = args.path
  const matchStr = args.match
  const doCount = args.count
  const byField = args.by || null
  const after = args.after || null
  const before = args.before || null
  const groupBy = args['group-by'] || null

  const counts = { total: 0 }
  let batchRows = 0
  let batchMatched = 0

  const csvStream = buildCsvStream(file)
  csvStream.on('data', (row) => {
    batchRows++

    if (byField && (after || before)) {
      const dateStr = row[COL[byField]]
      if (!inDateRange(dateStr, after, before)) {
        if (batchRows >= 10000) {
          parentPort.postMessage({ type: 'progress', rows: batchRows, matched: batchMatched })
          batchRows = 0; batchMatched = 0
        }
        return
      }
    }

    let detail
    try {
      detail = JSON.parse(row[COL.detail_dict])
    } catch (e) {
      if (batchRows >= 10000) {
        parentPort.postMessage({ type: 'progress', rows: batchRows, matched: batchMatched })
        batchRows = 0; batchMatched = 0
      }
      return
    }

    const values = resolveJsonPath(detail, jsonPath)
    const matched = values.some(v => {
      const s = typeof v === 'string' ? v : JSON.stringify(v)
      return s.includes(matchStr)
    })
    if (!matched) {
      if (batchRows >= 10000) {
        parentPort.postMessage({ type: 'progress', rows: batchRows, matched: batchMatched })
        batchRows = 0; batchMatched = 0
      }
      return
    }

    batchMatched++

    if (doCount) {
      counts.total++
      if (groupBy && byField) {
        const key = getDateKey(row[COL[byField]], groupBy)
        if (key) counts[key] = (counts[key] || 0) + 1
      }
    } else {
      parentPort.postMessage({ type: 'row', row: Array.from(row) })
    }

    if (batchRows >= 10000) {
      parentPort.postMessage({ type: 'progress', rows: batchRows, matched: batchMatched })
      batchRows = 0; batchMatched = 0
    }
  })
  csvStream.on('end', () => {
    if (batchRows > 0) parentPort.postMessage({ type: 'progress', rows: batchRows, matched: batchMatched })
    if (doCount) {
      parentPort.postMessage({ type: 'result', counts })
    }
  })
  csvStream.on('error', (err) => {
    throw err
  })
}

function mergeCounts (results) {
  const merged = { total: 0 }
  for (const counts of results) {
    for (const [key, val] of Object.entries(counts)) {
      merged[key] = (merged[key] || 0) + val
    }
  }
  return merged
}

// --- Main ---
if (!isMainThread) {
  workerMain()
} else {
  main().catch(err => {
    console.error(err.message)
    process.exit(1)
  })
}

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
  } else if (cmd === 'split') {
    await cmdSplit(files, args)
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

  const groupBy = args['group-by'] || null
  const parallelism = Math.min(args.parallel || os.cpus().length, files.length)

  if (files.length > 1 && parallelism > 1) {
    const { results } = await runWorkerPool(files, 'count', args, parallelism)
    const merged = mergeCounts(results)
    printCountTable(merged, groupBy)
    return
  }

  const colIdx = COL[byField]
  const after = args.after || null
  const before = args.before || null
  const counts = { total: 0 }
  const progress = createProgress(files.length)
  progress.start()

  for (const file of files) {
    const csvStream = buildCsvStream(file)

    await new Promise((resolve, reject) => {
      let batchRows = 0
      csvStream.on('data', (row) => {
        batchRows++
        const dateStr = row[colIdx]
        if (after || before) {
          if (!inDateRange(dateStr, after, before)) {
            if (batchRows >= 10000) { progress.tick(batchRows); batchRows = 0 }
            return
          }
        }
        counts.total++
        if (groupBy) {
          const key = getDateKey(dateStr, groupBy)
          if (key) counts[key] = (counts[key] || 0) + 1
        }
        if (batchRows >= 10000) { progress.tick(batchRows); batchRows = 0 }
      })
      csvStream.on('end', () => {
        if (batchRows > 0) progress.tick(batchRows)
        progress.fileDone()
        resolve()
      })
      csvStream.on('error', reject)
    })
  }

  progress.stop()
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

  const parallelism = Math.min(args.parallel || os.cpus().length, files.length)

  if (files.length > 1 && parallelism > 1) {
    const { results, rows } = await runWorkerPool(files, 'query', args, parallelism)
    if (doCount) {
      const merged = mergeCounts(results)
      printCountTable(merged, groupBy)
    } else {
      if (rows.length > 0) {
        console.log(HEADER.join(','))
        for (const row of rows) {
          console.log(csvStringify(row))
        }
      }
    }
    return
  }

  const counts = { total: 0 }
  let headerPrinted = false
  const progress = createProgress(files.length)
  progress.start()

  for (const file of files) {
    const csvStream = buildCsvStream(file)

    await new Promise((resolve, reject) => {
      let batchRows = 0
      let batchMatched = 0
      csvStream.on('data', (row) => {
        batchRows++

        if (byField && (after || before)) {
          const dateStr = row[COL[byField]]
          if (!inDateRange(dateStr, after, before)) {
            if (batchRows >= 10000) {
              progress.tick(batchRows, batchMatched); batchRows = 0; batchMatched = 0
            }
            return
          }
        }

        let detail
        try {
          detail = JSON.parse(row[COL.detail_dict])
        } catch (e) {
          if (batchRows >= 10000) {
            progress.tick(batchRows, batchMatched); batchRows = 0; batchMatched = 0
          }
          return
        }

        const values = resolveJsonPath(detail, jsonPath)
        const matched = values.some(v => {
          const s = typeof v === 'string' ? v : JSON.stringify(v)
          return s.includes(matchStr)
        })
        if (!matched) {
          if (batchRows >= 10000) {
            progress.tick(batchRows, batchMatched); batchRows = 0; batchMatched = 0
          }
          return
        }

        batchMatched++

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

        if (batchRows >= 10000) {
          progress.tick(batchRows, batchMatched); batchRows = 0; batchMatched = 0
        }
      })
      csvStream.on('end', () => {
        if (batchRows > 0) progress.tick(batchRows, batchMatched)
        progress.fileDone()
        resolve()
      })
      csvStream.on('error', reject)
    })
  }

  progress.stop()
  if (doCount) {
    printCountTable(counts, groupBy)
  }
}

// --- split command ---
async function cmdSplit (files, args) {
  const rowsPerChunk = args.rows
  if (!rowsPerChunk || rowsPerChunk <= 0) {
    console.error('Error: --rows N is required (positive integer)')
    process.exit(1)
  }

  for (const file of files) {
    const outputDir = args['output-dir'] || path.dirname(file)
    const basename = path.basename(file).replace(/\.csv(\.gz)?$/, '')
    let chunkIdx = 0
    let rowsInChunk = 0
    let totalRows = 0
    let gzStream = null
    let writeStream = null
    let pendingDrain = null

    const progress = createProgress(1)
    progress.start()

    function openChunk () {
      chunkIdx++
      const chunkName = `${basename}.part-${String(chunkIdx).padStart(3, '0')}.csv.gz`
      const chunkPath = path.join(outputDir, chunkName)
      writeStream = fs.createWriteStream(chunkPath)
      gzStream = zlib.createGzip()
      gzStream.pipe(writeStream)
      gzStream.write(HEADER.join(',') + '\n')
      rowsInChunk = 0
    }

    function closeChunk () {
      return new Promise((resolve) => {
        if (!gzStream) { resolve(); return }
        writeStream.on('finish', resolve)
        gzStream.end()
      })
    }

    const csvStream = buildCsvStream(file)

    await new Promise((resolve, reject) => {
      csvStream.on('data', (row) => {
        if (!gzStream || rowsInChunk >= rowsPerChunk) {
          if (gzStream) {
            csvStream.pause()
            closeChunk().then(() => {
              openChunk()
              writeRow(row)
              csvStream.resume()
            }).catch(reject)
            return
          }
          openChunk()
        }
        writeRow(row)
      })

      function writeRow (row) {
        const ok = gzStream.write(csvStringify(row) + '\n')
        rowsInChunk++
        totalRows++
        progress.tick(1)
        if (!ok && !pendingDrain) {
          csvStream.pause()
          pendingDrain = true
          gzStream.once('drain', () => {
            pendingDrain = false
            csvStream.resume()
          })
        }
      }

      csvStream.on('end', () => {
        closeChunk().then(resolve).catch(reject)
      })
      csvStream.on('error', reject)
    })

    progress.stop()
    process.stderr.write(`Split ${file}: ${totalRows.toLocaleString()} rows → ${chunkIdx} chunks in ${outputDir}/\n`)
  }
}
