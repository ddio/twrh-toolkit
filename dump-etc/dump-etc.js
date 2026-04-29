const parseArgs = require('command-line-args')
const { Client } = require('pg')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')

const REQUIRED_FIELDS = ['house_id', 'vendor_id', 'vendor_house_id', 'created', 'updated']

const optionDefs = [
  { name: 'db', type: String, description: 'PostgreSQL connection string (or set DATABASE_URL env)' },
  { name: 'output', alias: 'o', type: String, defaultValue: './output', description: 'Output directory' },
  { name: 'fields', alias: 'f', type: String, multiple: true, defaultValue: [], description: 'Extra fields to export (e.g. detail_dict)' },
  { name: 'batch', alias: 'b', type: Number, defaultValue: 5000, description: 'Rows per FETCH from cursor' },
  { name: 'split', alias: 's', type: Number, defaultValue: 200000, description: 'Max rows per output file' },
  { name: 'gzip', alias: 'z', type: Boolean, defaultValue: false, description: 'Gzip output files' },
  { name: 'ssl-ca', type: String, description: 'Path to CA certificate PEM file for SSL' },
  { name: 'help', alias: 'h', type: Boolean },
]

function printUsage () {
  console.log(`Usage: node dump-etc.js [--db CONNECTION_STRING] [-o OUTPUT_DIR] [-f FIELD...] [-s SPLIT_SIZE] [-b BATCH_SIZE]

Required fields (always exported): ${REQUIRED_FIELDS.join(', ')}

Options:
  --db        PostgreSQL connection string (default: DATABASE_URL env)
  -o, --output  Output directory (default: ./output)
  -f, --fields  Extra fields to include (e.g. -f detail_dict -f could_be_rooftop)
  -b, --batch   Rows per cursor FETCH (default: 5000)
  -s, --split   Max rows per output file (default: 200000)
  -z, --gzip    Gzip output files (.jsonl.gz)
  --ssl-ca      Path to CA certificate PEM file for SSL
  -h, --help    Show this help

Example:
  node dump-etc.js --db postgres://user:pass@localhost/twrh -f detail_dict -s 200000
  node dump-etc.js --db postgres://user:pass@prod/twrh --ssl-ca ca.pem -f detail_dict -z
  DATABASE_URL=postgres://... node dump-etc.js -f detail_dict -f detail_raw`)
}

function buildQuery (extraFields) {
  const allFields = [...REQUIRED_FIELDS, ...extraFields]
  const selectCols = allFields.map(f => {
    if (f === 'created' || f === 'updated') {
      return `${f}, EXTRACT(YEAR FROM ${f})::int AS ${f}_year`
    }
    return f
  })
  return `SELECT ${selectCols.join(', ')} FROM house_etc ORDER BY house_id`
}

class YearFileWriter {
  constructor (outputDir, splitSize, useGzip) {
    this.outputDir = outputDir
    this.splitSize = splitSize
    this.useGzip = useGzip
    this.writers = new Map()
    this.counts = new Map()
  }

  _key (year) {
    const chunk = Math.floor((this.counts.get(year) || 0) / this.splitSize) + 1
    return `${year}_${String(chunk).padStart(3, '0')}`
  }

  _getStream (year) {
    const key = this._key(year)
    if (!this.writers.has(key)) {
      const ext = this.useGzip ? 'jsonl.gz' : 'jsonl'
      const filePath = path.join(this.outputDir, `house_etc_${key}.${ext}`)
      const fileStream = fs.createWriteStream(filePath)
      if (this.useGzip) {
        const gz = zlib.createGzip()
        gz.pipe(fileStream)
        this.writers.set(key, { write: gz, file: fileStream })
      } else {
        this.writers.set(key, { write: fileStream, file: fileStream })
      }
    }
    return this.writers.get(key)
  }

  async write (year, obj) {
    const { write } = this._getStream(year)
    const ok = write.write(JSON.stringify(obj) + '\n')
    if (!ok) {
      await new Promise(resolve => write.once('drain', resolve))
    }
    this.counts.set(year, (this.counts.get(year) || 0) + 1)
  }

  close () {
    const summary = []
    for (const [year, count] of this.counts.entries()) {
      summary.push({ year, rows: count })
    }
    const promises = []
    for (const { write, file } of this.writers.values()) {
      promises.push(new Promise(resolve => {
        file.on('close', resolve)
        write.end()
      }))
    }
    return { summary: summary.sort((a, b) => a.year - b.year), done: Promise.all(promises) }
  }
}

async function main () {
  const args = parseArgs(optionDefs, { partial: true })

  if (args.help) {
    printUsage()
    process.exit(0)
  }

  const connectionString = args.db || process.env.DATABASE_URL
  if (!connectionString) {
    console.error('Error: provide --db or set DATABASE_URL')
    process.exit(1)
  }

  const extraFields = args.fields.filter(f => !REQUIRED_FIELDS.includes(f))
  const allFields = [...REQUIRED_FIELDS, ...extraFields]
  const query = buildQuery(extraFields)

  fs.mkdirSync(args.output, { recursive: true })

  const isLocal = connectionString.includes('localhost') || connectionString.includes('127.0.0.1')
  let ssl = false
  if (args['ssl-ca']) {
    ssl = { ca: fs.readFileSync(args['ssl-ca'], 'utf8'), rejectUnauthorized: true }
  } else if (!isLocal) {
    ssl = { rejectUnauthorized: false }
  }

  const client = new Client({ connectionString, ssl })
  await client.connect()

  const cursorName = 'dump_etc_cursor'
  await client.query('BEGIN')
  await client.query(`DECLARE ${cursorName} CURSOR FOR ${query}`)

  const writer = new YearFileWriter(args.output, args.split, args.gzip)
  let totalRows = 0
  let batch

  process.stderr.write(`Dumping house_etc: fields=[${allFields.join(', ')}] split=${args.split} batch=${args.batch}\n`)

  do {
    batch = await client.query(`FETCH ${args.batch} FROM ${cursorName}`)

    for (const row of batch.rows) {
      const year = row.created_year
      const obj = {}
      for (const f of allFields) {
        obj[f] = row[f]
      }
      await writer.write(year, obj)
      totalRows++
    }

    if (totalRows % 100000 === 0 || batch.rows.length > 0 && totalRows % 50000 < args.batch) {
      process.stderr.write(`  ${totalRows.toLocaleString()} rows processed\r`)
    }
  } while (batch.rows.length > 0)

  await client.query('CLOSE ' + cursorName)
  await client.query('COMMIT')
  await client.end()

  const { summary, done } = writer.close()
  await done
  process.stderr.write('\n')
  console.log(`Done. ${totalRows.toLocaleString()} rows total.`)
  console.log('Files by year:')
  for (const { year, rows } of summary) {
    const chunks = Math.ceil(rows / args.split)
    console.log(`  ${year}: ${rows.toLocaleString()} rows (${chunks} file${chunks > 1 ? 's' : ''})`)
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
