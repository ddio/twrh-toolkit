const parseArgs = require('command-line-args')
const fs = require('fs')
const zlib = require('zlib')
const { finished } = require('stream')
const AutoDetectDecoderStream = require('autodetect-decoder-stream')
const CsvReadableStream = require('csv-reader')
const { createObjectCsvStringifier } = require('csv-writer')

const OUTPUT_HEADER = [
  { id: 'id', title: '591 ID' },
  { id: 'createdAt', title: '建立時間' },
  { id: 'updatedAt', title: '最後更新時間' },
  { id: 'title', title: '標題' },
  { id: 'description', title: '說明' },
  { id: 'etc', title: '詳細資訊' }
]

function isMatched(title, desp, patterns) {
  return patterns.some((pattern) => {
    return title.includes(pattern) || desp.includes(pattern)
  })
}

function filterOneFile ({ gzip, pattern, appendLine, limit, file }) {
  let readStream = fs.createReadStream(file)
  if (gzip) {
    readStream = readStream.pipe(zlib.createGunzip())
  }
  const csvReader = readStream
    .pipe(new AutoDetectDecoderStream())
    .pipe(new CsvReadableStream({ skipHeader: true }))
  let matchCount = 0

  return new Promise((resolve) => {
    csvReader.on('data', (data) => {
      if (limit && matchCount >= limit) {
        csvReader.pause()
      }

      let detail = data[2].trim()

      if (!detail) {
        return
      }

      try {
        detail = JSON.parse(data[2])
      } catch (err) {
        console.error('invalid detail', detail)
        return
      }

      const title = detail.title || ''
      let desp = detail.remark?.content || ''

      if (!desp && detail.desp) {
        // for old 591 data
        desp = detail.desp.join('') || ''
      }

      if (isMatched(title, desp, pattern)) {
        appendLine({
          id: data[1],
          createdAt: data[3],
          updatedAt: data[4],
          title,
          description: desp,
          etc: JSON.stringify(detail)
        })
        matchCount += 1
      }
    })

    csvReader.on('pause', () => {
      resolve(limit - matchCount)
    })

    csvReader.on('close', () => {
      resolve(limit - matchCount)
    })
  })
}

async function main () {
  const argOpts = [
    { name: 'gzip', alias: 'c', type: Boolean },
    { name: 'pattern', alias: 'p', type: String, multiple: true },
    { name: 'output', alias: 'o', type: String },
    { name: 'limit', alias: 'l', type: Number, default: 0 },
    { name: 'files', alias: 'f', type: String, multiple: true, defaultOption: true, default: [] }
  ]

  const args = parseArgs(argOpts)

  if (!args.pattern) {
    console.error('pattern is required')
    return 1
  }
  if (!args.files.length) {
    console.error('files is required')
    return 2
  }

  const writerStream = args.output ? fs.createWriteStream(args.output) : process.stdout
  const csvWriter = createObjectCsvStringifier({
    header: OUTPUT_HEADER
  })
  writerStream.write(csvWriter.getHeaderString())

  function appendLine (row) {
    writerStream.write(csvWriter.stringifyRecords([row]))
  }

  const originalLimit = args.limit
  let remainingLimit = args.limit

  if (args.files.length) {
    for (const file of args.files) {
      remainingLimit = await filterOneFile({
        ...args,
        limit: remainingLimit,
        file,
        appendLine
      })

      if (originalLimit && remainingLimit <= 0) {
        console.debug('done as reaching limit')
        break
      }
    }
  }

  writerStream.end()
  await new Promise((resolve, reject) => {
    finished(writerStream, (err) => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
  if (!originalLimit || remainingLimit > 0) {
    console.debug('done as reaching EOF')
  }
}

main()