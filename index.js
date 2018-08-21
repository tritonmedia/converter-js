/**
 * Convert new media requests.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Config = require('triton-core/config')
const dyn = require('triton-core/dynamics')
const kue = require('kue')
const path = require('path')
const queue = kue.createQueue({
  redis: dyn('redis')
})
const logger = require('pino')({
  name: path.basename(__filename)
})

const init = async () => {
  const config = await Config('converter')

  await require('./lib/main')(config, queue)

  logger.info('initialized')
}

init()

const cleanup = (code = 0) => {
  logger.warn('cleanup() called')
  queue.shutdown(1000, err => {
    if (err) logger.warn('queue.shutdown errored:', err)
    process.exit(code)
  })
}

process.on('SIGINT', () => {
  cleanup()
})

// Handle shutdown / reject
process.on('unhandledRejection', error => {
  logger.error('Unhandled exception', error)

  cleanup(1)
})
