/**
 * Convert new media requests.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Config = require('triton-core/config')
const Tracer = require('triton-core/tracer').initTracer
const dyn = require('triton-core/dynamics')
const kue = require('kue')
const path = require('path')
const queue = kue.createQueue({
  redis: dyn('redis')
})
const logger = require('pino')({
  name: path.basename(__filename)
})
const tracer = Tracer('converter', logger)

const cleanup = async (code = 0) => {
  return new Promise((resolve, reject) => {
    logger.warn('cleanup() called')
    queue.shutdown(1000, err => {
      if (err) logger.warn('queue.shutdown errored:', err)
      return resolve()
    })
  })
}

const init = async () => {
  const config = await Config('converter')
  const termHandler = await require('./lib/main')(config, queue, tracer)

  const exit = async error => {
    if (error && typeof error === 'object') {
      logger.error('Unhandled exception', error.message)
    }

    logger.error(error)

    await termHandler()
    await cleanup()

    process.exit(error ? 1 : 0)
  }

  process.on('SIGINT', exit)
  process.on('unhandledRejection', exit)

  logger.info('initialized')
}

init()
