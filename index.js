/**
 * Convert new media requests.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Config = require('triton-core/config')
const Tracer = require('triton-core/tracer').initTracer
const path = require('path')
const logger = require('pino')({
  name: path.basename(__filename)
})
const tracer = Tracer('converter', logger)

const init = async () => {
  const config = await Config('converter')
  const termHandler = await require('./lib/main')(config, tracer)

  const exit = async error => {
    if (error && typeof error === 'object') {
      logger.error('Unhandled exception', error.message)
    }

    logger.error(error)

    await termHandler()

    process.exit(error ? 1 : 0)
  }

  process.on('SIGINT', exit)
  process.on('unhandledRejection', exit)

  logger.info('initialized')
}

init()
