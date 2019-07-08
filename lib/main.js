/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const path = require('path')
const logger = require('pino')({
  name: path.basename(__filename)
})

const AMQP = require('triton-core/amqp')
const proto = require('triton-core/proto')
const dyn = require('triton-core/dynamics')
const Telemetry = require('triton-core/telemetry')
const Storage = require('triton-core/db')

const activeJobs = []

/**
 * Main function that builds the execution stages
 * @param {Object} config - config object
 */
module.exports = async config => {
  const amqp = new AMQP(dyn('rabbitmq'), 1)
  await amqp.connect()

  const telem = new Telemetry(dyn('rabbitmq'))
  await telem.connect()

  const db = new Storage()
  await db.connect()

  // TODO: don't go global
  global.telem = telem
  global.db = db

  const downloadProto = await proto.load('api.Download')
  const downloader = require('./download')(config)

  /**
   * Process new media
   *
   */
  const processor = async rmsg => {
    const msg = proto.decode(downloadProto, rmsg.message.content)
    const jobId = msg.media.id

    // set status to CONVERTING
    await telem.emitStatus(jobId, 2)

    // start off the downloader
    try {
      activeJobs.push({
        id: jobId,
        rmsg
      })
      await downloader(msg)
      activeJobs.pop()
    } catch (err) {
      activeJobs.pop()
      logger.error('failed to convert media:', err.message || err)

      // set status to ERRORED
      await telem.emitStatus(jobId, 5)

      // TODO: backoff via deadletter
      return setTimeout(() => {
        rmsg.nack()
      }, 5000)
    }

    // set status to DEPLOYED
    await telem.emitStatus(jobId, 4)
    rmsg.ack()
  }

  amqp.listen('v1.convert', processor)

  const app = require('express')()

  app.get('/health', (req, res) => {
    return res.status(200).send({
      ping: 'pong'
    })
  })

  app.listen(process.env.PORT || 3401)
  logger.info('successfully connected to queue and started server')

  return async () => {
    logger.info('gracefully shutting down')

    await amqp.cancel()

    for (let container of activeJobs) {
      logger.info(`nack ${container.id}`)
      await container.rmsg.nack()
    }

    await amqp.close()

    logger.info('shutdown')
    process.exit(0)
  }
}
