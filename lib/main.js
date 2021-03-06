/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const path = require('path')
const os = require('os')
const logger = require('pino')({
  name: path.basename(__filename)
})

const AMQP = require('triton-core/amqp')
const proto = require('triton-core/proto')
const dyn = require('triton-core/dynamics')
const Telemetry = require('triton-core/telemetry')
const Storage = require('triton-core/db')
const Prom = require('triton-core/prom')

const activeJobs = []

/**
 * Main function that builds the execution stages
 * @param {Object} config - config object
 */
module.exports = async config => {
  const prom = Prom.new('converter')
  Prom.expose()

  const amqp = new AMQP(dyn('rabbitmq'), 1, 2, prom)
  await amqp.connect()

  const telem = new Telemetry(dyn('rabbitmq'), prom)
  await telem.connect()

  const db = new Storage()
  await db.connect()

  // TODO: don't go global
  global.telem = telem
  global.db = db

  const downloadProto = await proto.load('api.Download')
  const mediaProto = await proto.load('api.Media')
  const downloader = await require('./download')(config)

  const converterStage = proto.stringToEnum(mediaProto, 'TelemetryStatusEntry', 'CONVERTING')
  const deployedStage = proto.stringToEnum(mediaProto, 'TelemetryStatusEntry', 'DEPLOYED')
  const erroredStage = proto.stringToEnum(mediaProto, 'TelemetryStatusEntry', 'ERRORED')

  /**
   * Process new media
   *
   */
  const processor = async rmsg => {
    const msg = proto.decode(downloadProto, rmsg.message.content)
    const jobId = msg.media.id

    // set status to CONVERTING
    await telem.emitStatus(jobId, converterStage)

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
      await telem.emitStatus(jobId, erroredStage)

      // TODO: backoff via deadletter
      return setTimeout(() => {
        rmsg.nack()
      }, 5000)
    }

    // set status to DEPLOYED
    await telem.emitStatus(jobId, deployedStage)
    rmsg.ack()
  }

  amqp.listen('v1.convert', processor)

  const app = require('express')()

  app.get('/health', (req, res) => {
    if (activeJobs.length === 0) {
      return res.status(500).send({
        message: 'Not Running Jobs'
      })
    }

    return res.status(200).send({
      metadata: {
        success: true,
        host: os.hostname()
      },
      data: {
        active: activeJobs.length
      }
    })
  })

  app.listen(process.env.PORT || 3401)
  logger.info('successfully connected to queue and started server')

  return async () => {
    logger.info('gracefully shutting down')

    await amqp.cancel()

    for (const container of activeJobs) {
      logger.info(`nack ${container.id}`)
      await container.rmsg.nack()
    }

    await amqp.close()

    logger.info('shutdown')
    process.exit(0)
  }
}
