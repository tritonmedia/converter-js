/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const _ = require('lodash')
const EventEmitter = require('events').EventEmitter
const path = require('path')

const AMQP = require('triton-core/amqp')
const proto = require('triton-core/proto')
const dyn = require('triton-core/dynamics')
const Telemetry = require('triton-core/telemetry')

/* eslint no-unused-vars: 1 */
const logger = require('pino')({
  name: path.basename(__filename)
})

const EmitterTable = {}

const stages = [
  'download',
  'convert',
  'deploy'
]

const activeJobs = []

/**
 * Main function that builds the execution stages
 * @param {Object} config - config object
 * @param {opentracing.Tracer} tracer - tracer object
 */
module.exports = async (config, tracer) => {
  const amqp = new AMQP(dyn('rabbitmq'), 1)
  await amqp.connect()

  const telem = new Telemetry(dyn('rabbitmq'))
  await telem.connect()

  // TODO: don't go global
  global.telem = telem

  const downloadProto = await proto.load('api.Download')

  /**
   * Process new media
   *
   */
  const processor = async rmsg => {
    const msg = proto.decode(downloadProto, rmsg.message.content)
    const fileId = msg.media.creatorId
    const jobId = msg.media.id

    // set status to CONVERTING
    await telem.emitStatus(jobId, 2)

    let type = 'tv'

    activeJobs.push({
      cardId: fileId,
      jobId
    })

    const loggerData = {
      jobId,
      type,
      fileId
    }
    const child = logger.child(loggerData)

    const emitter = EmitterTable[fileId] = new EventEmitter()

    const stageStorage = {}
    const stageTable = {}

    // callback system to keep scope
    let lastStageData = {}

    // dynamically generate our stages
    for (const stage of stages) {
      logger.debug('creating stage', stage)

      const modulePath = path.join(__dirname, `${stage}.js`)
      const fn = await require(modulePath)(config, emitter, logger.child(_.extend({
        name: path.basename(modulePath)
      }, loggerData)))

      if (typeof fn !== 'function') {
        const err = new Error(`Invalid stage '${stage}' return value was not a function`)
        throw err
      }

      stageTable[stage] = {
        fn: fn
      }
    }

    logger.info('starting main processor after successful stage init')
    try {
      for (let stage of stages) {
        // TODO: make safer
        const staticCopy = _.create(msg, {
          lastStage: lastStageData
        })

        logger.info(`invoking stage '${stage}'`)
        stageStorage[stage] = {}
        const data = await stageTable[stage].fn(staticCopy)
        lastStageData = data
        emitter.emit('progress', 0)
      }
    } catch (err) {
      child.error('failed to invoke stage:', err.message)
      return
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
    if (activeJobs.length === 0) process.exit(0)

    await amqp.close()

    // TODO: fix this to work like it did before
    process.exit(1)
  }
}
