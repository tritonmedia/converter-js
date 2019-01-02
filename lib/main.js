/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const _ = require('lodash')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const path = require('path')
const Redis = require('ioredis')
const dyn = require('triton-core/dynamics')
/* eslint no-unused-vars: 1 */
const { opentracing, Tags, unserialize, error } = require('triton-core/tracer')
const kue = require('kue')
const os = require('os')
const logger = require('pino')({
  name: path.basename(__filename)
})

const EmitterTable = {}

const stages = [
  'download',
  'process',
  'convert',
  'deploy'
]

const metricsDb = dyn('redis') + '/1'
logger.info('metrics is at', metricsDb)
const metrics = new Redis(metricsDb)
const activeJobs = []

/**
 * Main function that builds the execution stages
 * @param {Object} config - config object
 * @param {kue.Queue} queue - queue object
 * @param {opentracing.Tracer} tracer - tracer object
 */
module.exports = async (config, queue, tracer) => {
  let isProcessing = false

  /**
   * Process new media
   *
   * @param {kue.Job} container the job
   * @param {function} done callback
   */
  const processor = async (container, realDone) => {
    const data = container.data
    const media = data.media
    const fileId = data.id
    const rawRootContext = data.rootContext

    const rootContext = unserialize(rawRootContext)
    const span = tracer.startSpan('stageProcessor', {
      references: [ opentracing.followsFrom(rootContext) ]
    })

    span.setTag(Tags.CARD_ID, data.id)

    let type = 'tv'

    activeJobs.push(fileId)

    const movieLabel = _.find(data.card.labels, {
      name: 'Movie'
    })
    if (movieLabel) type = 'movie'

    const loggerData = {
      job: container.id,
      type,
      fileID: fileId,
      attempt: container._attempts || 1
    }
    const child = logger.child(loggerData)

    const emitter = EmitterTable[fileId] = new EventEmitter()

    const staticData = {
      id: fileId,
      card: data.card,
      media,
      type
    }

    const stageStorage = {}
    const stageTable = {}
    const stage = {
      fn: function () {
        throw new Error('Invalid stage function, never overriden')
      }
    }

    // callback system to keep scope
    let currentStage = 'queue' // track our stage
    let lastStage = null
    let lastStageData = {}
    let lastTrace = span

    const done = err => {
      if (err) {
        error(lastTrace, err)
        logger.error(err)
        return realDone(err)
      }

      span.finish()
      return realDone()
    }

    emitter.on('status', status => {
      queue.create('status', {
        id: fileId,
        status
      }).save(err => {
        if (err) return child.error('failed to set status', err)
      })
    })

    // Emit progress events on pubsub
    emitter.on('progress', (percent, subTasks = 0, at = 0) => {
      child.info('progress', percent, `(${at}/${subTasks})`, currentStage)

      if (subTasks !== 0) {
        stageStorage[currentStage].subTasks = subTasks
      }

      if (at !== 0) {
        stageStorage[currentStage].subTask = at
      }

      const data = {
        job: fileId,
        percent,
        stage,
        host: os.hostname(),
        data: stageStorage[stage]
      }

      const safeData = JSON.stringify(data)
      metrics.publish('progress', safeData)
    })

    // dynamically generate our stages
    try {
      isProcessing = true
      async.forEach(stages, async stage => {
        logger.debug('creating stage', stage)

        // generate the span when called to prevent issues with timing
        const spanFactory = async () => {
          logger.info('stage', stage, 'generating span')
          lastTrace.finish()
          lastTrace = tracer.startSpan(stage, {
            references: [
              opentracing.followsFrom(span.context())
            ]
          })
          return lastTrace
        }

        const modulePath = path.join(__dirname, `${stage}.js`)

        // quick compat wrapper
        const fn = await require(modulePath)(config, queue, emitter, logger.child(_.extend({
          name: path.basename(modulePath)
        }, loggerData)), spanFactory)

        if (typeof fn !== 'function') {
          const err = new Error(`Invalid stage '${stage}' return value was not a function`)
          throw err
        }

        stageTable[stage] = _.extend({
          fn: fn
        }, stage)
      }, async err => {
        if (err) {
          return done(err)
        }

        // kick off the queue
        logger.info('starting main processor after successful stage init')
        try {
          for (let stage of stages) {
            // TODO: make safer
            const staticCopy = _.create(staticData, {
              data: lastStageData,
              active: () => {
                container.state('active')
                container.set('updated_at', Date.now())
                container.refreshTtl()
              }
            })

            logger.info(`invoking stage '${stage}'`)
            lastStage = stage
            currentStage = stage
            stageStorage[stage] = {}
            const data = await stageTable[stage].fn(staticCopy)
            lastStageData = data
          }
        } catch (err) {
          isProcessing = false
          child.error('failed to invoke stage:', err.message)

          const errorMetrics = {
            job: fileId,
            stage: stage,
            host: os.hostname(),
            data: {
              message: 'Internal Server Error',
              code: 'ERRNOCODE'
            }
          }

          if (data.data instanceof Error) {
            errorMetrics.data = {
              message: data.data.message,
              code: data.data.code
            }
          }

          metrics.publish('error', JSON.stringify(errorMetrics))
          return done(data.data)
        }

        lastTrace.finish()
        return done()
      })
    } catch (err) {
      return done(err)
    }
  }
  queue.process('newMedia', 1, processor)

  const app = require('express')()

  app.get('/health', (req, res) => {
    if (isProcessing) {
      return res.status(200).send({
        processing: true
      })
    }

    return res.status(500).send({
      processing: false
    })
  })

  app.listen(process.env.PORT || 3401)
  logger.info('successfully connected to queue and started server')

  return () => {
    if (activeJobs.length === 0) process.exit(0)

    // TODO: fix this to work like it did before
    process.exit(1)
  }
}
