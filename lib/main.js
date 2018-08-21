/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const _ = require('lodash')
const EventEmitter = require('events').EventEmitter
const debug = require('debug')('media:converter:main')
const async = require('async')
const path = require('path')
const Redis = require('ioredis')
const dyn = require('triton-core/dynamics')
const kue = require('kue')
const logger = require('pino')({
  name: path.basename(__filename)
})

// Store EventEmitters here for future cleanup
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

module.exports = async (config, queue) => {
  let isProcessing = false

  /**
   * Process new media
   *
   * @param {kue.Job} container the job
   * @param {function} done callback
   */
  const processor = async (container, done) => {
    const data = container.data
    const media = data.media
    const fileId = data.id

    let type = 'tv'

    const movieLabel = _.find(data.card.labels, {
      name: 'Movie'
    })
    if (movieLabel) type = 'movie'

    const loggerData = {
      job: container.id,
      type,
      fildID: fileId,
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

    // callback system to keep scope
    let stage = 'queue' // track our stage
    emitter.on('done', data => {
      if (!data) data = {}

      const next = data.next

      // don't emit progress on error
      if (next !== 'error') emitter.emit('progress', 100)

      // update our stage
      stage = next

      if (!next || next === '') return emitter.emit('finished')

      // copy the object and extend our staticly passed data
      const staticCopy = _.create(staticData, {
        data: data.data,
        active: () => {
          container.state('active')
          container.set('updated_at', Date.now())
          container.refreshTtl()
        }
      })
      emitter.emit('progress', 0)
      emitter.emit(next, staticCopy)
    })

    // finished event
    emitter.on('finished', () => {
      isProcessing = false
      child.info('finished')
      return done()
    })

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
      child.info('progress', percent, `(${at}/${subTasks})`, stage)

      if (subTasks !== 0) {
        stageStorage[stage].subTasks = subTasks
      }

      if (at !== 0) {
        stageStorage[stage].subTask = at
      }

      const data = {
        job: fileId,
        percent,
        stage,
        data: stageStorage[stage]
      }

      const safeData = JSON.stringify(data)
      metrics.publish('progress', safeData)
    })

    // error event
    emitter.on('error', data => {
      isProcessing = false
      child.error('failed', data.data)
      return done(data.data)
    })

    // dynamically generate our stages
    try {
      async.forEach(stages, async stage => {
        logger.debug('creating stage', stage)

        stageStorage[stage] = {}

        const modulePath = path.join(__dirname, `${stage}.js`)

        // quick compat wrapper
        await require(modulePath)(config, queue, emitter, logger.child(_.extend({
          name: path.basename(modulePath)
        }, loggerData)))
      }, err => {
        if (err) {
          return emitter.emit('done', {
            next: 'error',
            data: err
          })
        }

        // kick off the queue
        emitter.emit('done', {
          next: stages[0],
          data: staticData
        })
      })
    } catch (err) {
      return emitter.emit('done', {
        next: 'error',
        data: err
      })
    }

    isProcessing = true
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
}
