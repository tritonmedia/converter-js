/**
 * Attempts to deploy new media.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const fs = require('fs-extra')
const async = require('async')
const dyn = require('triton-core/dynamics')
const request = require('request-promise-native')
const path = require('path')

const tracer = require('triton-core/tracer')
const { Tags, serializeHTTP } = tracer

module.exports = async (config, queue, emitter, logger, spanFactory) => {
  return async job => {
    const span = await spanFactory()
    const mediaHost = dyn('media')
    const { name } = job.card
    const { data, type } = job
    const { files } = data

    span.setTag(Tags.CARD_ID, job.id)

    const spanHeaders = serializeHTTP(span)

    // create the new media
    logger.info('creating media:', name)
    try {
      await request({
        method: 'POST',
        url: `${mediaHost}/v1/media`,
        headers: spanHeaders,
        body: {
          name,
          id: job.id,
          files: files.length,
          type
        },
        json: true
      })
    } catch (err) {
      logger.error('Failed to connect to Twilight', err.message)
      return emitter.emit('done', {
        next: 'error',
        data: err
      })
    }

    const updateInterval = setInterval(() => {
      logger.debug('updating job')
      job.active()
    }, 10000)

    logger.info('starting file upload')

    return new Promise((resolve, reject) => {
      async.eachLimit(files, 1, async file => {
        logger.info('upload', `${path.basename(file)}`)

        if (!await fs.pathExists(file)) {
          logger.error('failed to upload file, not found')
          throw new Error(`${file} not found.`)
        }

        const fileStream = fs.createReadStream(file)
        const fstat = await fs.stat(file)
        const opts = {
          filename: path.basename(file),
          contentType: 'video/x-matroska',
          knownLength: fstat.size
        }

        logger.debug('form', opts)
        await request({
          url: `${mediaHost}/v1/media/${job.id}`,
          headers: spanHeaders,
          formData: {
            file: {
              value: fileStream,
              options: opts
            }
          },
          method: 'PUT'
        })
      }, err => {
        clearInterval(updateInterval)
        if (err) {
          return reject(err)
        }

        logger.info('finished')
        emitter.emit('status', 'deployed')
        return resolve()
      })
    })
  }
}
