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
const url = require('url')
const Minio = require('minio')
const _ = require('lodash')

const tracer = require('triton-core/tracer')
const { getObjects } = require('triton-core/minio')
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
      }, async err => {
        clearInterval(updateInterval)
        if (err) {
          return reject(err)
        }

        logger.info('finished')
        emitter.emit('status', 'deployed')

        logger.info('cleaning up ...')
        const minioEndpoint = url.parse(dyn('minio'))

        span.setTag(Tags.CARD_ID, job.id)

        const s3Client = new Minio.Client({
          endPoint: minioEndpoint.hostname,
          port: parseInt(minioEndpoint.port, 10),
          useSSL: minioEndpoint.protocol === 'https',
          accessKey: config.keys.minio.accessKey,
          secretKey: config.keys.minio.secretKey
        })

        const fileId = job.id
        const objects = await getObjects(s3Client, fileId, 'original')
        const objectNames = _.map(objects, (o) => o.name)

        logger.info(`Cleaninup up s3Buck for file id '${fileId}`)
        try {
          await s3Client.removeObjects(fileId, objectNames)
        } catch (err) {
          logger.error(`Failed to cleanup s3Bucket for file id '${fileId}`)
          throw err
        }
        return resolve()
      })
    })
  }
}
