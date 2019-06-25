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

const { getObjects } = require('triton-core/minio')

module.exports = async (config, emitter, logger) => {
  return async job => {
    const mediaHost = dyn('media')
    const { name, type } = job.media
    const { files } = job.lastStage

    const fileId = job.media.id

    /**
     * uploadFile uploads a file to twilight
     * @param {String} file - file path
     * @param {Number} retry - retry number
     * @returns {Error} error
     */
    const uploadFile = async (file, retry = 0) => {
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
      try {
        const res = await request({
          url: `${mediaHost}/v1/media/${fileId}`,
          formData: {
            file: {
              value: fileStream,
              options: opts
            }
          },
          method: 'PUT',
          simple: false,
          resolveWithFullResponse: true
        })

        // handle errors
        if (res.statusCode !== 200) {
          // check if it's retryable
          if (typeof res.body === 'object' && res.body.retryable) {
            if (retry < 3) {
              retry++
              return uploadFile(file, retry)
            } else {
              return new Error(`${res.body}: and hit max 3 retries`)
            }
          } else {
            return new Error(res.body)
          }
        }
      } catch (err) {
        return err
      }
    }

    // create the new media
    const body = {
      name,
      id: fileId,
      files: files.length,
      type: type === 0 ? 'movie' : 'tv'
    }
    logger.info('creating media:', name, body)
    try {
      await request({
        method: 'POST',
        url: `${mediaHost}/v1/media`,
        body,
        json: true
      })
    } catch (err) {
      logger.error('Failed to connect to Twilight', err.message)
      throw err
    }

    const updateInterval = setInterval(() => {
      logger.debug('updating job')
      job.active()
    }, 10000)

    logger.info('starting file upload')
    for (const file of files) {
      await uploadFile(file)
    }

    return new Promise((resolve, reject) => {
      async.eachLimit(files, 1, uploadFile, async err => {
        clearInterval(updateInterval)
        if (err) {
          return reject(err)
        }

        logger.info('finished')
        emitter.emit('status', 'deployed')

        logger.info('cleaning up ...')
        const minioEndpoint = new url.URL(dyn('minio'))

        const s3Client = new Minio.Client({
          endPoint: minioEndpoint.hostname,
          port: parseInt(minioEndpoint.port, 10),
          useSSL: minioEndpoint.protocol === 'https:',
          accessKey: config.keys.minio.accessKey,
          secretKey: config.keys.minio.secretKey
        })

        const objects = await getObjects(s3Client, 'triton-staging', path.join(fileId, 'original'))
        const objectNames = _.map(objects, (o) => o.name)

        logger.info(`cleaning up s3 contents for file id '${fileId}`)
        try {
          await s3Client.removeObjects('triton-staging', objectNames)
          await s3Client.removeObject('triton-staging', fileId)
        } catch (err) {
          logger.error(`Failed to cleanup s3 contents for file id '${fileId}`)
          throw err
        }
        return resolve()
      })
    })
  }
}
