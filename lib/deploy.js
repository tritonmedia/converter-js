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

module.exports = async (config, queue, emitter, logger) => {
  emitter.once('deploy', async job => {
    const mediaHost = dyn('media')
    const name = job.card.name
    const data = job.data
    const files = data.files
    const type = job.type

    // create the new media
    logger.info('creating media:', name)
    await request({
      method: 'POST',
      url: `${mediaHost}/v1/media`,
      body: {
        name,
        id: job.id,
        files: files.length,
        type
      },
      json: true
    })

    const updateInterval = setInterval(() => {
      logger.debug('updating job')
      job.active()
    }, 10000)

    logger.info('starting file upload')
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
        return emitter.emit('done', {
          next: 'error',
          data: err
        })
      }

      logger.info('finished')
      emitter.emit('status', 'deployed')
      return emitter.emit('done')
    })
  })
}
