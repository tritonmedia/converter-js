/**
 * Attempts to deploy new media.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const fs = require('fs-extra')
const dyn = require('triton-core/dynamics')
const request = require('request-promise-native')
const path = require('path')
const logger = require('pino')({
  name: path.basename(__filename)
})

module.exports = config => {
  return async (job, filePath) => {
    const mediaHost = dyn('media')

    const fileId = job.media.id

    // set status to UPLOADING
    await global.telem.emitStatus(job.media.id, 3)

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

    return uploadFile(filePath)
  }
}
