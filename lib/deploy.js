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

    const child = logger.child({
      jobId: job.media.id
    })

    /**
     * uploadFile uploads a file to twilight
     * @param {String} file - file path
     * @param {Number} retry - retry number
     * @returns {Error} error
     */
    const uploadFile = async (file, retry = 0) => {
      child.info('upload', `${path.basename(file)}`)

      if (!await fs.pathExists(file)) {
        child.error('failed to upload file, not found')
        throw new Error(`${file} not found.`)
      }

      const fileStream = fs.createReadStream(file)
      const fstat = await fs.stat(file)
      const opts = {
        filename: path.basename(file),
        contentType: 'video/x-matroska',
        knownLength: fstat.size
      }

      child.debug('form', opts)
      try {
        const res = await request({
          url: `${mediaHost}/v1/media`,
          formData: {
            file: {
              value: fileStream,
              options: opts
            }
          },
          headers: {
            'X-Media-ID': job.media.id,
            'X-Media-Type': job.media.type,
            'X-Media-Name': job.media.name,

            // Until we have support for quality
            'X-Media-Quality': '1080p'
          },
          method: 'PUT',
          simple: false,
          forever: true,
          resolveWithFullResponse: true
        })

        // handle errors
        if (res.statusCode !== 200) {
          // check if it's retryable
          if (typeof res.body === 'object' && res.body.retryable) {
            if (retry < 3) {
              child.info(`retrying upload:`, res.body, `(${retry}/3)`)
              retry++
              return uploadFile(file, retry)
            } else {
              throw new Error(`${res.body}: and hit max 3 retries`)
            }
          } else {
            throw new Error(res.body)
          }
        }
      } catch (err) {
        child.info('failed to upload file:', err.message || err)
        throw err
      }
    }

    return uploadFile(filePath)
  }
}
