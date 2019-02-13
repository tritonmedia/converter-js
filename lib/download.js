/**
 * Download new media.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const path = require('path')
const fs = require('fs-extra')
const _ = require('lodash')

const { Tags } = require('triton-core/tracer')
const minio = require('triton-core/minio')

// main function
module.exports = async (config, queue, emitter, logger, spanFactory) => {
  return async job => {
    const span = await spanFactory()
    const fileId = job.id

    span.setTag(Tags.CARD_ID, job.id)

    const s3Client = minio.newClient(config)

    let pathPrefix = ''
    if (!path.isAbsolute(config.instance.download_path)) {
      logger.debug('converting not absolute path to absolute path')
      pathPrefix = path.join(__dirname, '..')
    }
    const downloadPath = path.join(pathPrefix, config.instance.download_path, fileId)

    logger.info(`${fileId}: getting objects to download ...`)
    const objects = await minio.getObjects(s3Client, 'triton-staging', path.join(fileId, 'original/'))
    for (let o of objects) {
      // skip the done marker
      if (path.basename(o.name) === 'done') {
        continue
      }

      logger.info(`downloading from id '${fileId}' file '${o.name}'`)
      try {
        const filename = Buffer.from(path.basename(o.name), 'base64').toString('ascii')
        await s3Client.fGetObject('triton-staging', o.name, path.join(downloadPath, filename))
      } catch (err) {
        logger.error(`Failed to download from id '${fileId}' file '${o.name}'`)
        throw err
      }
    }

    // reconstruct the old format to convert
    logger.info('converting downloaded list to converter format ...')
    const files = await fs.readdir(downloadPath)
    const mediaFiles = _.map(files, file => {
      return {
        path: path.join(downloadPath, file)
      }
    })

    logger.info('finished download')
    emitter.emit('status', 'downloaded')
    return {
      media: mediaFiles
    }
  }
}
