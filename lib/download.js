/**
 * Download new media.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Minio = require('minio')
const path = require('path')
const fs = require('fs-extra')
const url = require('url')

const { Tags } = require('triton-core/tracer')
const dyn = require('triton-core/dynamics')
const { getObjects } = require('triton-core/minio')

// main function
module.exports = async (config, queue, emitter, logger, spanFactory) => {
  return async job => {
    const span = await spanFactory()
    const fileId = job.id
    const minioEndpoint = url.parse(dyn('minio'))

    span.setTag(Tags.CARD_ID, job.id)

    const s3Client = new Minio.Client({
      endPoint: minioEndpoint.hostname,
      port: parseInt(minioEndpoint.port, 10),
      useSSL: minioEndpoint.protocol === 'https',
      accessKey: config.keys.minio.accessKey,
      secretKey: config.keys.minio.secretKey
    })

    let pathPrefix = ''
    if (!path.isAbsolute(config.instance.download_path)) {
      logger.debug('converting not absolute path to absolute path')
      pathPrefix = path.join(__dirname, '..')
    }
    const downloadPath = path.join(pathPrefix, config.instance.download_path, fileId)

    logger.info(`${fileId}: getting objects to download ...`)
    const objects = await getObjects(s3Client, fileId, 'original')
    for (let o of objects) {
      logger.info(`downloading from id '${fileId}' file '${o.name}'`)
      try {
        await s3Client.fGetObject(fileId, o.name, path.join(downloadPath, path.basename(o.name)))
      } catch (err) {
        logger.error(`Failed to download from id '${fileId}' file '${o.name}'`)
        throw err
      }
    }

    // reconstruct the old format to convert
    logger.info('converting downloaded list to converter format ...')
    const mediaFiles = []
    const files = await fs.readdir(downloadPath)
    for (let file of files) {
      mediaFiles.push({
        path: path.join(downloadPath, file)
      })
    }

    logger.info('finished download')
    emitter.emit('status', 'downloaded')
    return {
      media: mediaFiles
    }
  }
}
