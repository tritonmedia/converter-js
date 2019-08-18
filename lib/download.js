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
const logger = require('pino')({
  name: path.basename(__filename)
})

const minio = require('triton-core/minio')
const proto = require('triton-core/proto')

// main function
module.exports = async config => {
  const converter = require('./convert')(config)
  const uploader = require('./deploy')(config)

  const mediaProto = await proto.load('api.Media')
  const converterStage = proto.stringToEnum(mediaProto, 'TelemetryStatusEntry', 'CONVERTING')

  return async job => {
    const fileId = job.media.id

    const s3Client = minio.newClient(config)

    const child = logger.child({
      jobId: job.media.id
    })

    let pathPrefix = ''
    if (!path.isAbsolute(config.instance.download_path)) {
      child.debug('converting not absolute path to absolute path')
      pathPrefix = path.join(__dirname, '..')
    }
    const downloadPath = path.join(pathPrefix, config.instance.download_path, fileId)

    // recreate the folder
    try {
      await fs.remove(downloadPath)
      child.info('cleaned up existing directory')
    } catch (err) {
      child.warn('failed to remove existing dir (usually OK):', err.message || err)
    }

    await fs.ensureDir(downloadPath)

    child.info(`getting objects to download ...`)

    let objects = _.filter(await minio.getObjects(s3Client, 'triton-staging', path.join(fileId, 'original/')), o => {
      return path.basename(o.name) !== 'done'
    }).sort()

    child.info(`there are ${objects.length} files available ...`)

    const originalLength = objects.length

    const pos = await global.db.getConverterStatus(job.media.id)
    for (let i = 0; i !== pos; i++) {
      objects.shift()
    }

    child.info('starting download at position', pos, `(there are ${objects.length} left)`)

    let i = 0
    for (const o of objects) {
      i++

      const percent = i / objects.length * 100
      try {
        const filename = Buffer.from(path.basename(o.name), 'base64').toString('ascii')
        child.info(`downloading file '${filename}'  (${i} / ${objects.length} - ${Math.floor(percent)}%)`)
        child.debug(`filename '${filename}' is equal to '${path.basename(o.name)}`)
        await s3Client.fGetObject('triton-staging', o.name, path.join(downloadPath, filename))
      } catch (err) {
        child.error(`Failed to download from id '${fileId}' file '${o.name}'`)
        throw err
      }
    }

    i = pos
    for (const o of objects) {
      i++

      // TODO: deduplicate this calculation
      const filename = Buffer.from(path.basename(o.name), 'base64').toString('ascii')

      const percent = i / originalLength * 100
      const fqfp = path.join(downloadPath, filename)

      child.info(`processing file ${i} / ${originalLength} (${Math.floor(percent)}%)`)
      const outputFile = await converter(job, fqfp)
      await uploader(job, outputFile)

      // set CONVERTING progress to the percent metric
      await global.telem.emitProgress(job.media.id, converterStage, Math.floor(percent))

      // update converter point
      await global.db.setConverterStatus(job.media.id, i)
    }

    objects = await minio.getObjects(s3Client, 'triton-staging', path.join(fileId, 'original'))
    const objectNames = _.map(objects, o => o.name)

    child.info(`cleaning up s3 contents`)
    try {
      await s3Client.removeObjects('triton-staging', objectNames)
      await s3Client.removeObject('triton-staging', fileId)
    } catch (err) {
      child.error(`failed to cleanup s3 contents: ${err.message || err}`)
      throw err
    }

    child.info('done')
  }
}
