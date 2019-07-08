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
const request = require('request-promise-native')
const logger = require('pino')({
  name: path.basename(__filename)
})

const minio = require('triton-core/minio')
const dyn = require('triton-core/dynamics')

// main function
module.exports = config => {
  const converter = require('./convert')(config)
  const uploader = require('./deploy')(config)

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

    child.info(`${fileId}: getting objects to download ...`)
    let objects = _.filter(await minio.getObjects(s3Client, 'triton-staging', path.join(fileId, 'original/')), o => {
      return path.basename(o.name) !== 'done'
    })

    const pos = await global.db.getConverterStatus(job.media.id)
    for (let i = 0; i !== pos; i++) {
      objects.shift()
    }

    child.info('starting download at position', pos, `(there are ${objects.length} left)`)

    let i = 0
    for (let o of objects) {
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

    const files = await fs.readdir(downloadPath)

    // inform twilight of the new media
    const body = {
      name: job.media.name,
      id: fileId,
      files: files.length,
      type: job.media.type === 0 ? 'movie' : 'tv'
    }

    child.info('creating media:', job.media.name, body)
    try {
      await request({
        method: 'POST',
        url: `${dyn('media')}/v1/media`,
        body,
        json: true
      })
    } catch (err) {
      child.error('Failed to connect to Twilight', err.message)
      throw err
    }

    i = 0
    for (const file of files) {
      i++
      const percent = i / files.length * 1000
      const fqfp = path.join(downloadPath, file)

      child.info(`processing file ${i} / ${files.length} (${Math.floor(percent)}%)`)
      const outputFile = await converter(job, fqfp)
      await uploader(job, outputFile)

      // set CONVERTING progress to the percent metric
      await global.telem.emitProgress(job.media.id, 2, Math.floor(percent))

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
  }
}
