/**
 * Download new media.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Webtorrent = require('webtorrent')
const request = require('request-promise-native')
const path = require('path')
const fs = require('fs-extra')
const url = require('url')

const client = new Webtorrent()

const TIMEOUT = 240000

// main function
module.exports = async (config, queue, emitter, logger) => {
  const methods = {
    /**
     * Download via torrent.
     *
     * @param {String} magnet          magnet link
     * @param {String} id              File ID
     * @param {String} downloadPath    Path to save file(s) in
     * @param {Object} job             Job Object
     * @return {Promise}               You know what to do.
     */
    magnet: async (magnet, id, downloadPath, job) => {
      logger.info('magnet', magnet.substr(0, 25) + '...')
      return new Promise((resolve, reject) => {
        const initStallHandler = setTimeout(() => {
          logger.warn('download failed to progress, killing')
          reject(new Error('Metadata fetch stalled'))
        }, TIMEOUT) // 2 minutes

        client.add(magnet, {
          path: downloadPath
        }, torrent => {
          const hash = torrent.infoHash

          logger.debug('hash', hash)
          logger.debug('files', torrent.files.length)

          clearTimeout(initStallHandler)
          logger.debug('cleared timeout handler')

          // sadness
          let lastProgress, stallHandler, progress
          const downloadProgress = setInterval(() => {
            progress = torrent.progress * 100
            logger.info('download progress', progress)
            job.active()

            emitter.emit('progress', progress)
          }, 1000 * 30) // every 30 seconds, emit download stats

          stallHandler = setInterval(() => {
            logger.info('stall check', progress, lastProgress)

            if (progress === lastProgress) {
              clearInterval(downloadProgress)
              return reject(new Error('Download stalled.'))
            }

            lastProgress = progress
          }, TIMEOUT)

          torrent.on('error', err => {
            logger.error('torrent error')
            console.log(err)
            client.remove(hash)
            return reject(err)
          })

          torrent.on('done', () => {
            logger.debug('finished, clearing watchers')

            clearInterval(downloadProgress)
            clearInterval(stallHandler)

            client.remove(hash)

            return resolve()
          })
        })
      })
    },

    /**
     * Download via HTTP.
     *
     * @param  {String} resourceUrl   resource url
     * @param  {String} id            File ID
     * @param  {String} downloadPath  Path to download file too
     * @param  {Object} job           Job Object
     * @return {Promise}              .then/.catch etc
     */
    http: async (resourceUrl, id, downloadPath, job) => {
      logger.info('http', resourceUrl)

      return new Promise(async (resolve) => {
        const parsed = url.parse(resourceUrl)
        const filename = path.basename(parsed.pathname)
        const output = path.join(downloadPath, filename)

        await fs.ensureDir(downloadPath)

        const write = fs.createWriteStream(output)
        request(resourceUrl).pipe(write)

        // assume it's downloadProgress
        write.on('close', () => {
          return resolve()
        })
      })
    }
  }

  emitter.once('download', async job => {
    const media = job.media
    const fileId = job.id

    let pathPrefix = ''
    if (!path.isAbsolute(config.instance.download_path)) {
      logger.debug('converting not absolute path to absolute path')
      pathPrefix = path.join(__dirname, '..')
    }

    const downloadPath = path.join(pathPrefix, config.instance.download_path, fileId)

    const download = /(\w+):([^)(\s]+)/g.exec(media.download)
    const url = download[0]

    let protocol = download[1]

    if (protocol === 'https') protocol = 'http'

    const method = methods[protocol]
    if (!method) throw new Error('Protocol not supported.')

    await fs.ensureDir(downloadPath)
    emitter.emit('status', 'downloading')
    try {
      await method(url, fileId, downloadPath, job)
    } catch (err) {
      console.log(err)
      logger.error('error was thrown when attempting to download')
      return emitter.emit('done', {
        next: 'error',
        data: err
      })
    }

    logger.info('finished download')
    emitter.emit('status', 'downloaded')
    emitter.emit('done', {
      next: 'process',
      data: {
        path: downloadPath
      }
    })
  })
}
