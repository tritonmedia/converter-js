/**
 * Convert media from one format to another.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const _ = require('lodash')
const fs = require('fs-extra')
const path = require('path')
const conv = require('handbrake-js')
const logger = require('pino')({
  name: path.basename(__filename)
})

module.exports = config => {
  const settings = config.instance.settings

  let pathPrefix = ''
  if (!path.isAbsolute(config.instance.transcoding_path)) {
    logger.debug('path is not absolute, converting to absolute path')
    pathPrefix = path.join(__dirname, '..')
  }
  const transcodingPath = path.join(
    pathPrefix,
    config.instance.transcoding_path
  )

  const videoObject = {
    encoder: settings.video.codec,
    'encoder-profile': settings.video.profile,
    'encoder-preset': settings.video.preset,
    'encoder-tune': settings.video.tune,
    quality: settings.video.quality
  }

  const audioObject = {
    aencoder: settings.audio.codec,
    ab: settings.audio.bitrate,
    'audio-fallback': settings.audio.fallback,
    arate: 'auto',
    mixdown: settings.audio.mixdown,
    'all-subtitles': true,
    'all-audio': true
  }

  for (let key of Object.keys(audioObject)) {
    if (key === '' || typeof key === 'undefined' || key === null) delete audioObject[key]
  }

  for (let key of Object.keys(videoObject)) {
    if (key === '' || typeof key === 'undefined' || key === null) delete videoObject[key]
  }

  /**
   * Convert a File
   *
   * @param {Object} job api.Converter object
   * @param {String} filePath file to convert
   * @returns {String} location of the transcoded file
   */
  return async (job, filePath) => {
    const jobId = job.media.id
    const baseDir = path.join(transcodingPath, jobId)

    await fs.ensureDir(baseDir)

    return new Promise((resolve, reject) => {
      const filename = path.basename(filePath.replace(/\.\w+$/, '.mkv'))
      const output = path.join(baseDir, filename)

      logger.info('writing file', filename, output)

      fs.ensureDir(path.dirname(output))
      let convObject = {
        input: filePath,
        output: output,
        'no-usage-stats': true
      }

      convObject = _.merge(convObject, videoObject)
      convObject = _.merge(convObject, audioObject)

      let isDead = setTimeout(() => {
        return reject(new Error('No output.'))
      }, 20000)

      let progressReporter = setInterval(() => {
        logger.info('progress', progress, eta)
      }, 10000)

      let progress; let eta; let endCatch; let completed; let errored = false
      conv.spawn(convObject)
        // on progress event
        .on('progress', prog => {
          if (isDead) {
            clearTimeout(isDead)
            isDead = null
          }

          progress = prog.percentComplete
          eta = prog.eta
        })

        // on recognized handbrake error
        .on('error', err => {
          errored = true
          logger.error(err.message || err)
          return reject(err)
        })

        // Fire when successfully converted
        .on('complete', async () => {
          if (errored) return
          logger.info('complete fired')

          // clear the end-but not complete catcher.
          clearTimeout(endCatch)

          if (!await fs.pathExists(convObject.output)) {
            throw new Error(`Failed to convert file: ${convObject.input}`)
          }

          return resolve(output)
        })

        // fired on handbrake output
        .on('output', output => {})

        // fired when the process ends.
        .on('end', () => {
          // Catch complete not triggering (error-d)
          endCatch = setTimeout(() => {
            if (!completed) {
              logger.error('failed to invoke handbrake')
              return reject(new Error('Failed to convert. See log.'))
            }
          }, 5000)

          logger.debug('end fired')
          clearInterval(progressReporter)
        })
    })
  }
}
