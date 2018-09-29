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
const async = require('async')

const conv = require('handbrake-js')

module.exports = (config, queue, emitter, logger) => {
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
    arate: 'auto',
    'all-subtitles': true,
    'all-audio': true
  }

  emitter.once('convert', async job => {
    emitter.emit('status', 'processing')
    const dirtyBit = path.join(transcodingPath, job.id, 'dirty')
    const baseDir = path.join(transcodingPath, job.id)

    await fs.ensureDir(baseDir)

    let files = job.data.media

    // only ever convert max one media at a time.
    const newFiles = []

    // If not exists, set dirtyBit true
    if (!await fs.pathExists(dirtyBit)) {
      await fs.writeFile(dirtyBit, 'true', 'utf8')
    }
    const isDirty = await fs.readFile(dirtyBit, 'utf8')

    let i = 0
    async.eachLimit(files, 1, (file, next) => {
      i++
      const { audio } = file

      const filename = path.basename(file.path.replace(/\.\w+$/, '.mkv'))
      const output = path.join(baseDir, filename)

      logger.info('converting file', i, 'out of', files.length)
      newFiles.push(output)

      // we check here to keep newFiles clean
      if (isDirty === 'false') return next(null)

      fs.ensureDir(path.dirname(output))
      let convObject = {
        input: file.path,
        output: output,
        'no-usage-stats': true
      }

      if (audio.length !== 0) {
        convObject = _.merge(convObject, audioObject)
      }

      convObject = _.merge(convObject, videoObject)

      let isDead = setTimeout(() => {
        return next('No output.')
      }, 20000)

      let progressReporter = setInterval(() => {
        emitter.emit('progress', progress)
        job.active()
        logger.info('progress', progress, eta)
      }, 10000)

      emitter.emit('progress', 0, files.length, i)

      let progress, eta, endCatch, completed, errored = false
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
          logger.error(err.message)
          return next(err)
        })

        // Fire when successfully converted
        .on('complete', () => {
          if (errored) return
          logger.debug('complete fired')

          // clear the end-but not complete catcher.
          clearTimeout(endCatch)

          emitter.emit('progress', 100, files.length, i)
          return next()
        })

        // fired on handbrake output
        .on('output', output => {
          logger.debug(output)
        })

        // fired when the process ends.
        .on('end', () => {
          // Catch complete not triggering (error-d)
          endCatch = setTimeout(() => {
            if (!completed) {
              logger.error('failed to invoke handbrake')
              return next(new Error('Failed to convert. See log.'))
            }
          }, 5000)

          logger.debug('end fired')
          clearInterval(progressReporter)
        })
    }, async err => {
      if (err) {
        throw err
      }

      logger.info('converted', files.lengh)
      logger.debug('clearing dirty bit')

      await fs.writeFile(dirtyBit, 'false', 'utf8')

      emitter.emit('status', 'complete')
      emitter.emit('done', {
        next: 'deploy',
        data: {
          files: newFiles
        }
      })
    })
  })
}
