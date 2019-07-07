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

module.exports = (config, emitter, logger) => {
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

  return async job => {
    const jobId = job.media.id

    emitter.emit('status', 'processing')
    const dirtyBit = path.join(transcodingPath, jobId, 'dirty')
    const baseDir = path.join(transcodingPath, jobId)

    await fs.ensureDir(baseDir)

    let files = job.lastStage.media

    // only ever convert max one media at a time.
    const newFiles = []

    // If not exists, set dirtyBit true
    if (!await fs.pathExists(dirtyBit)) {
      await fs.writeFile(dirtyBit, 'true', 'utf8')
    }

    let i = 0

    await global.telem.emitProgress(jobId, 2, 0)
    return new Promise((resolve, reject) => {
      async.eachLimit(files, 1, (file, next) => {
        i++

        const percent = i / files.length * 100

        const filename = path.basename(file.path.replace(/\.\w+$/, '.mkv'))
        const output = path.join(baseDir, filename)

        logger.info('converting file', i, 'out of', files.length, `(${percent}%)`)
        logger.info('writing file', filename, output)
        newFiles.push(output)

        fs.ensureDir(path.dirname(output))
        let convObject = {
          input: file.path,
          output: output,
          'no-usage-stats': true
        }

        convObject = _.merge(convObject, videoObject)
        convObject = _.merge(convObject, audioObject)

        let isDead = setTimeout(() => {
          return next('No output.')
        }, 20000)

        let progressReporter = setInterval(() => {
          emitter.emit('progress', progress)
          logger.info('progress', progress, eta)
        }, 10000)

        emitter.emit('progress', 0, files.length, i)

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
            logger.error(err.message)
            return next(err)
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

            // set progress of status CONVERTING to percent of files
            global.telem.emitProgress(jobId, 2, Math.floor(percent))
            return next()
          })

          // fired on handbrake output
          .on('output', output => {
            // noop
            // console.log(output)
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
          return reject(err)
        }

        logger.info('finished converting files')
        emitter.emit('status', 'complete')
        return resolve({
          files: newFiles
        })
      })
    })
  }
}
